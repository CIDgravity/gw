# Migrating a pre-Yugabyte `.ribsdata`

Runbook and reference for migrating a node from the old all-local storage
stack (top-level pebble index + sqlite `store.db`) to the Yugabyte-backed
stack, using the offline migrator (`ritool migrate`).

## What changes, what doesn't

| Old location            | Contents                                   | New location                      |
|-------------------------|--------------------------------------------|-----------------------------------|
| `index.pebble/`         | top-level block index (multihash → groups) | CQL `MultihashToGroup` (Yugabyte) |
| `store.db` (sqlite)     | groups, deals, providers, offloads, ...    | Yugabyte SQL tables               |
| `grp/`, `cardata/`, ... | group data (carlog), localweb CAR files    | unchanged, carried over verbatim  |

Per-group on-disk formats did **not** change: carlog data files, head
files, leveldb write indexes (`index.level`), finalized bsst indexes
(`index.bsst`, `fil.bsst`) and hash samples all remain readable by current
code. New groups use a WAL write index (`idx.wal`); legacy leveldb groups
keep working until they are finalized.

Untouched (keep in place, no action needed):

- **MongoDB** file metadata (`RIBS_MONGODB_URI`) — same database, same
  schema, same queries
- the wallet (`~/.ribswallet` by default)
- the kubo/kuri repo (MFS root and node datastore)

## Prerequisites

- The new binaries, built from this tree: `make all` produces `kuri`,
  `gwcfg`, `s3-proxy` and `ritool`.
- A reachable YugabyteDB cluster (YSQL on 5433, YCQL on 9042). The
  `ansible/` tree in this repo can deploy one (`playbooks/setup-yb.yml` +
  the `yugabyte_init` role).
- Enough disk space on the destination filesystem:
  - same filesystem as the source: almost nothing — files are hardlinked,
    except groups that are still being written (writable / full but not yet
    finalized), which are fully copied. That is typically a handful of
    groups (up to ~30 GiB each).
  - different filesystem, or `--copy`: a full copy of the source.
- Downtime window: the migration is **offline**. The old node must be
  stopped (or you must work from a snapshot taken while it was stopped).
  The long pole is streaming the block index into CQL; budget by index
  size, not by data size — the group data itself is not rewritten.

## 1. Provision the databases

Create the SQL database and CQL keyspace for the node (the daemon and the
migrator apply schema migrations automatically on connect, but the
database/keyspace themselves must exist). Following the naming convention
used by the ansible deployment (`filecoingw_<node_id>`):

```shell
PGPASSWORD=... psql -h <yb-host> -p 5433 -U yugabyte -d yugabyte \
  -c "CREATE DATABASE filecoingw_node1;"

cqlsh <yb-host> 9042 \
  -e "CREATE KEYSPACE IF NOT EXISTS filecoingw_node1
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"
```

## 2. Stop the old node and snapshot

```shell
systemctl stop <old-node-service>
```

Take a copy or filesystem snapshot of the old `.ribsdata` and point the
migrator at it. The migrator never writes into the source (a read-only
snapshot is fine), but note that by default the output hardlinks immutable
group data — the source copy and the new directory share those file inodes.
Keep the copy for rollback either way; use `--copy` if you want the output
fully independent of it.

Back up the wallet directory now if you haven't recently.

## 3. Run the migrator

```shell
# target databases, same variables the daemon uses
export RIBS_YUGABYTE_SQL_HOST=... RIBS_YUGABYTE_SQL_PORT=5433
export RIBS_YUGABYTE_SQL_USER=... RIBS_YUGABYTE_SQL_PASS=...
export RIBS_YUGABYTE_SQL_DB=filecoingw_node1
export RIBS_YUGABYTE_CQL_HOSTS=... RIBS_YUGABYTE_CQL_PORT=9042
export RIBS_YUGABYTE_CQL_KEYSPACE=filecoingw_node1

ritool migrate \
  --source /snapshots/ribsdata-old \
  --dest   /data/ribsdata
```

Three phases run in order (progress is logged; a summary and a
verification report are printed at the end, and the command exits non-zero
if verification finds any problem):

1. **SQL metadata** (fast) — every `store.db` table (`groups`, `deals`,
   `deals_archive`, `providers`, `offloads`, `offloads_s3`,
   `external_path`, `repairs`) is copied into Yugabyte SQL with type
   conversions (unix ints → timestamps, ints → booleans, uuid text →
   uuid). Group ids are preserved and the `groups` id sequence is advanced
   past them. Columns missing from old databases and NULLs in
   constraint-free tables (`deals_archive` is CTAS) are tolerated.
2. **File tree** (fast when hardlinking) — everything except the two
   replaced databases is replicated verbatim; still-mutable groups
   (`g_state < 2`) are always fully copied since the new node appends to
   those files.
3. **Block index** (the long pole) — the old `'s:'`/`'i:'` pebble key
   layout is streamed into CQL on a worker pool with idempotent upsert
   batches.

Useful knobs:

- `--workers` / `--batch-size` — CQL insert concurrency (defaults 16/1024);
  raise workers if the Yugabyte cluster has headroom.
- `--verify-sample N` — after migrating, every Nth block is checked against
  CQL (default 128). `--verify-sample 1` re-checks every block — thorough
  but roughly doubles total runtime. `--skip-verify` / `--verify-only`
  split the two stages.
- `--copy` — never hardlink.

### Interruptions

Progress is checkpointed in `<dest>/.ribsdata-migrate.state.json`. If the
migration dies or is stopped, **re-run the same command** — completed
phases are skipped and the index load resumes from the last checkpoint. All
database writes are idempotent, so overlap around a checkpoint is harmless.

When the SQL phase (re)starts — first run, or a re-run after a mid-phase
failure, or after deleting the state file for a full redo — any rows
already present in the migrated tables are cleared first (logged as
`clearing non-empty target table`). CQL rows are upserts and need no
reset.

### Verification

The automatic post-migration verify (also available standalone via
`--verify-only`) compares:

- row counts for every migrated table, plus a field-by-field comparison of
  `groups` and a check that the id sequence is ahead of `max(id)`,
- a rescan of the old pebble index against CQL (group sets and block
  sizes, sampled per `--verify-sample`),
- presence and size of every replicated file.

## 4. Cut over

Configure the new node (see `ansible/roles/kuri/templates/settings.env.j2`
for the full set):

```shell
RIBS_DATA=/data/ribsdata            # the migrated directory
RIBS_YUGABYTE_SQL_*                 # as during migration
RIBS_YUGABYTE_CQL_*                 # as during migration
RIBS_MONGODB_URI=...                # unchanged from the old deployment
RIBS_WEBDAV_ENABLED=true            # WebDAV frontend (off by default);
RIBS_WEBDAV_BINDADDR=:8077          # listen address
RIBS_GC_ENABLED=false               # keep off: GC refcounts derive from S3
                                    # object references, which this
                                    # deployment does not populate
```

For WebDAV/Mongo deployments two more things to know:

- File metadata keeps flowing to Mongo (`StartMeta` is wired in kuri as
  before). Nothing to migrate there.
- The WebDAV frontend is enabled with the two variables above. The NFS
  frontend remains disabled in code (`StartMfsNFSFs` in
  `integrations/kuri/ribsplugin/kuboribs.go`) and needs more than
  re-wiring — its listener is commented out internally.

Start the node and smoke-test:

1. `kuri daemon` comes up and logs no schema/connection errors.
2. Read back a handful of known CIDs / WebDAV paths (old finalized groups,
   old writable groups, and — if applicable — an offloaded group, which
   exercises the localweb/S3 retrieval path).
3. Write a new file and read it back (this creates the first new group; its
   id continues after the migrated ones).
4. Deal tracking: check that existing deals appear and the claim extender
   is running — the migrated `deals` table is what keeps existing deals
   maintained.

Notes:

- `groups.node_id` is left NULL by the migrator; nothing in the backend
  reads it (it is only used for S3 frontend routing).
- S3-era tables (`S3Objects`, `CidGroups`, GC tables, ...) are left empty;
  they have no pre-migration equivalent.

## 5. Rollback

Until the cutover smoke tests succeed, the old deployment is fully intact:
the source `.ribsdata` was never written to (caveat: if the new node has
*run* and appended to hardlinked files — it doesn't for finalized groups,
and mutable groups were copied precisely to avoid this). To roll back, stop
the new node and start the old binary against the old directory. The
Yugabyte side can simply be dropped and re-created for another attempt.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `clearing non-empty target table` (warning) | Leftovers of a previous SQL-phase attempt (or a deliberate redo after deleting the state file) are being truncated before recopying. |
| `values out of int64 range were clamped` (warning) | The old sqlite held REAL values beyond int64 (e.g. absurd `ask_price` asks like 1.23e20); they are clamped to the int64 maximum. Informational — these columns are refreshed by the SP crawler anyway. |
| `create cql migrate session: ...` | The CQL keyspace doesn't exist (see step 1) or the CQL host/port/credentials are wrong. |
| `read-only pebble open failed, copying index to temp space` (warning) | Expected on strictly read-only snapshots; the index is copied under `<dest>/.ribsdata-migrate.tmp` first. Needs free space for the pebble directory. |
| Verification reports `groups differ` / index mismatches | Do not cut over. Re-run with `--verify-sample 1` to bound the damage, and check whether the source was modified during migration (was the old node really stopped?). |
| `some blocks had no size entry` (warning) | The old index had `i:` entries without an `s:` size record; sizes are recorded as 0, matching what the old node would have returned. Informational. |
| Index load is slow | Raise `--workers`, check Yugabyte CPU/disk. Progress lines log entries done; checkpoints mean a restart never loses more than `--checkpoint-every` entries (default 1M). |

## Tests

`migrate/goldenrepo` generates a deterministic old-format `.ribsdata`
(pebble index, old-schema sqlite, and real carlog groups in writable,
full, finalized-local and offloaded states, including a block shared by
two groups). The integration tests in `migrate/` (`TestMigrateGolden`,
`TestMigrateResume`, `TestMigrateReadOnlySource`) migrate it into a
containerized Yugabyte, verify every table/index/file, exercise
interrupt+resume and re-run idempotency, and finally open the migrated
repository with rbstor and read every block back through the CQL index.
The WebDAV frontend has its own end-to-end suite in
`integrations/kuri/tests/webdav`.
