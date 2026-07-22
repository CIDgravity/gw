# Migration Guide: pre-Yugabyte `.ribsdata` → Yugabyte-backed deployment

This is the operator runbook for migrating an existing node from the old
all-local storage stack (top-level pebble index + sqlite `store.db`) to the
Yugabyte-backed stack. For what the migrator does internally, see
[migration.md](./migration.md).

## What changes, what doesn't

Migrated:

- `store.db` (groups, deals, providers, offloads, repairs, ...) → Yugabyte
  **SQL** tables
- `index.pebble` (block → group index) → Yugabyte **CQL**
  `MultihashToGroup` table
- everything else in `.ribsdata` (`grp/`, `cardata/`, ...) is carried over
  verbatim into a new directory — per-group on-disk formats (carlog data,
  leveldb write indexes, bsst indexes) are unchanged and stay readable

Untouched (keep in place, no action needed):

- **MongoDB** file metadata (`RIBS_MONGODB_URI`) — same database, same
  schema, same queries
- the wallet (`~/.ribswallet` by default)
- the kubo/kuri repo (MFS root and node datastore)

## Prerequisites

- The new binaries, built from this tree: `make all ritool` produces
  `kuri` and `ritool`.
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
export RIBS_YUGABYTE_SQL_HOST=<yb-host>
export RIBS_YUGABYTE_SQL_PORT=5433
export RIBS_YUGABYTE_SQL_USER=yugabyte
export RIBS_YUGABYTE_SQL_PASS=...
export RIBS_YUGABYTE_SQL_DB=filecoingw_node1
export RIBS_YUGABYTE_CQL_HOSTS=<yb-host>
export RIBS_YUGABYTE_CQL_PORT=9042
export RIBS_YUGABYTE_CQL_KEYSPACE=filecoingw_node1

ritool migrate \
  --source /snapshots/ribsdata-old \
  --dest   /data/ribsdata
```

Phases run in order: SQL tables (fast), file tree (fast when hardlinking),
block index (the long pole — progress is logged every 30s). A summary and a
verification report are printed at the end; the command exits non-zero if
verification finds any problem.

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
writes are idempotent, so overlap is harmless.

To redo everything from scratch instead, delete the state file and pass
`--force-sql` (truncates the migrated SQL tables; CQL rows are upserts and
need no reset).

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
- The WebDAV frontend is off by default; enable it with
  `RIBS_WEBDAV_ENABLED=true` (listen address `RIBS_WEBDAV_BINDADDR`,
  default `:8077`). The NFS frontend remains disabled in code
  (`StartMfsNFSFs` in `integrations/kuri/ribsplugin/kuboribs.go`) and needs
  more than re-wiring — its listener is commented out internally.

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

## 5. Rollback

Until step 4 succeeds, the old deployment is fully intact: the source
`.ribsdata` was never written to (caveat: if the new node has *run* and
appended to hardlinked files — it doesn't for finalized groups, and
mutable groups were copied precisely to avoid this). To roll back, stop the
new node and start the old binary against the old directory. The Yugabyte
side can simply be dropped and re-created for another attempt.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `target tables not empty` | A previous attempt wrote SQL rows but the state file is missing. Resume with the state file, or redo with `--force-sql`. |
| `create cql migrate session: ...` | The CQL keyspace doesn't exist (see step 1) or the CQL host/port/credentials are wrong. |
| `read-only pebble open failed, copying index to temp space` (warning) | Expected on strictly read-only snapshots; the index is copied under `<dest>/.ribsdata-migrate.tmp` first. Needs free space for the pebble directory. |
| Verification reports `groups differ` / index mismatches | Do not cut over. Re-run with `--verify-sample 1` to bound the damage, and check whether the source was modified during migration (was the old node really stopped?). |
| `some blocks had no size entry` (warning) | The old index had `i:` entries without an `s:` size record; sizes are recorded as 0, matching what the old node would have returned. Informational. |
| Index load is slow | Raise `--workers`, check Yugabyte CPU/disk. Progress lines log entries done; checkpoints mean a restart never loses more than `--checkpoint-every` entries (default 1M). |
