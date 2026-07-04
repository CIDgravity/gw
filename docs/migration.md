# Migrating a pre-Yugabyte `.ribsdata`

Older deployments (up to the last pebble+sqlite revision) kept all metadata
inside `.ribsdata`:

| Old location                | Contents                                   | New location                        |
|-----------------------------|--------------------------------------------|-------------------------------------|
| `index.pebble/`             | top-level block index (multihash → groups) | CQL `MultihashToGroup` (Yugabyte)   |
| `store.db` (sqlite)         | groups, deals, providers, offloads, ...    | Yugabyte SQL tables                 |
| `grp/`, `cardata/`, ...     | group data (carlog), localweb CAR files    | unchanged, carried over verbatim    |

Per-group on-disk formats did **not** change: carlog data files, head files,
leveldb write indexes (`index.level`), finalized bsst indexes (`index.bsst`,
`fil.bsst`) and hash samples all remain readable by current code. New groups
use a WAL write index (`idx.wal`), and legacy leveldb groups keep working
until they are finalized.

## Running the migrator

The migrator is offline: stop the old node (or work from a snapshot), and run
it against a source directory it treats as strictly read-only. It produces a
new `.ribsdata` (hardlinking data files where possible, so same-filesystem
migrations take seconds and no extra space) and fills the Yugabyte databases.

Groups that can still be written to (writable or full-but-not-finalized,
`g_state < 2`) are always fully copied instead of hardlinked — the new node
appends to those files, and a hardlink would write through to the source
copy. This is typically a handful of groups; everything finalized or
offloaded is immutable and safe to share. Pass `--copy` to copy everything.

```shell
# target databases, same variables the daemon uses
export RIBS_YUGABYTE_SQL_HOST=... RIBS_YUGABYTE_SQL_USER=... RIBS_YUGABYTE_SQL_PASS=... RIBS_YUGABYTE_SQL_DB=filecoingw
export RIBS_YUGABYTE_CQL_HOSTS=... RIBS_YUGABYTE_CQL_KEYSPACE=filecoingw

ritool migrate --source /old/.ribsdata --dest /new/.ribsdata
```

Notes:

- The CQL keyspace and SQL database must exist (as for the daemon); schema
  migrations are applied automatically on connect.
- Group ids are preserved, and the `groups` id sequence is advanced past the
  migrated ids.
- `groups.node_id` is left NULL; nothing in the backend reads it (it is only
  used for S3 frontend routing).
- S3-era tables (`S3Objects`, `CidGroups`, GC tables, ...) are left empty;
  they have no pre-migration equivalent.
- The wallet (`~/.ribswallet`) and the kubo/kuri repo are not part of
  `.ribsdata` and are carried over by simply keeping them in place.

### Interrupting and resuming

Progress is checkpointed in `<dest>/.ribsdata-migrate.state.json`. If the
migration is interrupted, re-run the same command: completed phases are
skipped and the block index load resumes from the last checkpoint. All
database writes are idempotent, so overlap around a checkpoint is harmless.

To redo the SQL phase from scratch against non-empty tables (for example
after a failed experiment), pass `--force-sql`, which truncates the migrated
tables first.

### Verification

After migrating, the tool verifies the result against the source (this is
also available standalone via `--verify-only`):

- row counts for every migrated table, plus a field-by-field comparison of
  `groups` and a check that the id sequence is ahead of `max(id)`,
- a rescan of the old pebble index comparing group sets and block sizes
  against CQL (`--verify-sample N` checks every Nth multihash; `1` checks
  everything),
- presence and size of every replicated file.

A read-only sanity check of the migrated deployment: start kuri against the
new `.ribsdata` and Yugabyte, and read a few known CIDs back.

## Tests

`migrate/goldenrepo` generates a deterministic old-format `.ribsdata`
(pebble index, old-schema sqlite, and real carlog groups in writable, full,
finalized-local and offloaded states, including a block shared by two
groups). The integration tests in `migrate/` (`TestMigrateGolden`,
`TestMigrateResume`, `TestMigrateReadOnlySource`) migrate it into a
containerized Yugabyte, verify every table/index/file, exercise
interrupt+resume and re-run idempotency, and finally open the migrated
repository with rbstor and read every block back through the CQL index.
