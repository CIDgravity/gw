package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/CIDgravity/filecoin-gateway/database/sqldb"
	"golang.org/x/xerrors"
)

// colKind drives the sqlite→postgres value conversion for one column.
type colKind int

const (
	kInt      colKind = iota // integer NOT NULL
	kNullInt                 // nullable integer
	kBool                    // sqlite integer 0/1 → boolean
	kText                    // text NOT NULL (also used for uuid columns)
	kNullText                // nullable text
	kBlob                    // blob → bytea (nil → NULL)
	kTimeUnix                // unix seconds integer → timestamp
)

type colSpec struct {
	name string
	kind colKind
}

type tableSpec struct {
	name string // table name, identical on both sides
	cols []colSpec
	// conflictKey is the primary key used for ON CONFLICT DO NOTHING;
	// empty means plain inserts (deals_archive has no primary key).
	conflictKey string
}

func cs(kind colKind, names ...string) []colSpec {
	out := make([]colSpec, len(names))
	for i, n := range names {
		out[i] = colSpec{n, kind}
	}
	return out
}

func concat(specs ...[]colSpec) []colSpec {
	var out []colSpec
	for _, s := range specs {
		out = append(out, s...)
	}
	return out
}

var dealCols = concat(
	cs(kText, "uuid"),
	cs(kTimeUnix, "start_time"),
	cs(kText, "client_addr"),
	cs(kInt, "provider_addr", "group_id", "price_afil_gib_epoch"),
	cs(kBool, "verified", "keep_unsealed"),
	cs(kInt, "start_epoch", "end_epoch"),
	cs(kBlob, "signed_proposal_bytes"),
	cs(kNullInt, "deal_id"),
	cs(kNullText, "deal_pub_ts"),
	cs(kNullInt, "sector_start_epoch"),
	cs(kInt, "proposed", "published", "sealed", "failed", "rejected", "failed_expired"),
	cs(kNullText, "error_msg"),
	cs(kInt, "last_state_query"),
	cs(kNullText, "last_state_query_error"),
	cs(kNullInt, "car_transfer_start_time"),
	cs(kInt, "car_transfer_attempts"),
	cs(kNullInt, "car_transfer_last_end_time", "car_transfer_last_bytes"),
	cs(kNullText, "sp_status", "sp_sealing_status", "sp_sig_proposal", "sp_pub_msg_cid"),
	cs(kNullInt, "sp_recv_bytes", "sp_txsize"),
	cs(kInt, "last_deal_state_check", "last_retrieval_check", "last_retrieval_check_success",
		"retrieval_probes_success", "retrieval_probes_fail"),
	cs(kNullText, "retrieval_probe_prev_error"),
	cs(kNullInt, "retrieval_probe_prev_ms", "retrieval_probe_prev_ttfb_ms"),
)

// tableSpecs lists every table copied out of the old store.db, in insert
// order (groups first: offloads has a foreign key on it).
var tableSpecs = []tableSpec{
	{
		name: "groups",
		cols: concat(
			cs(kInt, "id", "blocks", "bytes", "g_state", "jb_recorded_head"),
			cs(kNullInt, "piece_size"),
			cs(kBlob, "commp"),
			cs(kNullInt, "car_size"),
			cs(kBlob, "root"),
		),
		conflictKey: "id",
	},
	{
		name:        "offloads",
		cols:        cs(kInt, "group_id"),
		conflictKey: "group_id",
	},
	{
		name:        "deals",
		cols:        dealCols,
		conflictKey: "uuid",
	},
	{
		name: "deals_archive",
		cols: dealCols,
		// no primary key: created with `create table ... as select * from deals where false`
		conflictKey: "",
	},
	{
		name: "providers",
		cols: concat(
			cs(kInt, "id"),
			cs(kBool, "in_market", "ping_ok", "boost_deals", "booster_http", "booster_bitswap"),
			cs(kInt, "indexed_success", "indexed_fail",
				"retrprobe_success", "retrprobe_fail", "retrprobe_blocks", "retrprobe_bytes"),
			cs(kBool, "ask_ok"),
			cs(kInt, "ask_price", "ask_verif_price", "ask_min_piece_size", "ask_max_piece_size"),
			cs(kNullText, "addr_info_graphsync", "addr_info_bitswap", "addr_info_http"),
		),
		conflictKey: "id",
	},
	{
		name:        "offloads_s3",
		cols:        cs(kInt, "group_id"),
		conflictKey: "group_id",
	},
	{
		name: "external_path",
		cols: concat(
			cs(kInt, "group_id"),
			cs(kNullText, "module", "path"),
		),
		conflictKey: "group_id",
	},
	{
		name: "repairs",
		cols: concat(
			cs(kInt, "group_id", "retrievable_deals"),
			cs(kNullInt, "worker"),
			cs(kInt, "last_attempt"),
		),
		conflictKey: "group_id",
	},
}

const sqlInsertChunk = 200 // rows per multi-row INSERT

// migrateSQL copies all metadata tables from the old sqlite store.db into
// the (already migrated-to-schema) Yugabyte SQL database.
func migrateSQL(ctx context.Context, src *sql.DB, dst sqldb.Database, force bool) (map[string]int64, error) {
	if force {
		if err := truncateTargetTables(dst); err != nil {
			return nil, err
		}
	}

	if err := checkTargetTablesEmpty(dst); err != nil {
		return nil, err
	}

	counts := map[string]int64{}
	for _, spec := range tableSpecs {
		n, err := copyTable(ctx, src, dst, spec)
		if err != nil {
			return nil, xerrors.Errorf("copying table %s: %w", spec.name, err)
		}
		counts[spec.name] = n
		log.Infow("table copied", "table", spec.name, "rows", n)
	}

	if err := fixGroupsSequence(dst); err != nil {
		return nil, err
	}

	return counts, nil
}

// checkTargetTablesEmpty refuses to migrate into tables that already hold
// data: deals_archive has no primary key, so inserts into a non-empty table
// could silently duplicate rows.
func checkTargetTablesEmpty(dst sqldb.Database) error {
	var nonEmpty []string
	for _, spec := range tableSpecs {
		var exists bool
		if err := dst.QueryRow(fmt.Sprintf(`select exists (select 1 from %s)`, spec.name)).Scan(&exists); err != nil {
			return xerrors.Errorf("checking table %s: %w", spec.name, err)
		}
		if exists {
			nonEmpty = append(nonEmpty, spec.name)
		}
	}
	if len(nonEmpty) > 0 {
		return xerrors.Errorf("target tables not empty: %s (re-run with force to truncate them, or resume with the existing state file)", strings.Join(nonEmpty, ", "))
	}
	return nil
}

func truncateTargetTables(dst sqldb.Database) error {
	// reverse order, groups last; offloads references groups so groups
	// needs CASCADE anyway
	for i := len(tableSpecs) - 1; i >= 0; i-- {
		name := tableSpecs[i].name
		q := fmt.Sprintf(`truncate table %s`, name)
		if name == "groups" {
			q += " cascade"
		}
		if _, err := dst.Exec(q); err != nil {
			return xerrors.Errorf("truncating %s: %w", name, err)
		}
	}
	return nil
}

func copyTable(ctx context.Context, src *sql.DB, dst sqldb.Database, spec tableSpec) (int64, error) {
	present, err := sqliteColumns(src, spec.name)
	if err != nil {
		return 0, err
	}
	if len(present) == 0 {
		log.Warnw("source table missing, skipping", "table", spec.name)
		return 0, nil
	}

	// Old deployments may predate some columns (see sqliteColumns); insert
	// only what exists, the rest keeps target defaults / NULL.
	var cols []colSpec
	for _, c := range spec.cols {
		if present[c.name] {
			cols = append(cols, c)
		} else {
			log.Warnw("source column missing, target default will be used", "table", spec.name, "column", c.name)
		}
	}
	if len(cols) == 0 {
		return 0, nil
	}

	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}

	rows, err := src.QueryContext(ctx, fmt.Sprintf(`select %s from %s`, strings.Join(names, ", "), spec.name))
	if err != nil {
		return 0, xerrors.Errorf("selecting source rows: %w", err)
	}
	defer rows.Close() // nolint:errcheck

	ins := newChunkInserter(dst, spec, cols)

	var total int64
	for rows.Next() {
		// Every column scans as nullable: tables created with
		// `create table ... as select` (deals_archive) carry no NOT NULL
		// constraints, so even "required" columns can hold NULL there.
		holders := make([]interface{}, len(cols))
		for i, c := range cols {
			switch c.kind {
			case kInt, kTimeUnix, kNullInt, kBool:
				holders[i] = new(sql.NullInt64)
			case kText, kNullText:
				holders[i] = new(sql.NullString)
			case kBlob:
				holders[i] = new([]byte)
			}
		}
		if err := rows.Scan(holders...); err != nil {
			return total, xerrors.Errorf("scanning source row: %w", err)
		}

		args := make([]interface{}, len(cols))
		for i, c := range cols {
			switch c.kind {
			case kInt, kNullInt:
				args[i] = *holders[i].(*sql.NullInt64)
			case kTimeUnix:
				v := *holders[i].(*sql.NullInt64)
				if v.Valid {
					args[i] = time.Unix(v.Int64, 0).UTC()
				}
			case kBool:
				v := *holders[i].(*sql.NullInt64)
				if v.Valid {
					args[i] = v.Int64 != 0
				}
			case kText, kNullText:
				args[i] = *holders[i].(*sql.NullString)
			case kBlob:
				args[i] = *holders[i].(*[]byte)
			}
		}

		if err := ins.add(ctx, args); err != nil {
			return total, err
		}
		total++
	}
	if err := rows.Err(); err != nil {
		return total, xerrors.Errorf("iterating source rows: %w", err)
	}

	if err := ins.flush(ctx); err != nil {
		return total, err
	}

	return total, nil
}

// chunkInserter accumulates rows and writes them with multi-row INSERTs.
type chunkInserter struct {
	dst  sqldb.Database
	spec tableSpec
	cols []colSpec

	args    []interface{}
	pending int
}

func newChunkInserter(dst sqldb.Database, spec tableSpec, cols []colSpec) *chunkInserter {
	return &chunkInserter{dst: dst, spec: spec, cols: cols}
}

func (ci *chunkInserter) add(ctx context.Context, rowArgs []interface{}) error {
	ci.args = append(ci.args, rowArgs...)
	ci.pending++
	if ci.pending >= sqlInsertChunk {
		return ci.flush(ctx)
	}
	return nil
}

func (ci *chunkInserter) flush(ctx context.Context) error {
	if ci.pending == 0 {
		return nil
	}

	names := make([]string, len(ci.cols))
	for i, c := range ci.cols {
		names[i] = c.name
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "insert into %s (%s) values ", ci.spec.name, strings.Join(names, ", "))
	n := 1
	for row := 0; row < ci.pending; row++ {
		if row > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for col := range ci.cols {
			if col > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "$%d", n)
			n++
		}
		sb.WriteString(")")
	}
	if ci.spec.conflictKey != "" {
		fmt.Fprintf(&sb, " on conflict (%s) do nothing", ci.spec.conflictKey)
	}

	if _, err := ci.dst.ExecContext(ctx, sb.String(), ci.args...); err != nil {
		return xerrors.Errorf("inserting %d rows into %s: %w", ci.pending, ci.spec.name, err)
	}

	ci.args = ci.args[:0]
	ci.pending = 0
	return nil
}

// mutableGroupDirs returns the grp/ subdirectories of groups that can still
// be written to (writable or full-but-not-finalized, g_state < 2). Their
// files must be copied rather than hardlinked: the new node appends to them
// (block writes, finalization), which would otherwise write through to the
// read-only source copy.
func mutableGroupDirs(src *sql.DB) (map[string]bool, error) {
	rows, err := src.Query(`select id from groups where g_state < 2`)
	if err != nil {
		return nil, xerrors.Errorf("selecting mutable groups: %w", err)
	}
	defer rows.Close() // nolint:errcheck

	out := map[string]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, xerrors.Errorf("scanning group id: %w", err)
		}
		out[strconv.FormatInt(id, 32)] = true
	}
	return out, rows.Err()
}

// fixGroupsSequence bumps the groups id sequence past the migrated ids so
// the first CreateGroup on the new system doesn't collide.
func fixGroupsSequence(dst sqldb.Database) error {
	var maxID sql.NullInt64
	if err := dst.QueryRow(`select max(id) from groups`).Scan(&maxID); err != nil {
		return xerrors.Errorf("reading max group id: %w", err)
	}
	if !maxID.Valid {
		return nil // no groups; fresh sequence starts at 1
	}

	if _, err := dst.Exec(`select setval(pg_get_serial_sequence('groups', 'id'), $1, true)`, maxID.Int64); err != nil {
		return xerrors.Errorf("advancing groups id sequence: %w", err)
	}
	return nil
}
