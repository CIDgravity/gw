package rbstor

import (
	"context"
	"database/sql"

	"github.com/CIDgravity/filecoin-gateway/database/sqldb"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"golang.org/x/xerrors"

	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-cid"
	"github.com/lib/pq"
)

type RbsDB struct {
	db sqldb.Database
}

func NewRibsDB(db sqldb.Database) *RbsDB {
	return &RbsDB{
		db: db,
	}
}

func (r *RbsDB) GetGroupStats() (*iface.GroupStats, error) {
	var gs iface.GroupStats
	err := r.db.QueryRow(`SELECT group_count, total_data_size, non_offloaded_data_size, offloaded_data_size FROM group_stats_view`).Scan(&gs.GroupCount, &gs.TotalDataSize, &gs.NonOffloadedDataSize, &gs.OffloadedDataSize)
	if err != nil {
		return nil, xerrors.Errorf("querying group stats: %w", err)
	}
	return &gs, nil
}

func (r *RbsDB) GetWritableGroup() (selected iface.GroupKey, blocks, bytes, jbhead int64, state iface.GroupState, err error) {
	res, err := r.db.Query("select id, blocks, bytes, jb_recorded_head, g_state from groups where g_state = 0")
	if err != nil {
		return 0, 0, 0, 0, 0, xerrors.Errorf("finding writable groups: %w", err)
	}
	defer res.Close()

	selectedGroup := iface.UndefGroupKey

	if res.Next() {
		err := res.Scan(&selectedGroup, &blocks, &bytes, &jbhead, &state)
		if err != nil {
			return 0, 0, 0, 0, 0, xerrors.Errorf("scanning group: %w", err)
		}
	}

	if err := res.Err(); err != nil {
		return 0, 0, 0, 0, 0, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return 0, 0, 0, 0, 0, xerrors.Errorf("closing group iterator: %w", err)
	}

	return selectedGroup, blocks, bytes, jbhead, state, nil
}

func (r *RbsDB) CreateGroup() (out iface.GroupKey, err error) {
	err = r.db.QueryRow("insert into groups (blocks, bytes, g_state, jb_recorded_head) values (0, 0, 0, 0) returning id").Scan(&out)
	if err != nil {
		return iface.UndefGroupKey, xerrors.Errorf("creating group entry: %w", err)
	}

	return
}

func (r *RbsDB) OpenGroup(gid iface.GroupKey) (blocks, bytes, jbhead int64, state iface.GroupState, err error) {
	res, err := r.db.Query("select blocks, bytes, jb_recorded_head, g_state from groups where id = $1", gid)
	if err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("opening groups: %w", err)
	}
	defer res.Close()

	var found bool

	if res.Next() {
		err := res.Scan(&blocks, &bytes, &jbhead, &state)
		if err != nil {
			return 0, 0, 0, 0, xerrors.Errorf("scanning group: %w", err)
		}

		found = true
	}

	if err := res.Err(); err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return 0, 0, 0, 0, xerrors.Errorf("closing group iterator: %w", err)
	}
	if !found {
		return 0, 0, 0, 0, xerrors.Errorf("group %d not found", gid)
	}

	return blocks, bytes, jbhead, state, nil
}

func (r *RbsDB) AllGroupStates() (gs map[iface.GroupKey]iface.GroupState, err error) {
	res, err := r.db.Query("select id, g_state from groups")
	if err != nil {
		return nil, xerrors.Errorf("finding group states: %w", err)
	}
	defer closeRows(res)

	return scanGroupStates(res)
}

func (r *RbsDB) GroupStates(ids []iface.GroupKey) (gs map[iface.GroupKey]iface.GroupState, err error) {
	if len(ids) == 0 {
		return map[iface.GroupKey]iface.GroupState{}, nil
	}
	res, err := r.db.Query("select id, g_state from groups where id = ANY($1)", pq.Array(ids))
	if err != nil {
		return nil, xerrors.Errorf("finding group states: %w", err)
	}
	defer closeRows(res)

	return scanGroupStates(res)
}

func scanGroupStates(rows *sql.Rows) (gs map[iface.GroupKey]iface.GroupState, err error) {
	gs = make(map[iface.GroupKey]iface.GroupState)
	for rows.Next() {
		var id iface.GroupKey
		var state iface.GroupState
		err := rows.Scan(&id, &state)
		if err != nil {
			return nil, xerrors.Errorf("scanning group: %w", err)
		}
		gs[id] = state
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, xerrors.Errorf("closing group iterator: %w", err)
	}
	return gs, nil
}

func (r *RbsDB) SetGroupHead(ctx context.Context, id iface.GroupKey, state iface.GroupState, commBlk, commSz, at int64) error {
	_, err := r.db.ExecContext(ctx, `update groups set blocks = $1, bytes = $2, g_state = $3, jb_recorded_head = $4 where id = $5;`, commBlk, commSz, state, at, id)
	if err != nil {
		return xerrors.Errorf("update group head: %w", err)
	}

	return nil
}

func (r *RbsDB) SetGroupState(ctx context.Context, id iface.GroupKey, state iface.GroupState) error {
	_, err := r.db.ExecContext(ctx, `update groups set g_state = $1 where id = $2;`, state, id)
	if err != nil {
		return xerrors.Errorf("update group state: %w", err)
	}

	return nil
}

func (r *RbsDB) SetCommP(ctx context.Context, id iface.GroupKey, state iface.GroupState, commp []byte, paddedPieceSize int64, root cid.Cid, carSize int64) error {
	_, err := r.db.ExecContext(ctx, `update groups set commp = $1, piece_size = $2, root = $3, car_size = $4, g_state = $5 where id = $6;`,
		commp[:], paddedPieceSize, root.Bytes(), carSize, state, id)
	if err != nil {
		return xerrors.Errorf("update group commp: %w", err)
	}

	return nil
}

/* DIAGNOSTICS */

func (r *RbsDB) Groups() ([]iface.GroupKey, error) {
	res, err := r.db.Query("select id from groups order by id desc")
	if err != nil {
		return nil, xerrors.Errorf("listing groups: %w", err)
	}
	defer res.Close()

	var groups []iface.GroupKey
	for res.Next() {
		var id int64
		err := res.Scan(&id)
		if err != nil {
			return nil, xerrors.Errorf("scanning group: %w", err)
		}

		groups = append(groups, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating groups: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing group iterator: %w", err)
	}

	return groups, nil
}

func (r *RbsDB) GroupMeta(gk iface.GroupKey) (iface.GroupMeta, error) {
	res, err := r.db.Query("select blocks, bytes, g_state, car_size, commp, root from groups where id = $1", gk)
	if err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("getting group meta: %w", err)
	}
	defer res.Close()

	var blocks int64
	var bytes int64
	var state iface.GroupState
	var found bool
	var carSize *int64
	var commp, root []byte

	if res.Next() {
		err := res.Scan(&blocks, &bytes, &state, &carSize, &commp, &root)
		if err != nil {
			return iface.GroupMeta{}, xerrors.Errorf("scanning group: %w", err)
		}

		found = true
	}

	if err := res.Err(); err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("iterating groups: %w", err)
	}

	if err := res.Close(); err != nil {
		return iface.GroupMeta{}, xerrors.Errorf("closing group iterator: %w", err)
	}

	if !found {
		return iface.GroupMeta{}, xerrors.Errorf("group %d not found", gk)
	}

	var pcid, rcid string
	if len(commp) > 0 {
		c, err := commcid.PieceCommitmentV1ToCID(commp)
		if err != nil {
			return iface.GroupMeta{}, xerrors.Errorf("parsing commp: %w", err)
		}

		pcid = c.String()
	}
	if len(root) > 0 {
		c, err := cid.Cast(root)
		if err != nil {
			return iface.GroupMeta{}, xerrors.Errorf("parsing root: %w", err)
		}

		rcid = c.String()
	}

	return iface.GroupMeta{
		State: state,

		MaxBlocks: maxGroupBlocks,
		MaxBytes:  maxGroupSize,

		Blocks: blocks,
		Bytes:  bytes,

		DealCarSize: carSize,

		PieceCID: pcid,
		RootCID:  rcid,
	}, nil
}

func (r *RbsDB) DescibeGroup(ctx context.Context, group iface.GroupKey) (iface.GroupDesc, error) {
	var out iface.GroupDesc

	res, err := r.db.QueryContext(ctx, "SELECT root, commp, car_size FROM groups WHERE id = $1", group)
	if err != nil {
		return iface.GroupDesc{}, xerrors.Errorf("finding group: %w", err)
	}
	defer res.Close()

	var found bool

	if res.Next() {
		var root, commp []byte
		err := res.Scan(&root, &commp, &out.CarSize)
		if err != nil {
			return iface.GroupDesc{}, xerrors.Errorf("scanning group: %w", err)
		}

		_, out.RootCid, err = cid.CidFromBytes(root)
		if err != nil {
			return iface.GroupDesc{}, xerrors.Errorf("converting root to cid: %w", err)
		}

		out.PieceCid, err = commcid.DataCommitmentV1ToCID(commp)
		if err != nil {
			return iface.GroupDesc{}, xerrors.Errorf("converting commp to cid: %w", err)
		}

		found = true
	}

	if err := res.Err(); err != nil {
		return iface.GroupDesc{}, xerrors.Errorf("iterating groups: %w", err)
	}

	if !found {
		return iface.GroupDesc{}, xerrors.Errorf("group %d not found", group)
	}

	return out, nil
}

func (r *RbsDB) CountNonOffloadedGroups() (count int, err error) {
	err = r.db.QueryRow("SELECT COUNT(*) FROM groups LEFT JOIN offloads ON groups.id = offloads.group_id WHERE offloads.group_id IS NULL").Scan(&count)
	if err != nil {
		return 0, xerrors.Errorf("counting non-offloaded groups: %w", err)
	}
	return
}

func (r *RbsDB) GetOffloadCandidate() (id iface.GroupKey, err error) {
	err = r.db.QueryRow(`
		SELECT id
		FROM groups
		LEFT JOIN offloads ON groups.id = offloads.group_id
		WHERE offloads.group_id IS NULL AND g_state IN (3, 4)
		ORDER BY
			CASE WHEN g_state = 4 THEN 0 ELSE 1 END,
			id
		LIMIT 1
	`).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return iface.UndefGroupKey, nil
		}
		return 0, xerrors.Errorf("getting priority non-offloaded group ID: %w", err)
	}
	return
}

func (r *RbsDB) WriteOffloadEntry(gid iface.GroupKey) (err error) {
	_, err = r.db.Exec("insert into offloads (group_id) values ($1) on conflict (group_id) do nothing", gid)
	if err != nil {
		return xerrors.Errorf("writing offload entry: %w", err)
	}
	return nil
}

func closeRows(rows *sql.Rows) {
	if err := rows.Close(); err != nil {
		log.Errorf("closing rows: %s", err)
	}
}
