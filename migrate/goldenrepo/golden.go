// Package goldenrepo generates a .ribsdata directory in the old
// (pre-Yugabyte) on-disk format: a top-level pebble index, a sqlite
// store.db with the old schema, and carlog group directories in the states
// an aged deployment accumulates (writable with a leveldb write index,
// full, finalized with a bsst index, and offloaded).
//
// It exists to test the offline migrator: generation is deterministic for a
// given seed, and the returned Manifest describes everything the migrated
// deployment must contain.
package goldenrepo

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"github.com/CIDgravity/filecoin-gateway/carlog"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	_ "github.com/mattn/go-sqlite3"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

// Group states as stored in groups.g_state (unchanged across the migration).
const (
	StateWritable  = 0
	StateFull      = 1
	StateHasCommp  = 3
	StateOffloaded = 4
)

type Block struct {
	Mh     multihash.Multihash
	Data   []byte
	Groups []int64
}

type DealRow struct {
	UUID         string
	GroupID      int64
	ProviderAddr int64
	StartTime    int64
	Verified     bool
	Sealed       bool
	Failed       bool
	DealID       *int64
	ErrorMsg     *string
}

type Manifest struct {
	// Blocks lists every unique block with the set of groups containing it
	// and, implicitly, its expected index size (len(Data)).
	Blocks []Block

	// GroupStates maps group id → g_state as written to store.db.
	GroupStates map[int64]int64
	// GroupBlocks maps group id → number of blocks in the group.
	GroupBlocks map[int64]int64

	// ReadableGroups contains groups whose blocks are locally readable after
	// migration (i.e. not offloaded).
	ReadableGroups []int64
	OffloadedGroup int64

	Deals         []DealRow
	ArchivedDeals int
	Providers     int
	Repairs       int

	// ExtraFiles are paths (relative to the repo root) that carry no special
	// meaning to the migrator and must be replicated verbatim.
	ExtraFiles []string
}

// nullStaging satisfies carlog.CarStorageProvider; the golden repo never
// uses external/staged storage.
type nullStaging struct{}

func (nullStaging) Upload(context.Context, int64, func(io.Writer) error) error {
	return xerrors.New("golden repo staging must not be used")
}
func (nullStaging) Has(context.Context) (bool, error) { return false, nil }
func (nullStaging) ReadAt([]byte, int64) (int, error) {
	return 0, xerrors.New("golden repo staging must not be used")
}

// Generate writes an old-format .ribsdata into dir (which must not exist or
// be empty) and returns the manifest describing it.
func Generate(dir string, seed int64) (*Manifest, error) {
	rng := rand.New(rand.NewSource(seed))

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	m := &Manifest{
		GroupStates: map[int64]int64{},
		GroupBlocks: map[int64]int64{},
	}

	// --- groups -------------------------------------------------------

	shared := genBlocks(rng, 1, 512)[0] // lives in groups 1 and 2

	groupBlocks := map[int64][]blocks.Block{
		1: append(genBlocks(rng, 24, 2048), shared),
		2: append(genBlocks(rng, 29, 4096), shared),
		3: genBlocks(rng, 15, 1024),
		4: genBlocks(rng, 20, 1024),
	}

	type groupPlan struct {
		id       int64
		state    int64
		finalize bool
		offload  bool
		readable bool
	}
	plans := []groupPlan{
		{id: 1, state: StateWritable, readable: true},
		{id: 2, state: StateHasCommp, finalize: true, readable: true},
		{id: 3, state: StateOffloaded, finalize: true, offload: true},
		{id: 4, state: StateFull, readable: true},
	}

	heads := map[int64]int64{}
	for _, p := range plans {
		at, err := writeGroup(dir, p.id, groupBlocks[p.id], p.finalize, p.offload)
		if err != nil {
			return nil, xerrors.Errorf("writing group %d: %w", p.id, err)
		}
		heads[p.id] = at
		m.GroupStates[p.id] = p.state
		m.GroupBlocks[p.id] = int64(len(groupBlocks[p.id]))
		if p.readable {
			m.ReadableGroups = append(m.ReadableGroups, p.id)
		}
		if p.offload {
			m.OffloadedGroup = p.id
		}
	}

	// collect unique blocks with their group sets, in stable order
	seen := map[string]int{}
	for _, gid := range []int64{1, 2, 3, 4} {
		for _, b := range groupBlocks[gid] {
			key := string(b.Cid().Hash())
			if i, ok := seen[key]; ok {
				m.Blocks[i].Groups = append(m.Blocks[i].Groups, gid)
				continue
			}
			seen[key] = len(m.Blocks)
			m.Blocks = append(m.Blocks, Block{
				Mh:     b.Cid().Hash(),
				Data:   b.RawData(),
				Groups: []int64{gid},
			})
		}
	}

	// --- top-level pebble index ----------------------------------------

	if err := writePebbleIndex(filepath.Join(dir, "index.pebble"), m.Blocks); err != nil {
		return nil, xerrors.Errorf("writing pebble index: %w", err)
	}

	// --- localweb cardata for the offloaded group -----------------------

	carName := fmt.Sprintf("%d-%s.car", m.OffloadedGroup, uuid.New().String())
	if err := os.MkdirAll(filepath.Join(dir, "cardata"), 0755); err != nil {
		return nil, err
	}
	carData := make([]byte, 4096)
	rng.Read(carData)
	if err := os.WriteFile(filepath.Join(dir, "cardata", carName), carData, 0644); err != nil {
		return nil, err
	}
	m.ExtraFiles = append(m.ExtraFiles, filepath.Join("cardata", carName))

	// a file the migrator knows nothing about; must be carried verbatim
	junk := filepath.Join("grp", strconv.FormatInt(2, 32), "notes.txt")
	if err := os.WriteFile(filepath.Join(dir, junk), []byte("leftover operator note\n"), 0644); err != nil {
		return nil, err
	}
	m.ExtraFiles = append(m.ExtraFiles, junk)

	// --- sqlite store.db -------------------------------------------------

	if err := writeStoreDB(dir, rng, m, heads, carName); err != nil {
		return nil, xerrors.Errorf("writing store.db: %w", err)
	}

	return m, nil
}

func genBlocks(rng *rand.Rand, count, maxSize int) []blocks.Block {
	out := make([]blocks.Block, count)
	for i := range out {
		data := make([]byte, 64+rng.Intn(maxSize-64))
		rng.Read(data)
		out[i] = blocks.NewBlock(data)
	}
	return out
}

// writeGroup creates grp/<id> with a real carlog. Non-finalized groups get
// their write index converted from the WAL format (what current carlog code
// creates) to the leveldb format old deployments have on disk.
func writeGroup(dir string, id int64, blks []blocks.Block, finalize, offload bool) (int64, error) {
	groupPath := filepath.Join(dir, "grp", strconv.FormatInt(id, 32))
	if err := os.MkdirAll(groupPath, 0755); err != nil {
		return 0, err
	}

	metaPath := filepath.Join(groupPath, "blklog.meta")

	cl, err := carlog.Create(nullStaging{}, metaPath, groupPath, nil)
	if err != nil {
		return 0, xerrors.Errorf("creating carlog: %w", err)
	}

	mhs := make([]multihash.Multihash, len(blks))
	for i, b := range blks {
		mhs[i] = b.Cid().Hash()
	}
	if err := cl.Put(mhs, blks); err != nil {
		return 0, xerrors.Errorf("putting blocks: %w", err)
	}

	at, err := cl.Commit()
	if err != nil {
		return 0, xerrors.Errorf("committing carlog: %w", err)
	}

	if finalize {
		if err := cl.MarkReadOnly(); err != nil {
			return 0, err
		}
		// old deployments finalized locally (no staging storage): index.bsst
		// next to a local blklog.car, head not marked External
		if err := cl.FinalizeLegacyLocal(context.Background()); err != nil {
			return 0, xerrors.Errorf("finalizing carlog: %w", err)
		}
		if offload {
			if err := cl.Offload(); err != nil {
				return 0, xerrors.Errorf("offloading carlog: %w", err)
			}
		}
	}

	if err := cl.Close(); err != nil {
		return 0, xerrors.Errorf("closing carlog: %w", err)
	}

	if !finalize {
		if err := walIndexToLevel(metaPath, at); err != nil {
			return 0, xerrors.Errorf("converting write index to leveldb: %w", err)
		}
	}

	return at, nil
}

// walIndexToLevel rewrites the group write index from the WAL format into
// the leveldb format that pre-migration deployments have.
func walIndexToLevel(metaPath string, retiredAt int64) error {
	walPath := filepath.Join(metaPath, carlog.WalIndexFile)

	wal, err := carlog.OpenWalIndex(walPath, retiredAt)
	if err != nil {
		return xerrors.Errorf("opening wal index: %w", err)
	}

	var mhs []multihash.Multihash
	var offs []int64
	err = wal.List(func(c multihash.Multihash, offsets []int64) error {
		for _, off := range offsets {
			mhs = append(mhs, c)
			offs = append(offs, off)
		}
		return nil
	})
	if err != nil {
		return xerrors.Errorf("listing wal index: %w", err)
	}
	if err := wal.Close(); err != nil {
		return xerrors.Errorf("closing wal index: %w", err)
	}

	lvl, err := carlog.OpenLevelDBIndex(filepath.Join(metaPath, carlog.LevelIndex), true)
	if err != nil {
		return xerrors.Errorf("creating leveldb index: %w", err)
	}
	if err := lvl.Put(mhs, offs); err != nil {
		return xerrors.Errorf("filling leveldb index: %w", err)
	}
	if err := lvl.Sync(); err != nil {
		return xerrors.Errorf("syncing leveldb index: %w", err)
	}
	if err := lvl.Close(); err != nil {
		return xerrors.Errorf("closing leveldb index: %w", err)
	}

	return os.Remove(walPath)
}

// writePebbleIndex mirrors the old rbstor PebbleIndex.AddGroup key layout:
//
//	'i:[mh][group u64 BE]' -> {}
//	's:[mh]'               -> [size u32 BE][group u64 BE]
func writePebbleIndex(path string, blks []Block) error {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return err
	}

	batch := db.NewBatch()
	for _, b := range blks {
		for _, g := range b.Groups {
			groupBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(groupBytes, uint64(g))

			ikey := append(append([]byte("i:"), b.Mh...), groupBytes...)
			if err := batch.Set(ikey, nil, pebble.NoSync); err != nil {
				return err
			}

			sval := make([]byte, 12)
			binary.BigEndian.PutUint32(sval, uint32(len(b.Data)))
			copy(sval[4:], groupBytes)
			skey := append([]byte("s:"), b.Mh...)
			if err := batch.Set(skey, sval, pebble.NoSync); err != nil {
				return err
			}
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}
	return db.Close()
}

func writeStoreDB(dir string, rng *rand.Rand, m *Manifest, heads map[int64]int64, offloadCarName string) error {
	db, err := sql.Open("sqlite3", filepath.Join(dir, "store.db"))
	if err != nil {
		return err
	}
	defer db.Close() // nolint:errcheck

	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		return err
	}
	if _, err := db.Exec(oldRbstorSchema); err != nil {
		return xerrors.Errorf("applying old rbstor schema: %w", err)
	}
	if _, err := db.Exec(oldDealsSchema); err != nil {
		return xerrors.Errorf("applying old deals schema: %w", err)
	}

	// groups
	for _, gid := range []int64{1, 2, 3, 4} {
		var blockCount, byteCount int64
		for _, b := range m.Blocks {
			for _, g := range b.Groups {
				if g == gid {
					blockCount++
					byteCount += int64(len(b.Data))
				}
			}
		}

		var pieceSize, carSize interface{}
		var commp, root interface{}
		if m.GroupStates[gid] == StateHasCommp || m.GroupStates[gid] == StateOffloaded {
			pieceSize = int64(1 << 20)
			carSize = heads[gid]
			cp := make([]byte, 32)
			rng.Read(cp)
			commp = cp
			rt := make([]byte, 36)
			rng.Read(rt)
			root = rt
		}

		_, err = db.Exec(`insert into groups (id, blocks, bytes, g_state, jb_recorded_head, piece_size, commp, car_size, root)
			values (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			gid, blockCount, byteCount, m.GroupStates[gid], heads[gid], pieceSize, commp, carSize, root)
		if err != nil {
			return xerrors.Errorf("inserting group %d: %w", gid, err)
		}
	}

	// offload bookkeeping for group 3
	if _, err := db.Exec(`insert into offloads (group_id) values (?)`, m.OffloadedGroup); err != nil {
		return err
	}
	if _, err := db.Exec(`insert into external_path (group_id, module, path) values (?, 'local-web', ?)`, m.OffloadedGroup, offloadCarName); err != nil {
		return err
	}

	// deals
	deals := []DealRow{
		{GroupID: 2, ProviderAddr: 1001, Verified: true, Sealed: true, DealID: i64p(4242)},
		{GroupID: 2, ProviderAddr: 1002, Verified: true},
		{GroupID: 2, ProviderAddr: 1003, Failed: true, ErrorMsg: strp("deal rejected: price too low")},
		{GroupID: 3, ProviderAddr: 1001, Sealed: true, DealID: i64p(4243)},
		{GroupID: 3, ProviderAddr: 1004},
		{GroupID: 2, ProviderAddr: 1005, Failed: true},
	}
	for i := range deals {
		deals[i].UUID = uuid.New().String()
		deals[i].StartTime = 1700000000 + int64(i)*3600
		if err := insertOldDeal(db, "deals", deals[i], rng); err != nil {
			return xerrors.Errorf("inserting deal: %w", err)
		}
	}
	m.Deals = deals

	// archived deals (same shape, separate table without a primary key)
	for i := 0; i < 2; i++ {
		d := DealRow{
			UUID:         uuid.New().String(),
			GroupID:      2,
			ProviderAddr: 900 + int64(i),
			StartTime:    1690000000 + int64(i)*3600,
			Failed:       true,
		}
		if err := insertOldDeal(db, "deals_archive", d, rng); err != nil {
			return xerrors.Errorf("inserting archived deal: %w", err)
		}
	}
	m.ArchivedDeals = 2

	// providers
	providers := []struct {
		id                int64
		inMarket, pingOk  bool
		boostDeals, askOk bool
		askPrice          int64
	}{
		{1001, true, true, true, true, 100000},
		{1002, true, false, false, false, 0},
		{1003, false, false, false, false, 0},
	}
	for _, p := range providers {
		_, err := db.Exec(`insert into providers (id, in_market, ping_ok, boost_deals, booster_http, booster_bitswap,
			ask_ok, ask_price, ask_verif_price, ask_min_piece_size, ask_max_piece_size, addr_info_http)
			values (?, ?, ?, ?, 0, 1, ?, ?, ?, 256, ?, ?)`,
			p.id, p.inMarket, p.pingOk, p.boostDeals, p.askOk, p.askPrice, p.askPrice/2, int64(64<<30),
			fmt.Sprintf(`{"addr": "http://provider-%d.example.com"}`, p.id))
		if err != nil {
			return xerrors.Errorf("inserting provider %d: %w", p.id, err)
		}
	}
	m.Providers = len(providers)

	// repairs
	if _, err := db.Exec(`insert into repairs (group_id, retrievable_deals, worker, last_attempt) values (2, 3, NULL, 0)`); err != nil {
		return err
	}
	m.Repairs = 1

	return nil
}

func insertOldDeal(db *sql.DB, table string, d DealRow, rng *rand.Rand) error {
	proposal := make([]byte, 128)
	rng.Read(proposal)

	sealed, failed := 0, 0
	var sectorStart interface{}
	if d.Sealed {
		sealed = 1
		sectorStart = int64(300000)
	}
	var failedExpired int
	if d.Failed {
		failed = 1
		failedExpired = 1
	}
	var dealID interface{}
	var pubTs interface{}
	published := 0
	if d.DealID != nil {
		dealID = *d.DealID
		published = 1
		pubTs = "2023-11-15T10:00:00Z"
	}
	var errMsg interface{}
	if d.ErrorMsg != nil {
		errMsg = *d.ErrorMsg
	}

	_, err := db.Exec(fmt.Sprintf(`insert into %s (uuid, start_time, client_addr, provider_addr, group_id,
		price_afil_gib_epoch, verified, keep_unsealed, start_epoch, end_epoch, signed_proposal_bytes,
		deal_id, deal_pub_ts, sector_start_epoch,
		proposed, published, sealed, failed, rejected, failed_expired, error_msg,
		last_state_query, retrieval_probes_success, retrieval_probes_fail, sp_status)
		values (?, ?, 'f1goldenclientaddr', ?, ?, ?, ?, 1, 290000, 1800000, ?, ?, ?, ?, 1, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)`, table),
		d.UUID, d.StartTime, d.ProviderAddr, d.GroupID, int64(181818182), boolToInt(d.Verified), proposal,
		dealID, pubTs, sectorStart,
		published, sealed, failed, failedExpired, errMsg,
		d.StartTime+100, int64(5), int64(1), "Complete")
	return err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func i64p(v int64) *int64   { return &v }
func strp(s string) *string { return &s }
