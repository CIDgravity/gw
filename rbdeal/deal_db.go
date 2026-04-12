package rbdeal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/database/sqldb"
	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	types2 "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"

	"github.com/CIDgravity/filecoin-gateway/ributil"
	types "github.com/CIDgravity/filecoin-gateway/ributil/boosttypes"
)

type ribsDB struct {
	db sqldb.Database

	dealSummaryCq *ributil.CachedQuery[iface2.DealSummary]
	reachableCq   *ributil.CachedQuery[[]iface2.ProviderMeta]

	lastAnalyzed time.Time
}

// TODO: MIGRATE offloads_s3 to external_path !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

func openRibsDB(db sqldb.Database) (*ribsDB, error) {
	rd := &ribsDB{
		db: db,

		lastAnalyzed: time.Now(),
	}

	return rd, nil
}

var analyzeInterval = 6 * time.Hour

func (r *ribsDB) startDB() error {

	r.dealSummaryCq = ributil.NewCachedQuery[iface2.DealSummary](1*time.Minute, r.dealSummary)
	r.reachableCq = ributil.NewCachedQuery[[]iface2.ProviderMeta](1*time.Minute, r.reachableProviders)

	if err := timeDBOp("refresh_bad_providers_new_reject", r.db, refreshViewTable("bad_providers_new_reject")); err != nil {
		return err
	}
	if err := timeDBOp("refresh_sp_deal_stats", r.db, refreshViewTable("sp_deal_stats")); err != nil {
		return err
	}
	if err := timeDBOp("refresh_sp_retr_stats", r.db, refreshViewTable("sp_retr_stats")); err != nil {
		return err
	}
	if err := timeDBOp("refresh_good_providers", r.db, refreshGoodProviders()); err != nil {
		return err
	}

	go func() {
		for {
			time.Sleep(2 * time.Minute)
			if err := timeDBOp("refresh_bad_providers_new_reject", r.db, refreshViewTable("bad_providers_new_reject")); err != nil {
				continue
			}
			if err := timeDBOp("refresh_sp_deal_stats", r.db, refreshViewTable("sp_deal_stats")); err != nil {
				continue
			}
			if err := timeDBOp("refresh_sp_retr_stats", r.db, refreshViewTable("sp_retr_stats")); err != nil {
				continue
			}
			if err := timeDBOp("refresh_good_providers", r.db, refreshGoodProviders()); err != nil {
				continue
			}

			if time.Since(r.lastAnalyzed) > analyzeInterval {
				_ = timeDBOp("analyze", r.db, func(db sqldb.Database) error {
					_, err := db.Exec("ANALYZE")
					return err
				})
				r.lastAnalyzed = time.Now()
			}
		}
	}()

	return nil
}

func timeDBOp(name string, db sqldb.Database, f func(db sqldb.Database) error) error {
	start := time.Now()
	err := f(db)
	log.Debugw("DB op time", "name", name, "took", time.Since(start), "error", err)
	return err
}

func refreshViewTable(name string) func(db sqldb.Database) error {
	return func(db sqldb.Database) error {
		tempTable := name + "_tmp"
		viewTable := name + "_view"
		targetTable := name

		_, err := db.Exec(`
		CREATE TEMP TABLE ` + tempTable + ` AS SELECT * FROM ` + viewTable + `;
		DELETE FROM ` + targetTable + `;
		INSERT INTO ` + targetTable + ` SELECT * FROM ` + tempTable + `;
		DROP TABLE ` + tempTable + `;`)

		return err
	}
}

func refreshGoodProviders() func(db sqldb.Database) error {
	return func(db sqldb.Database) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer func(tx *sql.Tx) {
			err := tx.Rollback()
			if err != nil && !errors.Is(err, sql.ErrTxDone) {
				log.Errorw("failed to rollback transaction", "error", err)
			}
		}(tx)

		_, err = tx.Exec(`
	CREATE TEMP TABLE good_providers_tmp_imm1 AS SELECT p.* FROM providers p
		 LEFT JOIN bad_providers_new_reject bp ON p.id = bp.sp_id
	WHERE p.in_market = true
		AND p.ping_ok = true
        AND p.ask_ok = true
        AND p.ask_min_piece_size <= $1
        AND p.ask_max_piece_size >= $2
		AND bp.sp_id IS NULL;  -- Excludes bad providers`, maxPieceSize, minPieceSize)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`
	CREATE TEMP TABLE good_providers_tmp AS SELECT
        p.id, p.ping_ok, p.boost_deals, p.booster_http, p.booster_bitswap,
        p.indexed_success, p.indexed_fail,
        p.retrprobe_success, p.retrprobe_fail, p.retrprobe_blocks, p.retrprobe_bytes,
        p.ask_price, p.ask_verif_price, p.ask_min_piece_size, p.ask_max_piece_size
    FROM
        good_providers_tmp_imm1 p
        LEFT JOIN sp_deal_stats ds ON p.id = ds.sp_id
        LEFT JOIN sp_retr_stats rs ON p.id = rs.sp_id
    WHERE
        (ds.failed_all IS NULL OR ds.failed_all = 0)
        AND (rs.unretrievable_deals IS NULL OR (rs.unretrievable_deals <= 1 OR rs.retrievable_deals >= 0.7 * (rs.retrievable_deals + rs.unretrievable_deals) )) /* has up to 1 unretrievable deals, or most are retrievable  */
    ORDER BY
        (p.booster_bitswap::int + p.booster_http::int) ASC, p.boost_deals ASC, p.id DESC`)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`DROP TABLE good_providers_tmp_imm1`)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`DELETE FROM good_providers`)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO good_providers SELECT * FROM good_providers_tmp`)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`DROP TABLE good_providers_tmp`)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
}

type dealProvider struct {
	id              int64
	ask_price       float64
	ask_verif_price float64
}

func (r *ribsDB) SelectDealProviders(group iface2.GroupKey, pieceSize int64, verified bool, maxPrice float64) ([]dealProvider, error) {
	// only reachable, with boost_deals, only ones that don't have deals for this group
	// 6 at random
	// 2 of them with booster_http
	// 2 of them with booster_bitswap

	var withHttp []dealProvider
	var withBitswap []dealProvider
	var random []dealProvider

	res, err := r.db.Query(`select id, ask_price, ask_verif_price from good_providers
									WHERE id NOT IN (
										SELECT provider_addr FROM deals	WHERE group_id = $1
										  AND (rejected = 0 OR (rejected = 1 AND start_time >= now() - interval '24 hours'))
										  AND (failed = 0 OR (rejected = 0 AND failed = 1 AND  start_time >= now() - interval '100 hours'))
									) and ask_min_piece_size <= $2 and ask_max_piece_size >= $3 order by random() limit 15`,
		group, pieceSize, pieceSize)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var id dealProvider
		err := res.Scan(&id.id, &id.ask_price, &id.ask_verif_price)
		if err != nil {
			return nil, xerrors.Errorf("scanning provider: %w", err)
		}

		if verified && id.ask_verif_price > maxPrice {
			continue
		} else if !verified && id.ask_price > maxPrice {
			continue
		}

		random = append(random, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating providers: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing providers: %w", err)
	}

	res, err = r.db.Query(`select id, ask_price, ask_verif_price from good_providers
									WHERE id NOT IN (
										SELECT provider_addr FROM deals	WHERE group_id = $1
										  AND (rejected = 0 OR (rejected = 1 AND start_time >= now() - interval '24 hours'))
										  AND (failed = 0 OR (rejected = 0 AND failed = 1 AND  start_time >= now() - interval '100 hours'))
									) and booster_http = 1 and ask_min_piece_size <= $2 and ask_max_piece_size >= $3 order by random() limit 7`, group, pieceSize, pieceSize)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}

	for res.Next() {
		var id dealProvider
		err := res.Scan(&id.id, &id.ask_price, &id.ask_verif_price)
		if err != nil {
			return nil, xerrors.Errorf("scanning provider: %w", err)
		}

		withHttp = append(withHttp, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating providers: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing providers: %w", err)
	}

	res, err = r.db.Query(`select id, ask_price, ask_verif_price from good_providers
									WHERE id NOT IN (
										SELECT provider_addr FROM deals	WHERE group_id = $1
										  AND (rejected = 0 OR (rejected = 1 AND start_time >= now() - interval '24 hours'))
										  AND (failed = 0 OR (rejected = 0 AND failed = 1 AND  start_time >= now() - interval '100 hours'))
									) and booster_bitswap = 1 and ask_min_piece_size <= $2 and ask_max_piece_size >= $3 order by random() limit 7`, group, pieceSize, pieceSize)
	if err != nil {
		return nil, xerrors.Errorf("querying providers: %w", err)
	}

	for res.Next() {
		var id dealProvider
		err := res.Scan(&id.id, &id.ask_price, &id.ask_verif_price)
		if err != nil {
			return nil, xerrors.Errorf("scanning provider: %w", err)
		}

		withBitswap = append(withBitswap, id)
	}

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating providers: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing providers: %w", err)
	}

	out := make([]dealProvider, 0, 14)
	out = append(out, withHttp...)
	out = append(out, withBitswap...)
	out = append(out, random...)

	// dedup
	seen := make(map[int64]dealProvider)
	for _, p := range out {
		if _, ok := seen[p.id]; ok {
			continue
		}
		seen[p.id] = p
	}

	out = make([]dealProvider, 0, 9)
	for _, p := range seen {
		out = append(out, p)
		// trim to 9
		if len(out) == 9 {
			break
		}
	}

	return out, nil
}

func (r *ribsDB) ReachableProviders() []iface2.ProviderMeta {
	rp, err := r.reachableCq.Get()
	if err != nil {
		log.Errorw("getting reachable providers", "error", err)
	}

	return rp
}

func (r *ribsDB) reachableProviders() ([]iface2.ProviderMeta, error) {
	res, err := r.db.Query(`select id, ping_ok, boost_deals, booster_http, booster_bitswap,
       indexed_success, indexed_fail,
       ask_price, ask_verif_price, ask_min_piece_size, ask_max_piece_size
    from providers where in_market=true and ping_ok=true`)

	if err != nil {
		log.Errorw("querying providers", "error", err)
		return nil, err
	}

	out := make([]iface2.ProviderMeta, 0)

	for res.Next() {
		var pm iface2.ProviderMeta
		err := res.Scan(&pm.ID, &pm.PingOk, &pm.BoostDeals, &pm.BoosterHttp, &pm.BoosterBitswap,
			&pm.IndexedSuccess, &pm.IndexedFail, // &pm.DealAttempts, &pm.DealSuccess, &pm.DealFail,
			&pm.AskPrice, &pm.AskVerifiedPrice, &pm.AskMinPieceSize, &pm.AskMaxPieceSize)
		if err != nil {
			log.Errorw("scanning provider", "error", err)
			return nil, err
		}

		out = append(out, pm)
	}

	if err := res.Err(); err != nil {
		log.Errorw("scanning providers", "error", err)
		return nil, err
	}
	if err := res.Close(); err != nil {
		log.Errorw("closing providers", "error", err)
		return nil, err
	}

	res, err = r.db.Query(`select provider_addr, count(*),
       sum(case when sealed = 1 then 1 else 0 end),
       sum(case when rejected != 1 and failed = 1 then 1 else 0 end),
       sum(case when rejected = 1 then 1 else 0 end),
       max(start_time) from deals group by provider_addr`)
	if err != nil {
		log.Errorw("querying deals", "error", err)
		return nil, err
	}

	for res.Next() {
		var id int64
		var dealStarted, dealSuccess, dealFail, dealRejected int64
		var maxStart time.Time
		err := res.Scan(&id, &dealStarted, &dealSuccess, &dealFail, &dealRejected, &maxStart)
		if err != nil {
			log.Errorw("scanning deal", "error", err)
			return nil, err
		}

		for i := range out { // todo O(n^2)
			if out[i].ID == id {
				out[i].DealStarted = dealStarted
				out[i].DealSuccess = dealSuccess
				out[i].DealFail = dealFail
				out[i].DealRejected = dealRejected
				out[i].MostRecentDealStart = maxStart.Unix()
			}
		}
	}

	if err := res.Err(); err != nil {
		log.Errorw("scanning providers", "error", err)
		return nil, err
	}
	if err := res.Close(); err != nil {
		log.Errorw("closing providers", "error", err)
		return nil, err
	}

	res, err = r.db.Query(`select sp_id, retrievable_deals, unretrievable_deals from sp_retr_stats_view`)
	if err != nil {
		log.Errorw("querying deals", "error", err)
		return nil, err
	}
	for res.Next() {
		var id int64
		var retr, unretr int64
		err := res.Scan(&id, &retr, &unretr)
		if err != nil {
			log.Errorw("scanning deal retr stats", "error", err)
			return nil, err
		}

		for i := range out { // todo O(n^2)
			if out[i].ID == id {
				out[i].RetrievDeals = retr
				out[i].UnretrievDeals = unretr
			}
		}
	}

	if err := res.Err(); err != nil {
		log.Errorw("scanning providers", "error", err)
		return nil, err
	}
	if err := res.Close(); err != nil {
		log.Errorw("closing providers", "error", err)
		return nil, err
	}

	sort.SliceStable(out, func(i, j int) bool {
		iRejectAll := out[i].DealRejected == out[i].DealStarted
		jRejectAll := out[j].DealRejected == out[j].DealStarted
		if iRejectAll != jRejectAll {
			return iRejectAll
		}

		iRetrSuccess := (out[i].RetrievDeals+out[i].UnretrievDeals) > 0 && float64(out[i].RetrievDeals) >= 0.7*float64(out[i].RetrievDeals+out[i].UnretrievDeals)
		jRetrSuccess := (out[j].RetrievDeals+out[j].UnretrievDeals) > 0 && float64(out[j].RetrievDeals) >= 0.7*float64(out[j].RetrievDeals+out[j].UnretrievDeals)

		if iRetrSuccess != jRetrSuccess {
			return !iRetrSuccess
		}

		if out[i].DealSuccess == out[j].DealSuccess {
			return out[i].DealStarted < out[j].DealStarted
		}

		return out[i].DealSuccess < out[j].DealSuccess
	})

	return out, nil
}

func (r *ribsDB) GetNonFailedDealCount(group iface2.GroupKey) (int, int, error) {
	var count int
	var unretrievable int
	err := r.db.QueryRow(`select count(*), count(*) filter ( where  last_retrieval_check > (last_retrieval_check_success + 3600*24) and retrieval_probes_fail > 10) from deals where group_id = $1 and failed = 0`, group).Scan(&count, &unretrievable)
	if err != nil {
		return 0, 0, xerrors.Errorf("querying deal count: %w", err)
	}

	return count, unretrievable, nil
}

type dbDealInfo struct {
	DealUUID string
	GroupID  iface2.GroupKey

	ClientAddr   string
	ProviderAddr int64

	PricePerEpoch int64
	Verified      bool
	KeepUnsealed  bool

	StartEpoch abi.ChainEpoch
	EndEpoch   abi.ChainEpoch

	SignedProposalBytes []byte
}

func (r *ribsDB) StoreDealProposal(d dbDealInfo) error {
	_, err := r.db.Exec(`insert into deals (uuid, client_addr, provider_addr, group_id, price_afil_gib_epoch, verified, keep_unsealed, start_epoch, end_epoch, signed_proposal_bytes) values
                                   ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, d.DealUUID, d.ClientAddr, d.ProviderAddr, d.GroupID, d.PricePerEpoch, d.Verified, d.KeepUnsealed, d.StartEpoch, d.EndEpoch, d.SignedProposalBytes)
	if err != nil {
		return xerrors.Errorf("inserting deal: %w", err)
	}

	return nil
}

func (r *ribsDB) StoreSuccessfullyProposedDeal(d dbDealInfo) error {
	proposed := 1

	_, err := r.db.Exec(`update deals set proposed = $1 where uuid = $2`,
		proposed, d.DealUUID)
	if err != nil {
		return xerrors.Errorf("updating deal: %w", err)
	}

	return nil
}

func (r *ribsDB) StoreRejectedDeal(duuid string, emsg string, proposed int) error {
	failed, rejected := 1, 1
	state := "Rejected"

	_, err := r.db.Exec(`update deals set failed = $1, rejected = $2, sp_status = $3, error_msg = $4, proposed = $5 where uuid = $6`,
		failed, rejected, state, emsg, proposed, duuid)
	if err != nil {
		return xerrors.Errorf("updating deal: %w", err)
	}

	return nil
}

func (r *ribsDB) UpdateSPDealState(id uuid.UUID, stresp *types.DealStatusResponse, qerr error) error {
	now := time.Now().Unix()
	var lastError *string

	if qerr != nil || stresp == nil {
		if qerr != nil {
			errStr := qerr.Error()
			lastError = &errStr
		} else {
			errMsg := "DealStatusResponse is nil"
			lastError = &errMsg
		}

		_, err := r.db.Exec(`update deals set
        last_state_query_error = $1
        where uuid = $2`, lastError, id)
		if err != nil {
			return xerrors.Errorf("update sp tracker: %w", err)
		}
	} else if stresp.DealStatus == nil {
		errMsg := fmt.Sprintf("DealStatus is nil (resp err: '%s')", stresp.Error)

		_, err := r.db.Exec(`update deals set
                 sp_status = $1,
                 error_msg = $2,
                 last_state_query_error = $3,
                 last_state_query = $4
             where uuid = $5`, "Aborted", errMsg, errMsg, now, id)
		if err != nil {
			return xerrors.Errorf("update sp tracker: %w", err)
		}
	} else {
		var pubCid *string
		if stresp.DealStatus.PublishCid != nil {
			s := stresp.DealStatus.PublishCid.String()
			pubCid = &s
		}

		_, err := r.db.Exec(`update deals set
        sp_status = $1,
        error_msg = $2,
        sp_sealing_status = $3,
        sp_sig_proposal = $4,
        sp_pub_msg_cid = $5,
        sp_recv_bytes = CASE WHEN $6 > COALESCE(sp_recv_bytes, 0) THEN $7 ELSE sp_recv_bytes END,
        sp_txsize = $8,
        last_state_query = $9,
        last_state_query_error = $10
        where uuid = $11`, stresp.DealStatus.Status, stresp.DealStatus.Error, stresp.DealStatus.SealingStatus,
			stresp.DealStatus.SignedProposalCid.String(), pubCid,
			stresp.NBytesReceived, stresp.NBytesReceived, stresp.TransferSize, now, lastError, id)
		if err != nil {
			return xerrors.Errorf("update sp tracker: %w", err)
		}
	}

	return nil
}

type inactiveDealMeta struct {
	DealUUID     string
	ProviderAddr int64
}

func (r *ribsDB) InactiveDealsToCheck() ([]inactiveDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr from deals where sealed = 0 and failed = 0`) // todo any reason to re-check failed/rejected deals?
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]inactiveDealMeta, 0)

	for res.Next() {
		var dm inactiveDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}

type publishingDealMeta struct {
	DealUUID     string
	ProviderAddr int64

	Proposal   []byte
	PublishCid string
}

func (r *ribsDB) PublishingDeals() ([]publishingDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr, signed_proposal_bytes, sp_pub_msg_cid from deals where published = 0 and failed = 0 and sp_pub_msg_cid is not null`) // todo any reason to re-check failed/rejected deals?
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]publishingDealMeta, 0)

	for res.Next() {
		var dm publishingDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr, &dm.Proposal, &dm.PublishCid)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}
func (r *ribsDB) AllUnpublishedDeals() ([]publishingDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr, signed_proposal_bytes, coalesce(sp_pub_msg_cid, '') from deals where published = 0 and failed = 0`)
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]publishingDealMeta, 0)

	for res.Next() {
		var dm publishingDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr, &dm.Proposal, &dm.PublishCid)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}

func (r *ribsDB) UpdatePublishedDealLight(id string, dealID abi.DealID) error {
	_, err := r.db.Exec(`update deals set deal_id = $1, published = 1 where uuid = $2`, dealID, id)
	if err != nil {
		return xerrors.Errorf("update activated deal: %w", err)
	}

	return nil
}
func (r *ribsDB) UpdatePublishedDeal(id string, dealID abi.DealID, pubTs types2.TipSetKey) error {
	_, err := r.db.Exec(`update deals set deal_id = $1, deal_pub_ts = $2, published = 1 where uuid = $3`, dealID, pubTs.String(), id)
	if err != nil {
		return xerrors.Errorf("update activated deal: %w", err)
	}

	return nil
}

type publishedDealMeta struct {
	DealUUID     string
	ProviderAddr int64

	Proposal   []byte
	PublishCid string
	DealID     abi.DealID
}

func (r *ribsDB) PublishedDeals() ([]publishedDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr, signed_proposal_bytes, coalesce(sp_pub_msg_cid, ''), deal_id from deals where published = 1 and sealed = 0 and failed = 0 and sp_pub_msg_cid is not null`) // todo any reason to re-check failed/rejected deals?
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]publishedDealMeta, 0)

	for res.Next() {
		var dm publishedDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr, &dm.Proposal, &dm.PublishCid, &dm.DealID)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}
func (r *ribsDB) AllPublishedUnsealedDeals() ([]publishedDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr, signed_proposal_bytes, coalesce(sp_pub_msg_cid, ''), deal_id from deals where published = 1 and sealed = 0 and failed = 0`) // todo any reason to re-check failed/rejected deals?
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]publishedDealMeta, 0)

	for res.Next() {
		var dm publishedDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr, &dm.Proposal, &dm.PublishCid, &dm.DealID)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}
func (r *ribsDB) AllActiveDeals() ([]publishedDealMeta, error) {
	res, err := r.db.Query(`select uuid, provider_addr, signed_proposal_bytes, coalesce(sp_pub_msg_cid, ''), deal_id from deals where published = 1 and sealed = 1 and failed = 0`) // todo any reason to re-check failed/rejected deals?
	if err != nil {
		return nil, xerrors.Errorf("querying deals: %w", err)
	}
	defer res.Close()

	out := make([]publishedDealMeta, 0)

	for res.Next() {
		var dm publishedDealMeta
		err := res.Scan(&dm.DealUUID, &dm.ProviderAddr, &dm.Proposal, &dm.PublishCid, &dm.DealID)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		out = append(out, dm)
	}

	return out, nil
}

func (r *ribsDB) UpdateActivatedDeal(id string, sectorStart abi.ChainEpoch) error {
	_, err := r.db.Exec(`update deals set sector_start_epoch = $1, sealed = 1 where uuid = $2`, sectorStart, id)
	if err != nil {
		return xerrors.Errorf("update activated deal: %w", err)
	}

	return nil
}

func (r *ribsDB) MarkExpiredDeals(currentEpoch int64) error {
	query := `
		UPDATE deals
		SET failed = 1,
			failed_expired = 1,
			published = 0,
			sp_pub_msg_cid = null
		WHERE failed = 0 AND sealed = 0
			AND start_epoch < $1;
	`

	result, err := r.db.Exec(query, currentEpoch)
	if err != nil {
		return fmt.Errorf("error marking expired deals: %w", err)
	}

	affectedRows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error getting affected rows: %w", err)
	}

	log.Infow("Marked expired deals", "affectedRows", affectedRows)

	return nil
}

func (r *ribsDB) GetDealStartEpoch(uuid string) (abi.ChainEpoch, error) {
	var startEpoch abi.ChainEpoch
	err := r.db.QueryRow(`SELECT start_epoch FROM deals WHERE uuid = $1`, uuid).Scan(&startEpoch)
	if err != nil {
		return 0, xerrors.Errorf("getting start_epoch by uuid: %w", err)
	}

	return startEpoch, nil
}

func (r *ribsDB) UpdateExpiredDeal(id string) error {
	_, err := r.db.Exec(`update deals set failed = 1, failed_expired = 1 where uuid = $1`, id)
	if err != nil {
		return xerrors.Errorf("update activated deal: %w", err)
	}

	return nil
}

func (r *ribsDB) DealSummary() (iface2.DealSummary, error) {
	return r.dealSummaryCq.Get()
}

func (r *ribsDB) dealSummary() (iface2.DealSummary, error) {

	res, err := r.db.Query(`WITH deal_summary AS (
    SELECT
        d.group_id,
        SUM(CASE WHEN d.failed = 0 THEN g.car_size ELSE 0 END) AS total_data_size,
        SUM(CASE WHEN d.failed = 0 THEN g.piece_size ELSE 0 END) AS total_deal_size,
        SUM(CASE WHEN d.failed = 0 AND d.sealed = 1 THEN g.car_size ELSE 0 END) AS stored_data_size,
        SUM(CASE WHEN d.failed = 0 AND d.sealed = 1 THEN g.piece_size ELSE 0 END) AS stored_deal_size,
        COUNT(CASE WHEN d.failed = 0 AND d.sealed = 0 THEN 1 ELSE NULL END) AS deals_in_progress,
        COUNT(CASE WHEN d.sealed = 1 THEN 1 ELSE NULL END) AS deals_done,
        COUNT(CASE WHEN d.failed = 1 THEN 1 ELSE NULL END) AS deals_failed
    FROM
        deals d
            JOIN
        groups g ON d.group_id = g.id
    GROUP BY
        d.group_id
)
SELECT
    COALESCE(SUM(deals_in_progress+deals_done), 0) AS total_non_failed_deal_count,
    COALESCE(SUM(total_data_size), 0) AS total_data_size,
    COALESCE(SUM(total_deal_size), 0) AS total_deal_size,
    COALESCE(SUM(stored_data_size), 0) AS stored_data_size,
    COALESCE(SUM(stored_deal_size), 0) AS stored_deal_size,
    COALESCE(SUM(deals_in_progress), 0) AS deals_in_progress,
    COALESCE(SUM(deals_done), 0) AS deals_done,
    COALESCE(SUM(deals_failed), 0) AS deals_failed
FROM
    deal_summary
;`)
	if err != nil {
		return iface2.DealSummary{}, xerrors.Errorf("finding deal summary: %w", err)
	}
	defer res.Close()

	var ds iface2.DealSummary

	if res.Next() {
		err := res.Scan(&ds.NonFailed, &ds.TotalDataSize, &ds.TotalDealSize,
			&ds.StoredDataSize, &ds.StoredDealSize,
			&ds.InProgress, &ds.Done, &ds.Failed)
		if err != nil {
			return iface2.DealSummary{}, xerrors.Errorf("scanning group: %w", err)
		}
	}

	return ds, nil
}

func (r *ribsDB) ProviderInfo(providerID int64) (iface2.ProviderInfo, error) {
	var pInfo iface2.ProviderInfo
	err := r.db.QueryRow(`
		SELECT p.id, p.ping_ok, p.boost_deals, p.booster_http, p.booster_bitswap,
		p.indexed_success, p.indexed_fail, p.retrprobe_success, p.retrprobe_fail, p.retrprobe_blocks, p.retrprobe_bytes,
		p.ask_price, p.ask_verif_price, p.ask_min_piece_size, p.ask_max_piece_size,
		COALESCE(ds.deal_started, 0), COALESCE(ds.deal_success, 0), COALESCE(ds.deal_fail, 0), COALESCE(ds.deal_rejected, 0), COALESCE(ds.most_recent_deal_start, 0),
		COALESCE(rs.retrievable_deals, 0), COALESCE(rs.unretrievable_deals, 0)
		FROM providers p
		LEFT JOIN (
			SELECT provider_addr,
				count(*) AS deal_started,
				sum(case when sealed = 1 then 1 else 0 end) AS deal_success,
				sum(case when rejected != 1 and failed = 1 then 1 else 0 end) AS deal_fail,
				sum(case when rejected = 1 then 1 else 0 end) AS deal_rejected,
				COALESCE(extract(epoch from max(start_time))::bigint, 0) AS most_recent_deal_start
			FROM deals
			WHERE provider_addr = $1
			GROUP BY provider_addr
		) ds ON p.id = ds.provider_addr
		LEFT JOIN sp_retr_stats_view rs ON p.id = rs.sp_id
		WHERE p.id = $1`, providerID).Scan(
		&pInfo.Meta.ID, &pInfo.Meta.PingOk, &pInfo.Meta.BoostDeals,
		&pInfo.Meta.BoosterHttp, &pInfo.Meta.BoosterBitswap, &pInfo.Meta.IndexedSuccess,
		&pInfo.Meta.IndexedFail, &pInfo.Meta.RetrProbeSuccess, &pInfo.Meta.RetrProbeFail, &pInfo.Meta.RetrProbeBlocks, &pInfo.Meta.RetrProbeBytes,
		&pInfo.Meta.AskPrice, &pInfo.Meta.AskVerifiedPrice, &pInfo.Meta.AskMinPieceSize, &pInfo.Meta.AskMaxPieceSize,
		&pInfo.Meta.DealStarted, &pInfo.Meta.DealSuccess, &pInfo.Meta.DealFail, &pInfo.Meta.DealRejected, &pInfo.Meta.MostRecentDealStart,
		&pInfo.Meta.RetrievDeals, &pInfo.Meta.UnretrievDeals)
	if err != nil {
		return pInfo, xerrors.Errorf("querying provider metadata: %w", err)
	}

	res, err := r.db.Query(`select uuid, provider_addr, group_id, verified, keep_unsealed,
		sealed, failed, rejected, deal_id, sp_status, sp_sealing_status, error_msg,
		sp_recv_bytes, sp_txsize, sp_pub_msg_cid, start_epoch, end_epoch, start_time,
		retrieval_probes_success, retrieval_probes_fail, retrieval_probe_prev_ttfb_ms,
		last_retrieval_check > 0 AND last_retrieval_check > (last_retrieval_check_success + 3600*24) as no_recent_retr
		from deals where provider_addr = $1 ORDER BY start_time DESC LIMIT 100`, providerID)
	if err != nil {
		return pInfo, xerrors.Errorf("getting group meta: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var dealUuid string
		var provider int64
		var groupID iface2.GroupKey
		var verified bool
		var keepUnsealed bool
		var sealed, failed, rejected bool
		var startEpoch, endEpoch int64
		var startTime time.Time
		var status *string
		var sealStatus *string
		var errMsg *string
		var bytesRecv *int64
		var txSize *int64
		var pubCid *string
		var dealID *int64
		var retrievalProbesSuccess *int64
		var retrievalProbesFail *int64
		var retrievalProbeTTFBMS *int64
		var noRecentRetrievalSuccess bool

		err := res.Scan(&dealUuid, &provider, &groupID, &verified, &keepUnsealed, &sealed, &failed, &rejected, &dealID, &status, &sealStatus, &errMsg, &bytesRecv, &txSize, &pubCid, &startEpoch, &endEpoch, &startTime, &retrievalProbesSuccess, &retrievalProbesFail, &retrievalProbeTTFBMS, &noRecentRetrievalSuccess)
		if err != nil {
			return pInfo, xerrors.Errorf("scanning deal: %w", err)
		}

		pInfo.RecentDeals = append(pInfo.RecentDeals, iface2.DealMeta{
			UUID:            dealUuid,
			Provider:        provider,
			GroupID:         groupID,
			Verified:        verified,
			KeepUnsealed:    keepUnsealed,
			Sealed:          sealed,
			Failed:          failed,
			Rejected:        rejected,
			StartEpoch:      startEpoch,
			EndEpoch:        endEpoch,
			StartTime:       startTime.Unix(),
			Status:          DerefOr(status, ""),
			SealStatus:      DerefOr(sealStatus, ""),
			Error:           DerefOr(errMsg, ""),
			DealID:          DerefOr(dealID, 0),
			BytesRecv:       DerefOr(bytesRecv, 0),
			TxSize:          DerefOr(txSize, 0),
			PubCid:          DerefOr(pubCid, ""),
			RetrSuccess:     DerefOr(retrievalProbesSuccess, 0),
			RetrFail:        DerefOr(retrievalProbesFail, 0),
			RetrTTFBMs:      DerefOr(retrievalProbeTTFBMS, 0),
			NoRecentSuccess: noRecentRetrievalSuccess,
		})
	}

	if err := res.Err(); err != nil {
		return iface2.ProviderInfo{}, err
	}
	if err := res.Close(); err != nil {
		return iface2.ProviderInfo{}, err
	}

	return pInfo, nil
}

type dealParams struct {
	CommP     []byte
	Root      cid.Cid
	PieceSize int64
	CarSize   int64
}

func (r *ribsDB) GetDealParams(ctx context.Context, id iface2.GroupKey) (out dealParams, err error) {
	res, err := r.db.QueryContext(ctx, "select commp, root, piece_size, car_size from groups where id = $1", id)
	if err != nil {
		return dealParams{}, xerrors.Errorf("finding deal params: %w", err)
	}
	defer res.Close()

	var found bool

	if res.Next() {
		var commp, root []byte
		var pieceSize, carSize int64
		err := res.Scan(&commp, &root, &pieceSize, &carSize)
		if err != nil {
			return dealParams{}, xerrors.Errorf("scanning group: %w", err)
		}

		out.CommP = commp
		_, out.Root, err = cid.CidFromBytes(root)
		if err != nil {
			return dealParams{}, xerrors.Errorf("parsing cid: %w", err)
		}
		out.PieceSize = pieceSize
		out.CarSize = carSize

		found = true
	}

	if err := res.Err(); err != nil {
		return dealParams{}, xerrors.Errorf("iterating groups: %w", err)
	}
	if err := res.Close(); err != nil {
		return dealParams{}, xerrors.Errorf("closing group iterator: %w", err)
	}
	if !found {
		return dealParams{}, xerrors.Errorf("group %d not found", id)
	}

	return out, nil
}

func (r *ribsDB) UpsertMarketActors(actors []int64) error {
	/*_, err := r.db.Exec(`
	begin transaction;
	    update providers set in_market = 0 where in_market = 1;
	    insert into providers (address, in_market) values (?, 1) on conflict (address) do update set in_market = 1;
	end transaction;
	`, actors)*/

	tx, err := r.db.Begin()
	if err != nil {
		return xerrors.Errorf("begin transaction: %w", err)
	}

	_, err = tx.Exec("update providers set in_market = false where in_market = true")
	if err != nil {
		if err := tx.Rollback(); err != nil {
			log.Errorw("rollback UpsertMarketActors", "error", err)
		}
		return xerrors.Errorf("reset in_market: %w", err)
	}

	stmt, err := tx.Prepare("insert into providers (id, in_market) values ($1, true) on conflict (id) do update set in_market = true")
	if err != nil {
		if err := tx.Rollback(); err != nil {
			log.Errorw("rollback UpsertMarketActors", "error", err)
		}
		return xerrors.Errorf("prepare statement: %w", err)
	}

	for _, actor := range actors {
		_, err = stmt.Exec(actor)
		if err != nil {
			if err := tx.Rollback(); err != nil {
				log.Errorw("rollback UpsertMarketActors", "error", err)
			}
			return xerrors.Errorf("insert actor: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return xerrors.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *ribsDB) UpdateProviderProtocols(provider int64, pres providerResult) error {

	var LibP2PMaddrsJson string
	var BitswapMaddrsJson string
	var HttpMaddrsJson string

	if len(pres.LibP2PMaddrs) > 0 {
		a, err := json.Marshal(pres.LibP2PMaddrs)
		if err != nil {
			return xerrors.Errorf("marshal libp2p maddrs: %w", err)
		}
		LibP2PMaddrsJson = string(a)
	}
	if len(pres.BitswapMaddrs) > 0 {
		a, err := json.Marshal(pres.BitswapMaddrs)
		if err != nil {
			return xerrors.Errorf("marshal bitswap maddrs: %w", err)
		}
		BitswapMaddrsJson = string(a)
	}
	if len(pres.HttpMaddrs) > 0 {
		a, err := json.Marshal(pres.HttpMaddrs)
		if err != nil {
			return xerrors.Errorf("marshal http maddrs: %w", err)
		}
		HttpMaddrsJson = string(a)
	}

	_, err := r.db.Exec(`
	update providers set ping_ok = $1, boost_deals = $2, booster_http = $3, booster_bitswap = $4, addr_info_graphsync = $5, addr_info_bitswap = $6, addr_info_http = $7 where id = $8;
	`, pres.PingOk, pres.BoostDeals, pres.BoosterHttp, pres.BoosterBitswap, LibP2PMaddrsJson, BitswapMaddrsJson, HttpMaddrsJson,
		provider)
	if err != nil {
		return xerrors.Errorf("update provider: %w", err)
	}

	return nil
}

func (r *ribsDB) UpdateProviderStorageAsk(provider int64, ask *storagemarket.StorageAsk) error {
	_, err := r.db.Exec(`
	update providers set ask_price = $1, ask_verif_price = $2, ask_min_piece_size = $3, ask_max_piece_size = $4, ask_ok = true where id = $5;
	`, ask.Price.String(), ask.VerifiedPrice.String(), ask.MinPieceSize, ask.MaxPieceSize, provider)
	if err != nil {
		return xerrors.Errorf("update provider: %w", err)
	}

	return nil
}

func (r *ribsDB) GroupDeals(gk iface2.GroupKey) ([]iface2.DealMeta, error) {
	dealMeta := make([]iface2.DealMeta, 0)

	res, err := r.db.Query(`select uuid, provider_addr, sealed, failed, rejected, deal_id,
										sp_status, sp_sealing_status, error_msg, sp_recv_bytes, sp_txsize, sp_pub_msg_cid, start_epoch, end_epoch,
										retrieval_probes_success, retrieval_probes_fail, retrieval_probe_prev_ttfb_ms,
										last_retrieval_check > 0 AND last_retrieval_check > (last_retrieval_check_success + 3600*24) as no_recent_retr
										from deals where group_id = $1`, gk)
	if err != nil {
		return nil, xerrors.Errorf("getting group meta: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var dealUuid string
		var provider int64
		var sealed, failed, rejected bool
		var startEpoch, endEpoch int64
		var status *string
		var sealStatus *string
		var errMsg *string
		var bytesRecv *int64
		var txSize *int64
		var pubCid *string
		var dealID *int64
		var retrievalProbesSuccess *int64
		var retrievalProbesFail *int64
		var retrievalProbeTTFBMS *int64
		var noRecentRetrievalSuccess bool

		err := res.Scan(&dealUuid, &provider, &sealed, &failed, &rejected, &dealID, &status, &sealStatus, &errMsg, &bytesRecv, &txSize, &pubCid, &startEpoch, &endEpoch, &retrievalProbesSuccess, &retrievalProbesFail, &retrievalProbeTTFBMS, &noRecentRetrievalSuccess)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}

		dealMeta = append(dealMeta, iface2.DealMeta{
			UUID:       dealUuid,
			Provider:   provider,
			Sealed:     sealed,
			Failed:     failed,
			Rejected:   rejected,
			StartEpoch: startEpoch,
			EndEpoch:   endEpoch,
			Status:     DerefOr(status, ""),
			SealStatus: DerefOr(sealStatus, ""),
			Error:      DerefOr(errMsg, ""),
			DealID:     DerefOr(dealID, 0),
			BytesRecv:  DerefOr(bytesRecv, 0),
			TxSize:     DerefOr(txSize, 0),
			PubCid:     DerefOr(pubCid, ""),

			RetrTTFBMs:  DerefOr(retrievalProbeTTFBMS, 0),
			RetrSuccess: DerefOr(retrievalProbesSuccess, 0),
			RetrFail:    DerefOr(retrievalProbesFail, 0),

			NoRecentSuccess: noRecentRetrievalSuccess,
		})
	}

	sort.SliceStable(dealMeta, func(i, j int) bool {
		return (dealMeta[i].Sealed && !dealMeta[j].Sealed) || (!dealMeta[i].Failed && dealMeta[j].Failed)
	})

	if err := res.Err(); err != nil {
		return nil, xerrors.Errorf("iterating deals: %w", err)
	}

	if err := res.Close(); err != nil {
		return nil, xerrors.Errorf("closing deals iterator: %w", err)
	}

	return dealMeta, nil
}

type noSectorDealInfo struct {
	UUID     string
	Provider int64
	DealID   int64
}

func (r *ribsDB) GetSealedDealsWithNoSectorNums() ([]noSectorDealInfo, error) {
	res, err := r.db.Query(`select uuid, provider_addr, deal_id from deals where deal_id is not null and sector_number is null order by provider_addr limit 1000`)
	if err != nil {
		return nil, xerrors.Errorf("getting deals: %w", err)
	}
	defer res.Close()

	var out []noSectorDealInfo
	for res.Next() {
		var d noSectorDealInfo
		if err := res.Scan(&d.UUID, &d.Provider, &d.DealID); err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}
		out = append(out, d)
	}

	return out, nil
}

func (r *ribsDB) FillDealSectorNumber(uuid string, sectorNum abi.SectorNumber) error {
	_, err := r.db.Exec(`update deals set sector_number = $1 where uuid = $2`, sectorNum, uuid)
	if err != nil {
		return xerrors.Errorf("updating deal: %w", err)
	}

	return nil
}

type GroupDealStats struct {
	GroupID        int64
	State          iface2.GroupState
	TotalDeals     int64
	PublishedDeals int64
	SealedDeals    int64
	FailedDeals    int64
	RejectedDeals  int64
	Retrievable    int64
	Unretrievable  int64
}

func (r *ribsDB) GetGroupDealStats() (map[int64]GroupDealStats, error) {
	query := `
        SELECT
    g.id AS group_id,
    g.g_state AS group_state,
    COUNT(d.group_id) AS total_deals,
    COUNT(CASE WHEN d.published = 1 THEN 1 ELSE NULL END) AS published_deals,
    COUNT(CASE WHEN d.sealed = 1 THEN 1 ELSE NULL END) AS sealed_deals,
    COUNT(CASE WHEN d.failed = 1 THEN 1 ELSE NULL END) AS failed_deals,
    COUNT(CASE WHEN d.rejected = 1 THEN 1 ELSE NULL END) AS rejected_deals,
    COUNT(CASE WHEN d.failed = 0 AND d.last_retrieval_check > 0 AND d.last_retrieval_check < (d.last_retrieval_check_success + 3600*24) THEN 1 ELSE NULL END) AS retrievable_deals,
    COUNT(CASE WHEN d.failed = 0 AND d.last_retrieval_check > 0 AND d.last_retrieval_check > (d.last_retrieval_check_success + 3600*24) THEN 1 ELSE NULL END) AS unretrievable_deals
FROM
    groups g
        LEFT JOIN
    deals d ON g.id = d.group_id
GROUP BY
    g.id;`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, xerrors.Errorf("fetch group deal stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[int64]GroupDealStats)
	for rows.Next() {
		var s GroupDealStats
		err := rows.Scan(&s.GroupID, &s.State, &s.TotalDeals, &s.PublishedDeals, &s.SealedDeals, &s.FailedDeals, &s.RejectedDeals, &s.Retrievable, &s.Unretrievable)
		if err != nil {
			return nil, xerrors.Errorf("scan group deal stats: %w", err)
		}
		stats[s.GroupID] = s
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterate group deal stats: %w", err)
	}

	return stats, nil
}

func DerefOr[T any](v *T, def T) T {
	if v == nil {
		return def
	}
	return *v
}

/* type TransferInfo struct {
	Failed                 int
	CarTransferAttempts    int
	CarTransferStartTime   *int64
	CarTransferLastBytes   *int64
	CarTransferLastEndTime *int64
}

func (r *ribsDB) GetTransferStatusByDealUUID(dealUUID uuid.UUID) (*TransferInfo, error) {
	var transferInfo TransferInfo

	err := r.db.QueryRow(`select failed, car_transfer_attempts, car_transfer_start_time, car_transfer_last_bytes, car_transfer_last_end_time from deals where uuid = ?`, dealUUID).Scan(&transferInfo.Failed, &transferInfo.CarTransferAttempts, &transferInfo.CarTransferStartTime, &transferInfo.CarTransferLastBytes, &transferInfo.CarTransferLastEndTime)
	if err != nil {
		return nil, xerrors.Errorf("getting transfer status: %w", err)
	}

	if transferInfo.CarTransferAttempts == 0 {
		_, err := r.db.Exec(`update deals set car_transfer_start_time = ? where uuid = ?`, time.Now().Unix(), dealUUID)
		if err != nil {
			return nil, xerrors.Errorf("setting transfer start time: %w", err)
		}
	}

	return &transferInfo, nil
}

func (r *ribsDB) UpdateTransferStats(dealUUID uuid.UUID, lastBytes int64, abortError error) error {
	failed := 0
	errorMsg := ""

	if abortError != nil {
		failed = 1
		errorMsg = abortError.Error()
	}

	_, err := r.db.Exec(`update deals set car_transfer_last_end_time = ?, car_transfer_last_bytes = ?, car_transfer_attempts = car_transfer_attempts + 1, failed = ?, error_msg = CASE WHEN error_msg = '' THEN ? ELSE error_msg END where uuid = ?`, time.Now().Unix(), lastBytes, failed, errorMsg, dealUUID)
	if err != nil {
		return xerrors.Errorf("updating transfer stats: %w", err)
	}

	return nil
} */

type RetrCheckCandidate struct {
	DealID   string
	Provider int64
	Group    int64
	Verified bool
	FastRetr bool
}

func (r *ribsDB) GetRetrievalCheckCandidates() ([]RetrCheckCandidate, error) {
	const secondsIn6Hours = 6 * 60 * 60
	now := time.Now().Unix()

	rows, err := r.db.Query(`
		SELECT uuid, provider_addr, group_id, verified, keep_unsealed FROM deals
		WHERE sealed = 1
		AND failed = 0
		AND last_retrieval_check <= $1`,
		now-secondsIn6Hours)
	if err != nil {
		return nil, xerrors.Errorf("getting retrieval check candidates: %w", err)
	}
	defer rows.Close()

	var deals []RetrCheckCandidate
	for rows.Next() {
		var deal RetrCheckCandidate
		// Assuming Deal is a struct that can scan all columns from the deals table
		err := rows.Scan(&deal.DealID, &deal.Provider, &deal.Group, &deal.Verified, &deal.FastRetr)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}
		deals = append(deals, deal)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating rows: %w", err)
	}

	return deals, nil
}

type RetrievalResult struct {
	Success bool
	Error   string

	Duration        time.Duration
	TimeToFirstByte time.Duration
}

func (r *ribsDB) RecordRetrievalCheckResult(dealId string, res RetrievalResult) error {
	// Convert durations to milliseconds for storing in the database.
	durationMs := int(res.Duration / time.Millisecond)
	ttfbMs := int(res.TimeToFirstByte / time.Millisecond)

	// Determine success or failure count increment.
	successIncrement := 0
	if res.Success {
		successIncrement = 1
	}

	// Prepare error message string.
	var errMsg *string
	if res.Error != "" {
		errMsg = &res.Error
	}

	//todo
	_, err := r.db.Exec(`
        UPDATE deals SET
            last_retrieval_check = extract(epoch from now())::bigint,
            last_retrieval_check_success = CASE WHEN $1 THEN extract(epoch from now())::bigint ELSE last_retrieval_check_success END,
            retrieval_probe_prev_ms = $2,
            retrieval_probe_prev_ttfb_ms = $3,
            retrieval_probes_success = retrieval_probes_success + $4,
            retrieval_probes_fail = retrieval_probes_fail + $5,
            retrieval_probe_prev_error = $6
        WHERE uuid = $7`,
		res.Success, durationMs, ttfbMs, successIncrement, 1-successIncrement, errMsg, dealId)

	if err != nil {
		return xerrors.Errorf("updating retrieval check result: %w", err)
	}

	return nil
}

type RetrCandidate struct {
	DealID                    string
	Provider                  int64
	Verified                  bool
	FastRetr                  bool
	LastRetrievalCheckSuccess int64
}

func (r *ribsDB) GetRetrievalCandidates(group iface2.GroupKey) ([]RetrCandidate, error) {
	rows, err := r.db.Query(`
		SELECT uuid, provider_addr, verified, keep_unsealed, last_retrieval_check_success FROM deals
		WHERE group_id = $1 AND sealed = 1 AND failed = 0 order by retrieval_probe_prev_ttfb_ms asc, last_retrieval_check_success desc, keep_unsealed desc`,
		group)
	if err != nil {
		return nil, xerrors.Errorf("getting retrieval candidates: %w", err)
	}
	defer rows.Close()

	var deals []RetrCandidate
	for rows.Next() {
		var deal RetrCandidate
		// Assuming Deal is a struct that can scan all columns from the deals table
		err := rows.Scan(&deal.DealID, &deal.Provider, &deal.Verified, &deal.FastRetr, &deal.LastRetrievalCheckSuccess)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal: %w", err)
		}
		deals = append(deals, deal)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating rows: %w", err)
	}

	return deals, nil
}

type ProviderAddrInfo struct {
	LibP2PMaddrs  []multiaddr.Multiaddr
	BitswapMaddrs []multiaddr.Multiaddr
	HttpMaddrs    []multiaddr.Multiaddr
}

func (r *ribsDB) GetProviderAddrs(provider int64) (ProviderAddrInfo, error) {
	/*
	   addr_info_graphsync text, // json of []multiaddr.Multiaddr
	   addr_info_bitswap text,
	   addr_info_http text
	*/

	var addrInfo ProviderAddrInfo
	var addrInfoGraphsync string
	var addrInfoBitswap string
	var addrInfoHttp string

	err := r.db.QueryRow(`
		SELECT addr_info_graphsync, addr_info_bitswap, addr_info_http FROM providers
		WHERE id = $1`, provider).Scan(&addrInfoGraphsync, &addrInfoBitswap, &addrInfoHttp)
	if err != nil {
		return addrInfo, xerrors.Errorf("query: %w", err)
	}

	if addrInfoGraphsync != "" {
		var strings []string
		err = json.Unmarshal([]byte(addrInfoGraphsync), &strings)
		if err != nil {
			return addrInfo, xerrors.Errorf("unmarshal graphsync: %w", err)
		}

		for _, s := range strings {
			maddr, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				return addrInfo, xerrors.Errorf("parsing graphsync multiaddr: %w", err)
			}
			addrInfo.LibP2PMaddrs = append(addrInfo.LibP2PMaddrs, maddr)
		}
	}

	if addrInfoBitswap != "" {
		var strings []string
		err = json.Unmarshal([]byte(addrInfoBitswap), &strings)
		if err != nil {
			return addrInfo, xerrors.Errorf("unmarshal bitswap: %w", err)
		}

		for _, s := range strings {
			maddr, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				return addrInfo, xerrors.Errorf("parsing bitswap multiaddr: %w", err)
			}
			addrInfo.BitswapMaddrs = append(addrInfo.BitswapMaddrs, maddr)
		}
	}

	if addrInfoHttp != "" {
		var strings []string
		err = json.Unmarshal([]byte(addrInfoHttp), &strings)
		if err != nil {
			return addrInfo, xerrors.Errorf("unmarshal http: %w", err)
		}

		for _, s := range strings {
			maddr, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				return addrInfo, xerrors.Errorf("parsing http multiaddr: %w", err)
			}
			addrInfo.HttpMaddrs = append(addrInfo.HttpMaddrs, maddr)
		}
	}

	return addrInfo, nil
}

func (r *ribsDB) NeedExternalModule() (*string, error) {
	var module string
	err := r.db.QueryRow(`select module from external_path limit 1`).Scan(&module)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, xerrors.Errorf("XYZ: query: %w", err)
	}

	return &module, nil
}
func (r *ribsDB) GetExternalPath(group iface2.GroupKey) (*string, *string, error) {
	var module, path string
	err := r.db.QueryRow(`select module, path from external_path where group_id = $1`, group).Scan(&module, &path)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, nil
		}
		return nil, nil, xerrors.Errorf("XYZ: query: %w", err)
	}

	return &module, &path, nil
}

func (r *ribsDB) GetGroupByExternalPath(module string, path string) (*iface2.GroupKey, error) {
	var group iface2.GroupKey
	err := r.db.QueryRow(`select group_id from external_path where module = $1 and path = $2`, module, path).Scan(&group)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, xerrors.Errorf("XYZ: query: %w", err)
	}

	return &group, nil
}

func (r *ribsDB) AddExternalPath(group iface2.GroupKey, module string, path string) error {
	_, err := r.db.Exec(`insert into external_path (group_id, module, path) values ($1, $2, $3)`, group, module, path)
	if err != nil {
		return xerrors.Errorf("XYZ: exec: %w", err)
	}

	return nil
}
func (r *ribsDB) DropExternalPath(group iface2.GroupKey) error {
	_, err := r.db.Exec(`delete from external_path where group_id = $1`, group)
	if err != nil {
		return xerrors.Errorf("XYZ: exec: %w", err)
	}

	return nil
}

func (r *ribsDB) GetStagingGroupCount() (count int, err error) {
	err = r.db.QueryRow(`SELECT COUNT(*) FROM external_path`).Scan(&count)
	if err != nil {
		return 0, xerrors.Errorf("counting staging groups: %w", err)
	}
	return
}

func (r *ribsDB) LastTotalUploadedBytes() (int64, error) {
	var b *int64
	err := r.db.QueryRow(`select sum(sp_recv_bytes) from deals`).Scan(&b)
	if err != nil {
		return 0, xerrors.Errorf("querying last transferred bytes: %w", err)
	}

	if b == nil {
		return 0, nil
	}

	return *b, nil
}

func (r *ribsDB) GetRetrievableDealStats() ([]iface2.DealCountStats, error) {
	query := `SELECT
    COALESCE(retrievable_count, 0) AS "X retrievable deals",
    COUNT(*) AS "Number of groups"
FROM
    (
        SELECT
            all_groups.group_id,
            COUNT(d.group_id) AS retrievable_count
        FROM
            (SELECT DISTINCT group_id FROM deals) AS all_groups
        LEFT JOIN
            deals d ON all_groups.group_id = d.group_id AND d.last_retrieval_check > 0 AND d.last_retrieval_check < (d.last_retrieval_check_success + 3600*24)
        JOIN groups g on g.id = all_groups.group_id
        WHERE
            g.g_state in (4, 5)
        GROUP BY
            all_groups.group_id
    ) AS retrievable_deals_per_group
GROUP BY
    retrievable_count
ORDER BY
    retrievable_count;
`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, xerrors.Errorf("getting retrievable deal stats: %w", err)
	}
	defer rows.Close()

	var stats []iface2.DealCountStats
	for rows.Next() {
		var stat iface2.DealCountStats
		err := rows.Scan(&stat.Count, &stat.Groups)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal stats: %w", err)
		}
		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating rows: %w", err)
	}

	return stats, nil
}

func (r *ribsDB) GetSealedDealStats() ([]iface2.DealCountStats, error) {
	query := `SELECT
    COALESCE(retrievable_count, 0) AS "X sealed deals",
    COUNT(*) AS "Number of groups"
FROM
    (
        SELECT
            all_groups.group_id,
            COUNT(d.group_id) AS retrievable_count
        FROM
            (SELECT DISTINCT group_id FROM deals) AS all_groups
        LEFT JOIN
            deals d ON all_groups.group_id = d.group_id AND d.sealed = 1
        JOIN groups g on g.id = all_groups.group_id
        WHERE
            g.g_state in (4, 5)
        GROUP BY
            all_groups.group_id
    ) AS retrievable_deals_per_group
GROUP BY
    retrievable_count
ORDER BY
    retrievable_count;`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, xerrors.Errorf("getting sealed deal stats: %w", err)
	}
	defer rows.Close()

	var stats []iface2.DealCountStats
	for rows.Next() {
		var stat iface2.DealCountStats
		err := rows.Scan(&stat.Count, &stat.Groups)
		if err != nil {
			return nil, xerrors.Errorf("scanning deal stats: %w", err)
		}
		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("iterating rows: %w", err)
	}

	return stats, nil
}

func (r *ribsDB) AddRepairsForLowRetrievableDeals() error {
	cfg := configuration.GetConfig()
	query := `
        INSERT INTO repairs (group_id, retrievable_deals)
			SELECT
				all_groups.group_id,
				COALESCE(COUNT(d.group_id), 0)
			FROM
				(SELECT DISTINCT d.group_id FROM deals d JOIN groups g ON d.group_id = g.id WHERE g.g_state = 4) AS all_groups
			LEFT JOIN
				deals d ON all_groups.group_id = d.group_id AND d.last_retrieval_check > 0 AND d.last_retrieval_check < (d.last_retrieval_check_success + 3600*24)
			GROUP BY
				all_groups.group_id
			HAVING
				COALESCE(COUNT(d.group_id), 0) < $1
		ON CONFLICT (group_id) DO UPDATE
		SET retrievable_deals = EXCLUDED.retrievable_deals;
    `
	_, err := r.db.Exec(query, cfg.Ribs.RetrievableRepairThreshold)
	return err
}

func (r *ribsDB) AssignRepairToWorker(workerID int) (*iface2.GroupKey, error) {
	query := `
        UPDATE repairs
        SET worker = $1
        WHERE group_id = (
            SELECT group_id FROM repairs
            WHERE worker IS NULL
            ORDER BY last_attempt ASC, retrievable_deals ASC
            LIMIT 1
        )
        RETURNING group_id;
    `
	var groupID iface2.GroupKey
	err := r.db.QueryRow(query, workerID).Scan(&groupID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No rows were updated, return nil
			return nil, nil
		}
		// Other errors
		return nil, err
	}
	return &groupID, nil
}

func (r *ribsDB) GetRepairStats() (out iface2.RepairQueueStats, err error) {
	query := `
        SELECT COUNT(*), COUNT(CASE WHEN worker IS NOT NULL THEN 1 END)
        FROM repairs;
    `
	err = r.db.QueryRow(query).Scan(&out.Total, &out.Assigned)
	return out, err
}

func (r *ribsDB) GetAssignedRepairWorkByWorkerID(workerID int) ([]iface2.GroupKey, error) {
	query := `
        SELECT group_id FROM repairs
        WHERE worker = $1;
    `

	rows, err := r.db.Query(query, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var groupIDs []iface2.GroupKey
	for rows.Next() {
		var groupID iface2.GroupKey
		if err := rows.Scan(&groupID); err != nil {
			return nil, err
		}
		groupIDs = append(groupIDs, groupID)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return groupIDs, nil
}

func (r *ribsDB) DelRepair(groupID iface2.GroupKey) error {
	query := `
		DELETE FROM repairs
		WHERE group_id = $1;
	`
	_, err := r.db.Exec(query, groupID)
	return err
}

func (r *ribsDB) UpdateRepairOnStepNotDone(workerID int) error {
	query := `
		UPDATE repairs
		SET
		    worker = NULL,
		    last_attempt = now()
		WHERE worker = $1;
	`

	_, err := r.db.Exec(query, workerID)
	if err != nil {
		return fmt.Errorf("failed to update repair: %w", err)
	}

	return nil
}
