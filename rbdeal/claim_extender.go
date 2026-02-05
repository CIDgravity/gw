package rbdeal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifregtypes13 "github.com/filecoin-project/go-state-types/builtin/v13/verifreg"
	"github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/lotus/api"
	aclient "github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	verifreg2 "github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/must"
	"golang.org/x/xerrors"
)

func (r *ribs) claimChecker() {
	for {
		select {
		case <-r.close:
			return
		case <-time.After(3 * time.Minute): // don't start immediately
			if err := r.claimExtendCycle(context.Background()); err != nil {
				log.Errorw("claim extend cycle failed", "error", err)
				return
			}
		}

		select {
		case <-r.close:
			return
		case <-time.After(24 * time.Hour):
			// daily
		}
	}
}

func (r *ribs) claimExtendCycle(ctx context.Context) error {
	var provs []address.Address

	// Get groups that are marked for GC (should not have claims extended)
	gcGroups := make(map[int64]bool)
	{
		rows, err := r.db.db.Query("SELECT id FROM groups WHERE gc_state >= $1", GCStateCandidate)
		if err != nil {
			// If gc_state column doesn't exist yet (migration not run), continue without filtering
			log.Debugw("gc_state query failed (migration may not be applied)", "error", err)
		} else {
			defer rows.Close()
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					log.Warnw("failed to scan gc group", "error", err)
					continue
				}
				gcGroups[id] = true
			}
			rows.Close()
		}
	}

	if len(gcGroups) > 0 {
		fmt.Printf("(claim ext) Skipping %d groups marked for GC\n", len(gcGroups))
	}

	{
		// SELECT DISTINCT provider_addr FROM deals WHERE sealed=1 AND group not in GC candidates
		// We still get all providers, but will filter claims by group later
		rows, err := r.db.db.Query("SELECT DISTINCT provider_addr FROM deals WHERE sealed=1")
		if err != nil {
			return xerrors.Errorf("query: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var prov uint64
			if err := rows.Scan(&prov); err != nil {
				return xerrors.Errorf("scan: %w", err)
			}
			provs = append(provs, must.One(address.NewIDAddress(prov)))
		}
		if err := rows.Err(); err != nil {
			return xerrors.Errorf("rows: %w", err)
		}
		if err := rows.Close(); err != nil {
			return xerrors.Errorf("close: %w", err)
		}
	}

	// Build a map of deal -> group for filtering
	dealToGroup := make(map[string]int64) // deal UUID -> group ID
	{
		rows, err := r.db.db.Query("SELECT uuid, group_id FROM deals WHERE sealed=1")
		if err != nil {
			log.Warnw("failed to query deal groups", "error", err)
		} else {
			defer rows.Close()
			for rows.Next() {
				var uuid string
				var groupID int64
				if err := rows.Scan(&uuid, &groupID); err != nil {
					continue
				}
				dealToGroup[uuid] = groupID
			}
			rows.Close()
		}
	}

	fmt.Printf("(claim ext) Getting claims for %d providers\n", len(provs))

	chain, closer, err := aclient.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		panic(err)
	}
	defer closer()

	clkey, err := r.wallet.GetDefault()
	if err != nil {
		return xerrors.Errorf("getting default wallet: %w", err)
	}

	clientAddr, err := chain.StateLookupID(ctx, clkey, types.EmptyTSK)
	if err != nil {
		return xerrors.Errorf("looking up client address: %w", err)
	}

	client, err := address.IDFromAddress(clientAddr)
	if err != nil {
		return xerrors.Errorf("getting client id: %w", err)
	}

	r.msgSendLk.Lock()
	defer r.msgSendLk.Unlock()

	nonce, err := chain.MpoolGetNonce(ctx, clkey)
	if err != nil {
		return xerrors.Errorf("getting nonce: %w", err)
	}

	claimOurs := func(claim verifreg.Claim) bool {
		return claim.Client == abi.ActorID(client)
	}

	// Build a map of (provider, piece_cid) -> group_id for GC filtering
	type provPiece struct {
		provider uint64
		pieceCid string
	}
	provPieceToGroup := make(map[provPiece]int64)
	{
		rows, err := r.db.db.Query("SELECT provider_addr, piece_cid, group_id FROM deals WHERE sealed=1")
		if err != nil {
			log.Warnw("failed to query deal piece mappings", "error", err)
		} else {
			for rows.Next() {
				var prov uint64
				var pieceCid string
				var groupID int64
				if err := rows.Scan(&prov, &pieceCid, &groupID); err != nil {
					continue
				}
				provPieceToGroup[provPiece{prov, pieceCid}] = groupID
			}
			rows.Close()
		}
	}

	var nclaims int
	var skippedGC int
	claims := map[address.Address]map[verifreg.ClaimId]verifreg.Claim{}

	for n, prov := range provs {
		fmt.Printf("(claim ext) Getting claims for %s (%d/%d)\n", prov, n, len(provs))
		allProvClaims, err := chain.StateGetClaims(ctx, prov, types.EmptyTSK)
		if err != nil {
			return xerrors.Errorf("getting claims: %w", err)
		}

		provID, _ := address.IDFromAddress(prov)

		for claimID, claim := range allProvClaims {
			if !claimOurs(claim) {
				continue
			}

			// Check if this claim is for a group marked for GC
			pieceCid := claim.Data.String()
			if groupID, ok := provPieceToGroup[provPiece{provID, pieceCid}]; ok {
				if gcGroups[groupID] {
					skippedGC++
					log.Debugw("skipping claim for GC group", "provider", prov, "claim", claimID, "group", groupID)
					continue
				}
			}

			if _, ok := claims[prov]; !ok {
				claims[prov] = map[verifreg.ClaimId]verifreg.Claim{}
			}
			claims[prov][claimID] = claim
			nclaims++
		}
	}

	if skippedGC > 0 {
		fmt.Printf("(claim ext) Skipped %d claims for GC candidate groups\n", skippedGC)
	}

	fmt.Printf("(claim ext) Got %d claims, creating messages\n", nclaims)

	tmax := abi.ChainEpoch(verifregtypes13.MaximumVerifiedAllocationTerm)
	params := verifreg.ExtendClaimTermsParams{}

	var totalGas int64
	totalFee := big.Zero()

	var mkMessage func(params verifreg.ExtendClaimTermsParams) (*types.Message, error)
	mkMessage = func(params verifreg.ExtendClaimTermsParams) (*types.Message, error) {
		clientAddr, err := address.NewIDAddress(client)
		if err != nil {
			return nil, err
		}

		enc, err := actors.SerializeParams(&params)
		if err != nil {
			return nil, err
		}

		gwNonce, err := chain.MpoolGetNonce(ctx, clientAddr)
		if err != nil {
			return nil, xerrors.Errorf("getting nonce: %w", err)
		}
		if gwNonce > nonce {
			nonce = gwNonce
		}

		m := &types.Message{
			To:     verifreg2.Address,
			From:   clkey,
			Method: verifreg2.Methods.ExtendClaimTerms,
			Params: enc,
			Value:  types.NewInt(0),
			Nonce:  nonce,
		}

		m, err = chain.GasEstimateMessageGas(ctx, m, nil, types.EmptyTSK)

		if (err != nil && strings.Contains(err.Error(), "call ran out of gas")) || m.GasLimit >= build.BlockGasLimit*4/5 {
			// estimate two messages
			p1 := params
			p2 := params

			p1.Terms = p1.Terms[:len(p1.Terms)/2]
			p2.Terms = p2.Terms[len(p2.Terms)/2:]

			_, err := mkMessage(p1)
			if err != nil {
				return nil, err
			}
			_, err = mkMessage(p2)
			if err != nil {
				return nil, err
			}
			return nil, nil
		}

		// overestimate a little more
		m.GasLimit = m.GasLimit * 10 / 9

		if err != nil {
			return nil, err
		}

		fmt.Printf("(claim ext) Message: %d Exts GasLimit=%d (%02d%% blk lim), GasFeeCap=%s, GasPremium=%s, Fee=%s\n", len(params.Terms),
			m.GasLimit, m.GasLimit*100/build.BlockGasLimit, m.GasFeeCap, m.GasPremium, types.FIL(m.RequiredFunds()))

		totalGas += m.GasLimit
		totalFee = types.BigAdd(totalFee, m.RequiredFunds())

		cfg := configuration.GetConfig()
		if cfg.Ribs.SendExtends {
			sig, err := r.wallet.WalletSign(ctx, clkey, m.Cid().Bytes(), api.MsgMeta{
				Type: api.MTChainMsg,
			})
			if err != nil {
				return nil, xerrors.Errorf("signing message: %w", err)
			}

			sm := &types.SignedMessage{
				Message:   *m,
				Signature: *sig,
			}

			c, aerr := chain.MpoolPush(ctx, sm)
			if aerr != nil {
				return nil, xerrors.Errorf("push: %w", aerr)
			}

			fmt.Printf("(claim ext) Pushed EXTEND message: %s\n", c)

			nonce++

			rec, err := chain.StateWaitMsg(ctx, c, 1, api.LookbackNoLimit, true)
			if err != nil {
				return nil, xerrors.Errorf("waiting for message: %w", err)
			}

			if rec.Receipt.ExitCode != 0 {
				return nil, xerrors.Errorf("message failed: %d", rec.Receipt.ExitCode)
			}
		}

		return m, nil
	}

	for provider, cls := range claims {
		mid, err := address.IDFromAddress(provider)
		if err != nil {
			return xerrors.Errorf("getting provider id: %w", err)
		}

		for claimId, claim := range cls {
			if claim.TermMax == tmax {
				continue
			}

			params.Terms = append(params.Terms, verifreg.ClaimTerm{
				Provider: abi.ActorID(mid),
				ClaimId:  claimId,
				TermMax:  tmax,
			})

			if len(params.Terms) >= 1000 {
				if _, err := mkMessage(params); err != nil {
					return err
				}
				params.Terms = nil
			}
		}
	}

	if len(params.Terms) > 0 {
		if _, err := mkMessage(params); err != nil {
			return err
		}
	}

	fmt.Printf("(claim ext) Total gas: %d (%.2f blks)\n", totalGas, float64(totalGas)/float64(build.BlockGasLimit))
	fmt.Printf("(claim ext) Total fee: %s\n", types.FIL(totalFee))

	return nil
}
