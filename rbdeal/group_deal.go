package rbdeal

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os/exec"
	"fmt"
	"strconv"
	//gobig "math/big"
	"time"

	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/ributil"
	types "github.com/lotus-web3/ribs/ributil/boosttypes"
	"golang.org/x/xerrors"
	"github.com/lotus-web3/ribs/cidgravity"
	"github.com/lotus-web3/ribs/configuration"
)

const DealProtocolv121 = "/fil/storage/mk/1.2.1"

type ErrRejected struct {
	Reason string
}

func (e ErrRejected) Error() string {
	return fmt.Sprintf("deal proposal rejected: %s", e.Reason)
}

func makeTraceToken(prov dealProvider) (string) {
	auth := fmt.Sprintf("f0%d-%s:password", prov.id, time.Now().Format("20060102150405"))
	return fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(auth)))
}

func (r *ribs) canSendMoreDeals(since time.Time) bool {
	log.Debugw("CanSendDeal?")
	cfg := configuration.GetConfig()
	if cfg.Ribs.DealCanSendCommand == "" {
		return true
	}
	if since.Before(r.canSendDealLastCheck) {
		log.Debugw("CanSendDeal: cached", "result", r.canSendDealLastResult, r.canSendDealLastCheck, "since", since)
		return r.canSendDealLastResult
	}
	log.Debugw("CanSendDeal? running command")
	cmd := exec.Command(cfg.Ribs.DealCanSendCommand)
	err := cmd.Run()
	r.canSendDealLastResult = err == nil
	r.canSendDealLastCheck = time.Now()
	log.Debugw("CanSendDeal: command called", "result", r.canSendDealLastResult, "ts", r.canSendDealLastCheck, "err", err)
	return r.canSendDealLastResult
}

func (r *ribs) makeMoreDeals(ctx context.Context, id iface.GroupKey, h host.Host, w *ributil.LocalWallet) error {
	check_start := time.Now()
	log.Debugw("makeMoreDeals", "id", id, "time", check_start)

	r.dealsLk.Lock()
	defer r.dealsLk.Unlock()
	log.Debugw("makeMoreDeals: lock acquired", "id", id)
	// Only try to send a single deal at once
	if !r.canSendMoreDeals(check_start) {
		return nil
	}
	/* // only sending 1 deal at once overall anyway
	if _, ok := r.moreDealsLocks[id]; ok {
		// r.dealsLk.Unlock()

		// another goroutine is already making deals for this group
		return nil
	}
	r.moreDealsLocks[id] = struct{}{}
	// r.dealsLk.Unlock()
	defer func() {
		// r.dealsLk.Lock()
		delete(r.moreDealsLocks, id)
		// r.dealsLk.Unlock()
	}()
	*/

	if err := r.maybeEnsureS3Offload(id); err != nil {
		return xerrors.Errorf("attempting s3 offload: %w", err)
	}
	if err := r.maybeEnsureEnsureExternalPush(id); err != nil {
		return xerrors.Errorf("XYZ: attempting external offload: %w", err)
	}

	dealInfo, err := r.db.GetDealParams(ctx, id)
	if err != nil {
		return xerrors.Errorf("get deal params: %w", err)
	}

	notFailed, unretrievable, err := r.db.GetNonFailedDealCount(id)
	if err != nil {
		log.Errorw("getting non-failed deal count", "error", err)
		return xerrors.Errorf("getting non-failed deal count: %w", err)
	}

	cfg := configuration.GetConfig()
	max := func(a, b int) (int) {
		if a > b {
			return a
		}
		return b
	}
	min := func(a, b int) (int) {
		if a < b {
			return a
		}
		return b
	}
	copiesRequired := max(0, cfg.Ribs.MinimumReplicaCount - notFailed)
	copiesRequired = max(copiesRequired, cfg.Ribs.MinimumRetrievableCount - (notFailed - unretrievable))
	copiesRequired = min(copiesRequired, cfg.Ribs.MaximumReplicaCount - notFailed)
	log.Debugw("makeMoreDeals", "group", id, "copiesRequired", copiesRequired, "notFailed", notFailed, "unretrievable", unretrievable)
	if copiesRequired <= 0 {
		// occasionally in some racy cases we can end up here
		return nil
	}

	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		return xerrors.Errorf("creating gateway rpc client: %w", err)
	}
	defer closer()

	walletAddr, err := w.GetDefault()
	if err != nil {
		return xerrors.Errorf("get wallet address: %w", err)
	}

	vc, err := gw.StateVerifiedClientStatus(ctx, walletAddr, ctypes.EmptyTSK)
	if err != nil {
		return xerrors.Errorf("getting verified client status: %w", err)
	}

	verified := false
	maxToPay := maxPrice

	if vc != nil {
		if vc.LessThan(minDatacap) {
			return xerrors.Errorf("not starting additional verified deals: datacap too low (%s, min %s)", ctypes.SizeStr(*vc), ctypes.SizeStr(minDatacap))
		}

		maxToPay = maxVerifPrice
		verified = true
	}

	pieceCid, err := commcid.PieceCommitmentV1ToCID(dealInfo.CommP)
	if err != nil {
		return fmt.Errorf("failed to convert commP to cid: %w", err)
	}

	// Gather more data for CIDGravity get-best-providers first
	var providerCollateral abi.TokenAmount

	bounds, err := gw.StateDealProviderCollateralBounds(ctx, abi.PaddedPieceSize(dealInfo.PieceSize), verified, ctypes.EmptyTSK)
	if err != nil {
		return fmt.Errorf("node error getting collateral bounds: %w", err)
	}
	providerCollateral = big.Div(big.Mul(bounds.Min, big.NewInt(6)), big.NewInt(5)) // add 20%

	head, err := gw.ChainHead(ctx)
	if err != nil {
		return fmt.Errorf("getting chain head: %w", err)
	}

	startEpoch := head.Height() + abi.ChainEpoch(cfg.Deal.StartTime * builtin.EpochsInDay / 24)

	duration := cfg.Deal.Duration * builtin.EpochsInDay

	// XXX: price?
	price := big.Zero()

	transfer := types.Transfer{
		Type:   "libp2p",
		Size:   uint64(dealInfo.CarSize),
	}
	url, err := r.maybeGetExternalURL(id)
	if err != nil {
		return fmt.Errorf("Failed to get External URL: %w", err)
	}
	if url != nil {
		transfer.Type = "http"
	}

	removeUnsealed := cfg.Deal.RemoveUnsealedCopy

	provsIds, err := r.cidg.GetBestAvailableProviders(cidgravity.CIDgravityGetBestAvailableProvidersRequest{
                PieceCid:             pieceCid.String(),
                StartEpoch:           uint64(startEpoch),
                Duration:             uint64(duration),
                StoragePricePerEpoch: json.Number(price.String()),
                ProviderCollateral:   json.Number(providerCollateral.String()),
                VerifiedDeal:         &verified,
                TransferSize:         transfer.Size,
                TransferType:         transfer.Type,
                RemoveUnsealedCopy:   &removeUnsealed,
	})
	if err != nil {
		return xerrors.Errorf("select deal providers: %w", err)
	}

	log.Debugw("making more deal", "group", id, "providers", provsIds)

        provs := []dealProvider{}
        for _, prov := range provsIds {
		provid, err := strconv.Atoi(prov[2:])
		if err != nil {
			return xerrors.Errorf("invalid selected provider: %s: %w", prov, err)
		}
		provs = append(provs, dealProvider{id: int64(provid)})
	}

	makeDealWith := func(prov dealProvider) error {
		// check proposal params
		maddr, err := address.NewIDAddress(uint64(prov.id))
		if err != nil {
			return xerrors.Errorf("new id address: %w", err)
		}

		addrInfo, err := GetAddrInfo(ctx, gw, maddr)
		if err != nil {
			return xerrors.Errorf("get addr info: %w", err)
		}


		// generate proposal
		dealUuid := uuid.New()

		dealProposal, err := dealProposal(ctx, w, walletAddr, dealInfo.Root, abi.PaddedPieceSize(dealInfo.PieceSize), pieceCid, maddr, startEpoch, duration, verified, providerCollateral, price)
		if err != nil {
			return fmt.Errorf("failed to create a deal proposal: %w", err)
		}

		var proposalBuf bytes.Buffer
		if err := dealProposal.MarshalCBOR(&proposalBuf); err != nil {
			return fmt.Errorf("failed to marshal deal proposal: %w", err)
		}

		// generate transfer token
		transfer, err := r.makeCarRequest(id, time.Hour*36, dealInfo.CarSize, dealUuid)
		if err != nil {
			return xerrors.Errorf("make car request token: %w", err)
		}

		dealParams := types.DealParams{
			DealUUID:           dealUuid,
			ClientDealProposal: *dealProposal,
			DealDataRoot:       dealInfo.Root,
			IsOffline:          false,
			Transfer:           transfer,
			RemoveUnsealedCopy: removeUnsealed,
			SkipIPNIAnnounce:   cfg.Deal.SkipIPNIAnnounce,
		}

		di := dbDealInfo{
			DealUUID:            dealUuid.String(),
			GroupID:             id,
			ClientAddr:          walletAddr.String(),
			ProviderAddr:        prov.id,
			PricePerEpoch:       price.Int64(),
			Verified:            verified,
			KeepUnsealed:        true,
			StartEpoch:          startEpoch,
			EndEpoch:            startEpoch + abi.ChainEpoch(duration),
			SignedProposalBytes: proposalBuf.Bytes(),
		}

		err = r.db.StoreDealProposal(di)
		if err != nil {
			return fmt.Errorf("saving deal info: %w", err)
		}

		if price.GreaterThan(big.NewInt(int64(maxToPay))) {
			err = r.db.StoreRejectedDeal(dealUuid.String(), fmt.Sprintf("price %d is greater than max price %f", price, maxToPay), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			// this check is probably redundant, buuut..
			return fmt.Errorf("price %d is greater than max price %f", price, maxToPay)
		}

		if err := r.host.Connect(ctx, *addrInfo); err != nil {
			err = r.db.StoreRejectedDeal(di.DealUUID, fmt.Sprintf("failed to connect to miner: %s", err), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return xerrors.Errorf("connect to miner: %w", err)
		}

		x, err := r.host.Peerstore().FirstSupportedProtocol(addrInfo.ID, DealProtocolv121)
		if err != nil {
			err = r.db.StoreRejectedDeal(di.DealUUID, fmt.Sprintf("failed to connect to miner: %s", err), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return fmt.Errorf("getting protocols for peer %s: %w", addrInfo.ID, err)
		}

		if len(x) == 0 {
			err := fmt.Errorf("boost client cannot make a deal with storage provider %s because it does not support protocol version 1.2.0", maddr)

			if err := r.db.StoreRejectedDeal(di.DealUUID, err.Error(), 0); err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return err
		}

		// MAKE THE DEAL

		s, err := r.host.NewStream(ctx, addrInfo.ID, DealProtocolv121)
		if err != nil {
			err = r.db.StoreRejectedDeal(di.DealUUID, xerrors.Errorf("opening deal proposal stream: %w", err).Error(), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return fmt.Errorf("failed to open stream to peer %s: %w", addrInfo.ID, err)
		}
		defer s.Close()

		var resp types.DealResponse
		if err := doRpc(ctx, s, &dealParams, &resp); err != nil {
			err = r.db.StoreRejectedDeal(di.DealUUID, xerrors.Errorf("sending deal proposal rpc: %w", err).Error(), 0)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return fmt.Errorf("send proposal rpc: %w", err)
		}

		if !resp.Accepted {
			err = r.db.StoreRejectedDeal(di.DealUUID, resp.Message, 1)
			if err != nil {
				return fmt.Errorf("saving rejected deal info: %w", err)
			}

			return ErrRejected{Reason: resp.Message}
		}

		if err := r.db.StoreSuccessfullyProposedDeal(di); err != nil {
			return xerrors.Errorf("marking deal as successfully proposed: %w", err)
		}

		log.Infof("Deal %s with %s accepted for group %d!!!", dealUuid, maddr, id)

		return nil
	}

	// make deals with candidates
	for _, prov := range provs {
		err := makeDealWith(prov)
		if err == nil {
			copiesRequired--

			// reselt last check to clear cache
			r.canSendDealLastCheck = time.Unix(0, 0)
			if copiesRequired <= 0 {
				// enough
				break
			}
			if !r.canSendMoreDeals(time.Now()) {
				return nil
			}

			// deal made
			continue
		}
		/*if re, ok := err.(ErrRejected); ok {
			// deal rejected
			continue
		}*/

		log.Errorw("failed to make deal with provider", "provider", fmt.Sprintf("f0%d", prov.id), "error", err)
	}

	return nil
}

func dealProposal(ctx context.Context, w *ributil.LocalWallet, clientAddr address.Address, rootCid cid.Cid, pieceSize abi.PaddedPieceSize, pieceCid cid.Cid, minerAddr address.Address, startEpoch abi.ChainEpoch, duration int, verified bool, providerCollateral abi.TokenAmount, storagePrice abi.TokenAmount) (*market.ClientDealProposal, error) {
	endEpoch := startEpoch + abi.ChainEpoch(duration)
	// deal proposal expects total storage price for deal per epoch, therefore we
	// multiply pieceSize * storagePrice (which is set per epoch per GiB) and divide by 2^30
	storagePricePerEpochForDeal := big.Div(big.Mul(big.NewInt(int64(pieceSize)), storagePrice), big.NewInt(int64(1<<30)))
	l, err := market.NewLabelFromString(rootCid.String())
	if err != nil {
		return nil, err
	}
	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize,
		VerifiedDeal:         verified,
		Client:               clientAddr,
		Provider:             minerAddr,
		Label:                l,
		StartEpoch:           startEpoch,
		EndEpoch:             endEpoch,
		StoragePricePerEpoch: storagePricePerEpochForDeal,
		ProviderCollateral:   providerCollateral,
	}

	buf, err := cborutil.Dump(&proposal)
	if err != nil {
		return nil, err
	}

	sig, err := w.WalletSign(ctx, clientAddr, buf, api.MsgMeta{Type: api.MTDealProposal})
	if err != nil {
		return nil, fmt.Errorf("wallet sign failed: %w", err)
	}

	return &market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: *sig,
	}, nil
}
