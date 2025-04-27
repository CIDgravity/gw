package rbdeal

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fatih/color"
	"github.com/filecoin-project/go-address"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/cidgravity"
	"github.com/lotus-web3/ribs/configuration"
	"github.com/lotus-web3/ribs/rbmeta"
	"github.com/lotus-web3/ribs/rbstor"
	"github.com/lotus-web3/ribs/ributil"
	"golang.org/x/xerrors"
)

var log = logging.Logger("ribs:rbdeal")

type openOptions struct {
	hostGetter          func(...libp2p.Option) (host.Host, error)
	localWalletOpener   func(path string) (*ributil.LocalWallet, error)
	localWalletPath     string
	fileCoinAPIEndpoint string
}

type OpenOption func(*openOptions)

// WithHostGetter sets the function used to instantiate the libp2p host used by RIBS.
// Defaults to libp2p.New.
func WithHostGetter(hg func(...libp2p.Option) (host.Host, error)) OpenOption {
	return func(o *openOptions) {
		o.hostGetter = hg
	}
}

// WithLocalWalletOpener sets the function used to open the local wallet path.
// Defaults to using ributil.OpenWallet, where the wallet is instantiated if it does not exist.
// In a case where it is auto generated, the wallet path must be backed up elsewhere.
//
// See: WithLocalWalletPath.
func WithLocalWalletOpener(wg func(path string) (*ributil.LocalWallet, error)) OpenOption {
	return func(o *openOptions) {
		o.localWalletOpener = wg
	}
}

// WithLocalWalletPath sets the path to the local directory containing the wallet.
// Care must be taken in backing up this directory.
// Defaults to `.ribswallet` under user home directory.
func WithLocalWalletPath(wp string) OpenOption {
	return func(o *openOptions) {
		o.localWalletPath = wp
	}
}

// WithFileCoinApiEndpoint sets the FileCoin API endpoint used to probe the chain.
// Defaults to "https://api.chain.love/rpc/v1".
func WithFileCoinApiEndpoint(wp string) OpenOption {
	return func(o *openOptions) {
		o.fileCoinAPIEndpoint = wp
	}
}

type ribs struct {
	iface.RBS
	db  *ribsDB
	mdb iface.MetadataDB

	host   host.Host
	wallet *ributil.LocalWallet

	lotusRPCAddr string

	msgSendLk sync.Mutex

	marketFundsLk        sync.Mutex
	cachedWalletInfo     *iface.WalletInfo
	lastWalletInfoUpdate time.Time

	//

	close chan struct{}
	//workerClosed chan struct{}
	spCrawlClosed     chan struct{}
	marketWatchClosed chan struct{}

	//

	/* sp crawl */

	crawlHost host.Host

	/* sp tracker */
	crawlState atomic.Pointer[iface.CrawlState]

	/* car uploads */
	uploadStats     map[iface.GroupKey]*iface.GroupUploadStats
	uploadStatsSnap map[iface.GroupKey]*iface.GroupUploadStats

	activeUploads map[uuid.UUID]int
	uploadStatsLk sync.Mutex

	rateCounters *ributil.RateCounters[peer.ID]

	/* car upload offload (S3) */
	cidg                  cidgravity.CIDGravity
	canSendDealLastCheck  time.Time
	canSendDealLastResult bool

	s3          *s3.S3
	s3Bucket    string
	s3BucketUrl *url.URL

	s3Uploads map[iface.GroupKey]struct{}
	s3Lk      sync.Mutex

	/* s3 stats */

	s3UploadBytes, s3UploadStarted, s3UploadDone, s3UploadErr, s3Redirects, s3ReadReqs, s3ReadBytes atomic.Int64

	/* external modules */
	externalOffloader ExternalOffloader

	/* dealmaking */
	dealsLk        sync.Mutex
	moreDealsLocks map[iface.GroupKey]struct{}

	/* retrieval */
	retrHost host.Host
	retrProv *retrievalProvider

	retrSuccess, retrBytes, retrFail, retrCacheHit, retrCacheMiss, retrHttpTries, retrHttpSuccess, retrHttpBytes, retrActive atomic.Int64

	/* retrieval checker */
	rckToDo, rckStarted, rckSuccess, rckFail, rckSuccessAll, rckFailAll atomic.Int64

	/* repair */
	repairDir     string
	repairStats   map[int]*iface.RepairJob // workerid -> repair job
	repairStatsLk sync.Mutex

	repairFetchCounters *ributil.RateCounters[iface.GroupKey]
}

func (r *ribs) MetaDB() iface.MetadataDB {
	return r.mdb
}

func (r *ribs) Wallet() iface.Wallet {
	return r
}

func OpenOrCreateWallet(path string) (*ributil.LocalWallet, address.Address, error) {
	wallet, err := ributil.OpenWallet(path)
	if err != nil {
		return nil, address.Undef, fmt.Errorf("open wallet: %w", err)
	}

	defWallet, err := wallet.GetDefault()
	if err != nil {
		wl, err := wallet.WalletList(context.TODO())
		if err != nil {
			return nil, address.Undef, fmt.Errorf("get wallet list: %w", err)
		}

		if len(wl) == 0 {
			a, err := wallet.WalletNew(context.TODO(), "secp256k1")
			if err != nil {
				return nil, address.Undef, fmt.Errorf("creating wallet: %w", err)
			}

			color.Yellow("--------------------------------------------------------------")
			fmt.Println("CREATED NEW GATEWAY WALLET")
			fmt.Println("ADDRESS: ", color.GreenString("%s", a))
			fmt.Println("")
			fmt.Printf("BACKUP YOUR WALLET DIRECTORY (%s)\n", path)
			fmt.Println("")
			fmt.Println("Before using Gateway, you must fund your wallet with FIL.")
			fmt.Println("You can also supply it with DataCap if you want to make")
			fmt.Println("FIL+ deals.")
			color.Yellow("--------------------------------------------------------------")

			wl = append(wl, a)
		}

		if len(wl) != 1 {
			return nil, address.Undef, fmt.Errorf("no default wallet or more than one wallet: %#v", wl)
		}

		if err := wallet.SetDefault(wl[0]); err != nil {
			return nil, address.Undef, fmt.Errorf("setting default wallet: %w", err)
		}

		defWallet, err = wallet.GetDefault()
		if err != nil {
			return nil, address.Undef, fmt.Errorf("getting default wallet: %w", err)
		}
	}

	return wallet, defWallet, nil
}

func Open(root string, opts ...OpenOption) (iface.RIBS, error) {
	if err := os.Mkdir(root, 0755); err != nil && !os.IsExist(err) {
		return nil, xerrors.Errorf("make root dir: %w", err)
	}

	cfg := configuration.GetConfig()
	opt := &openOptions{
		hostGetter:          libp2p.New,
		localWalletOpener:   ributil.OpenWallet,
		localWalletPath:     "~/.ribswallet",
		fileCoinAPIEndpoint: cfg.Ribs.FilecoinApiEndpoint,
	}

	for _, o := range opts {
		o(opt)
	}

	db, err := openRibsDB(root)
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	rbs, err := rbstor.Open(root, rbstor.WithDB(db.db))
	if err != nil {
		return nil, xerrors.Errorf("open RBS: %w", err)
	}

	if err := db.startDB(); err != nil {
		return nil, xerrors.Errorf("start db: %w", err)
	}

	r := &ribs{
		RBS: rbs,
		db:  db,

		lotusRPCAddr: opt.fileCoinAPIEndpoint,

		uploadStats:     map[iface.GroupKey]*iface.GroupUploadStats{},
		uploadStatsSnap: map[iface.GroupKey]*iface.GroupUploadStats{},
		activeUploads:   map[uuid.UUID]int{},
		rateCounters:    ributil.NewRateCounters[peer.ID](ributil.MinAvgGlobalLogPeerRate(float64(minTransferMbps), float64(linkSpeedMbps))),

		s3Uploads: map[iface.GroupKey]struct{}{},

		repairDir:   filepath.Join(root, "repair"),
		repairStats: map[int]*iface.RepairJob{},

		close: make(chan struct{}),
		//workerClosed: make(chan struct{}),
		spCrawlClosed:     make(chan struct{}),
		marketWatchClosed: make(chan struct{}),

		moreDealsLocks: map[iface.GroupKey]struct{}{},

		repairFetchCounters: ributil.NewRateCounters[iface.GroupKey](ributil.MinAvgGlobalLogPeerRate(float64(minTransferMbps), float64(linkSpeedMbps/4))),
	}

	rp, err := newRetrievalProvider(context.TODO(), r)
	if err != nil {
		return nil, xerrors.Errorf("creating retrieval provider: %w", err)
	}

	r.retrProv = rp

	{
		wallet, defWallet, err := OpenOrCreateWallet(opt.localWalletPath)
		if err != nil {
			return nil, xerrors.Errorf("open/create wallet: %w", err)
		}
		fmt.Println("RIBS Wallet: ", defWallet)
		r.wallet = wallet
		r.host, err = opt.hostGetter()
		if err != nil {
			return nil, xerrors.Errorf("creating host: %w", err)
		}
	}

	if err := r.maybeInitS3Offload(); err != nil {
		return nil, xerrors.Errorf("trying to initialize S3 offload: %w", err)
	}

	if err := r.maybeInitExternal(); err != nil {
		return nil, xerrors.Errorf("XYZ: trying to initialize external offload: %w", err)
	}

	r.RBS.ExternalStorage().InstallProvider(rp)

	if err := r.RBS.Start(); err != nil {
		return nil, xerrors.Errorf("start storage: %w", err)
	}
	mdb, err := rbmeta.Open(r)
	if err != nil {
		return nil, xerrors.Errorf("Initializing MetaDatabase: %w", err)
	}
	r.mdb = mdb

	go r.spCrawler()
	go r.dealTracker(context.TODO())
	go r.watchMarket(context.TODO())
	go r.retrievalChecker(context.TODO())
	if err := r.setupCarServer(context.TODO()); err != nil {
		return nil, xerrors.Errorf("setup car server: %w", err)
	}

	/* XXX: no repair worker for now, we don't have a staging area to repair to
	go r.repairWorker(context.TODO(), 0)
	go r.repairWorker(context.TODO(), 1)
	go r.repairWorker(context.TODO(), 2)
	go r.repairWorker(context.TODO(), 3)
	*/
	/*go r.repairWorker(context.TODO(), 4)
	go r.repairWorker(context.TODO(), 5)
	go r.repairWorker(context.TODO(), 6)
	go r.repairWorker(context.TODO(), 7)
	go r.repairWorker(context.TODO(), 8)
	/*go r.repairWorker(context.TODO(), 9)
	go r.repairWorker(context.TODO(), 10)*/

	r.subGroupChanges()

	go r.claimChecker()

	return r, nil
}

func (r *ribs) subGroupChanges() {
	r.Storage().Subscribe(func(group iface.GroupKey, from, to iface.GroupState) {
		go r.onSub(group, from, to)
	})
}

func (r *ribs) onSub(group iface.GroupKey, from, to iface.GroupState) {
	if to == iface.GroupStateLocalReadyForDeals {
		c, _, err := r.db.GetNonFailedDealCount(group)
		if err != nil {
			log.Errorf("getting non-failed deal count: %s", err)
			return
		}

		cfg := configuration.GetConfig()
		// lose check, will go in loop and check it out later anyway if needed
		if c >= cfg.Ribs.MinimumRetrievableCount {
			return
		}

		go func() {
			err = r.makeMoreDeals(context.TODO(), group, r.wallet, nil)
			if err != nil {
				log.Errorf("starting new deals: %s", err)
			}
		}()
	}
}

func (r *ribs) RetrStats() (iface.RetrStats, error) {
	return iface.RetrStats{
		Success: r.retrSuccess.Load(),
		Bytes:   r.retrBytes.Load(),
		Fail:    r.retrFail.Load(),

		CacheHit:  r.retrCacheHit.Load(),
		CacheMiss: r.retrCacheMiss.Load(),

		Active: r.retrActive.Load(),

		HTTPTries:   r.retrHttpTries.Load(),
		HTTPSuccess: r.retrHttpSuccess.Load(),
		HTTPBytes:   r.retrHttpBytes.Load(),
	}, nil
}

func (r *ribs) Close() error {
	close(r.close)
	<-r.spCrawlClosed
	<-r.marketWatchClosed

	return r.RBS.Close()
}
