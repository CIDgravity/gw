package rbdeal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/cidgravity"
	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/database/sqldb"
	iface2 "github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/rbmeta"
	"github.com/CIDgravity/filecoin-gateway/server/metrics"
	"golang.org/x/xerrors"

	"github.com/CIDgravity/filecoin-gateway/ributil"

	"github.com/fatih/color"
	"github.com/filecoin-project/go-address"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
)

var log = logging.Logger("ribs:rbdeal")

type openOptions struct {
	hostGetter          func(...libp2p.Option) (host.Host, error)
	localWalletOpener   func(path string) (*ributil.LocalWallet, error)
	localWalletPath     string
	fileCoinAPIEndpoint string
	sqldb               sqldb.Database
	cqldb               cqldb.Database
	rbstor              iface2.RBS
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

func WithSqlDatabase(db sqldb.Database) OpenOption {
	return func(o *openOptions) {
		o.sqldb = db
	}
}

func WithRbstor(rbs iface2.RBS) OpenOption {
	return func(o *openOptions) {
		o.rbstor = rbs
	}
}

type ribs struct {
	iface2.RBS
	db  *ribsDB
	mdb iface2.MetadataDB

	host   host.Host
	wallet *ributil.LocalWallet

	lotusRPCAddr string

	msgSendLk sync.Mutex

	marketFundsLk        sync.Mutex
	cachedWalletInfo     *iface2.WalletInfo
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
	crawlState atomic.Pointer[iface2.CrawlState]

	/*  */
	cidg                  cidgravity.CIDGravity
	canSendDealLastCheck  time.Time
	canSendDealLastResult bool

	/* external modules */
	externalOffloader ExternalOffloader

	/* dealmaking */
	dealsLk        sync.Mutex
	moreDealsLocks map[iface2.GroupKey]struct{}

	/* retrieval */
	retrHost host.Host
	retrProv *retrievalProvider

	retrCheckMetrics *retrievalCheckMetrics

	/* repair */
	repairDir     string
	repairStats   map[int]*iface2.RepairJob // workerid -> repair job
	repairStatsLk sync.Mutex

	repairFetchCounters *ributil.RateCounters[iface2.GroupKey]
}

func (r *ribs) MetaDB() iface2.MetadataDB {
	return r.mdb
}

func (r *ribs) Wallet() iface2.Wallet {
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

func Open(root string, opts ...OpenOption) (iface2.RIBS, error) {
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

	if opt.sqldb == nil {
		return nil, fmt.Errorf("sql database is required")
	}
	if opt.rbstor == nil {
		return nil, fmt.Errorf("rbstor is required")
	}

	db, err := openRibsDB(opt.sqldb)
	if err != nil {
		return nil, xerrors.Errorf("open db: %w", err)
	}

	if err := db.startDB(); err != nil {
		return nil, xerrors.Errorf("start db: %w", err)
	}

	r := &ribs{
		RBS: opt.rbstor,
		db:  db,

		lotusRPCAddr: opt.fileCoinAPIEndpoint,

		//uploadStats:     map[iface.GroupKey]*iface.GroupUploadStats{},
		//uploadStatsSnap: map[iface.GroupKey]*iface.GroupUploadStats{},
		//activeUploads:   map[uuid.UUID]int{},
		//rateCounters:    ributil.NewRateCounters[peer.ID](ributil.MinAvgGlobalLogPeerRate(float64(minTransferMbps), float64(linkSpeedMbps))),

		repairDir:   filepath.Join(root, "repair"),
		repairStats: map[int]*iface2.RepairJob{},

		close: make(chan struct{}),
		//workerClosed: make(chan struct{}),
		spCrawlClosed:     make(chan struct{}),
		marketWatchClosed: make(chan struct{}),

		moreDealsLocks: map[iface2.GroupKey]struct{}{},

		retrCheckMetrics: newRetrievalCheckMetrics(),

		repairFetchCounters: ributil.NewRateCounters[iface2.GroupKey](ributil.MinAvgGlobalLogPeerRate(float64(minTransferMbps), float64(linkSpeedMbps/4))),
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

	if err := r.initExternal(); err != nil {
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
	r.Storage().Subscribe(func(group iface2.GroupKey, from, to iface2.GroupState) {
		go r.onSub(group, from, to)
	})
}

func (r *ribs) onSub(group iface2.GroupKey, from, to iface2.GroupState) {
	if to == iface2.GroupStateLocalReadyForDeals {
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

func (r *ribs) RetrStats() (iface2.RetrStats, error) {
	return iface2.RetrStats{
		Success: int64(metrics.GetCounterValue(r.retrProv.metrics.success)),
		Bytes:   int64(metrics.GetCounterValue(r.retrProv.metrics.bytes)),
		Fail:    int64(metrics.GetCounterValue(r.retrProv.metrics.fail)),

		CacheHit:  int64(metrics.GetCounterValue(r.retrProv.metrics.cacheHit)),
		CacheMiss: int64(metrics.GetCounterValue(r.retrProv.metrics.cacheMiss)),

		Active: 0,

		HTTPTries:   int64(metrics.GetCounterValue(r.retrProv.metrics.httpTries)),
		HTTPSuccess: int64(metrics.GetCounterValue(r.retrProv.metrics.httpSuccess)),
		HTTPBytes:   int64(metrics.GetCounterValue(r.retrProv.metrics.httpBytes)),
	}, nil
}

func (r *ribs) Close() error {
	close(r.close)
	<-r.spCrawlClosed
	<-r.marketWatchClosed

	return r.RBS.Close()
}
