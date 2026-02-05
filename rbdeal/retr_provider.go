package rbdeal

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/CIDgravity/filecoin-gateway/carlog"
	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/CIDgravity/filecoin-gateway/rbcache"
	"github.com/CIDgravity/filecoin-gateway/rbstor"
	"github.com/CIDgravity/filecoin-gateway/server/metrics"
	pool "github.com/libp2p/go-buffer-pool"

	"github.com/CIDgravity/filecoin-gateway/ributil"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/lib/must"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

type mhStr string // multihash bytes in a string

type retrievalProvider struct {
	r *ribs

	reqSourcesLk sync.Mutex
	requests     map[mhStr]map[iface.GroupKey]int

	gw api.Gateway

	addrLk sync.Mutex
	addrs  map[int64]ProviderAddrInfo

	statLk   sync.Mutex
	attempts map[peer.ID]int64
	fails    map[peer.ID]int64
	success  map[peer.ID]int64

	ongoingRequestsLk sync.Mutex
	ongoingRequests   map[cid.Cid]*requestPromise

	// Multi-tier caching
	l1Cache    *rbcache.ARCCache[mhStr, []byte] // L1 in-memory ARC cache
	l2Cache    *rbcache.SSDCache                // L2 SSD cache (optional)
	prefetcher *rbcache.Prefetcher              // Prefetch engine (optional)

	// Legacy LRU cache for fallback
	blockCache *lru.Cache[mhStr, []byte]

	candidateCache *lru.Cache[iface.GroupKey, cachedRetrCandidates]

	metrics *RetrievalMetrics

	accessTracker *rbstor.AccessTracker
}

type cachedRetrCandidates struct {
	candidates []RetrCandidate
	readTime   time.Time
}

const BlockCacheSizeMiB = 512
const AvgBlockSize = 256 << 10
const BlockCacheSize = BlockCacheSizeMiB << 20 / AvgBlockSize
const RetrievalCandidateCacheSize = 10000
const RetrievalCandidateTimeout = 5 * time.Minute

type requestPromise struct {
	done    chan struct{}
	res     []byte
	err     error
	claimed bool
}

func (r *retrievalProvider) getAddrInfoCached(provider int64) (ProviderAddrInfo, error) {
	r.addrLk.Lock()
	defer r.addrLk.Unlock()

	if _, ok := r.addrs[provider]; !ok {
		// todo optimization: don't hold the lock here
		ai, err := r.r.db.GetProviderAddrs(provider)
		if err != nil {
			return ProviderAddrInfo{}, xerrors.Errorf("failed to get provider addrs: %w", err)
		}

		r.addrs[provider] = ai
	}

	addrInfo := r.addrs[provider]
	return addrInfo, nil
}

func (r *retrievalProvider) retrievalCandidatesForGroupCached(source iface.GroupKey) (cachedRetrCandidates, error) {
	if v, ok := r.candidateCache.Get(source); ok {
		if time.Since(v.readTime) < RetrievalCandidateTimeout {
			return v, nil
		}
	}

	candidates, err := r.r.db.GetRetrievalCandidates(source)
	if err != nil {
		return cachedRetrCandidates{}, xerrors.Errorf("failed to get retrieval candidates: %w", err)
	}

	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	v := cachedRetrCandidates{candidates, time.Now()}
	// this can technically race on expired entries, but the duplicate work should be minimal
	r.candidateCache.Add(source, v)
	return v, nil
}

func newRetrievalProvider(ctx context.Context, r *ribs) (*retrievalProvider, error) {
	gw, closer, err := client.NewGatewayRPCV1(ctx, r.lotusRPCAddr, nil)
	if err != nil {
		return nil, xerrors.Errorf("create retrieval gateway rpc: %w", err)
	}
	// TODO defer closer() more better
	go func() {
		<-ctx.Done()
		closer()
	}()

	cfg := configuration.GetConfig()
	cacheCfg := cfg.Cache

	rp := &retrievalProvider{
		r: r,

		requests: map[mhStr]map[iface.GroupKey]int{},
		gw:       gw,

		attempts: map[peer.ID]int64{},
		fails:    map[peer.ID]int64{},
		success:  map[peer.ID]int64{},

		addrs: map[int64]ProviderAddrInfo{},

		ongoingRequests: map[cid.Cid]*requestPromise{},

		candidateCache: must.One(lru.New[iface.GroupKey, cachedRetrCandidates](RetrievalCandidateCacheSize)),

		metrics: newRetrievalMetrics(),
	}

	// Initialize L1 cache based on configuration
	l1SizeBytes := int64(cacheCfg.L1SizeMiB) * 1024 * 1024
	if l1SizeBytes <= 0 {
		l1SizeBytes = BlockCacheSizeMiB << 20 // Default 512MB
	}

	if cacheCfg.L1Policy == "arc" || cacheCfg.L1Policy == "" {
		// Use new ARC cache
		rp.l1Cache = rbcache.NewARCCache[mhStr, []byte](
			l1SizeBytes,
			func(data []byte) int64 { return int64(len(data)) },
			"l1_block",
		)
		log.Infow("initialized L1 ARC cache", "size_mib", cacheCfg.L1SizeMiB)
	} else {
		// Fallback to legacy LRU cache
		rp.blockCache = must.One(lru.New[mhStr, []byte](BlockCacheSize))
		log.Infow("initialized legacy LRU cache", "size_entries", BlockCacheSize)
	}

	// Initialize L2 SSD cache if enabled
	if cacheCfg.L2Enabled && cacheCfg.L2Path != "" {
		l2SizeBytes := int64(cacheCfg.L2SizeGB) * 1024 * 1024 * 1024
		l2Cache, err := rbcache.NewSSDCache(rbcache.SSDCacheConfig{
			Path:          cacheCfg.L2Path,
			MaxSizeBytes:  l2SizeBytes,
			FlushSize:     100,
			FlushInterval: time.Second,
			MetricsName:   "l2_ssd",
		})
		if err != nil {
			log.Warnw("failed to initialize L2 SSD cache, continuing without it", "error", err, "path", cacheCfg.L2Path)
		} else {
			rp.l2Cache = l2Cache
			log.Infow("initialized L2 SSD cache", "size_gb", cacheCfg.L2SizeGB, "path", cacheCfg.L2Path)
		}
	}

	// Set up L1→L2 promotion callback if both caches are enabled
	if rp.l1Cache != nil && rp.l2Cache != nil {
		rp.l1Cache.SetEvictionCallback(func(key mhStr, value []byte, stats rbcache.EvictionStats) {
			// Only admit to L2 if item was accessed at least twice (from T2) or
			// if it was in T1 but had some access count
			if stats.AccessCount >= 2 || !stats.FromT1 {
				rp.l2Cache.Put(string(key), value)
			}
		})
		log.Infow("enabled L1→L2 cache promotion")
	}

	// Initialize prefetcher if enabled
	if cacheCfg.PrefetchEnabled && rp.l1Cache != nil {
		prefetchCfg := rbcache.DefaultPrefetcherConfig()
		prefetchCfg.NumWorkers = cacheCfg.PrefetchWorkers
		prefetchCfg.MaxDAGDepth = cacheCfg.PrefetchDepth

		// Create a simple fetcher adapter
		fetcher := &retrievalFetcher{rp: rp}

		// Use the L1 cache as the target (with adapter)
		l1Adapter := &l1CacheAdapter{cache: rp.l1Cache}
		var l2Adapter rbcache.CacheInterface
		if rp.l2Cache != nil {
			l2Adapter = &l2CacheAdapter{cache: rp.l2Cache}
		}

		rp.prefetcher = rbcache.NewPrefetcher(
			l1Adapter,
			l2Adapter,
			fetcher,
			rbcache.NullLinkResolver{}, // TODO: implement proper link resolver for UnixFS
			prefetchCfg,
		)
		log.Infow("initialized prefetcher", "workers", prefetchCfg.NumWorkers, "depth", prefetchCfg.MaxDAGDepth)
	}

	// Initialize access tracker
	rp.accessTracker = rbstor.NewAccessTracker(rbstor.DefaultAccessTrackerConfig())
	log.Infow("initialized access tracker")

	return rp, nil
}

// l1CacheAdapter adapts ARCCache to CacheInterface
type l1CacheAdapter struct {
	cache *rbcache.ARCCache[mhStr, []byte]
}

func (a *l1CacheAdapter) Has(key string) bool {
	return a.cache.Has(mhStr(key))
}

func (a *l1CacheAdapter) Put(key string, data []byte) {
	a.cache.Put(mhStr(key), data)
}

// l2CacheAdapter adapts SSDCache to CacheInterface
type l2CacheAdapter struct {
	cache *rbcache.SSDCache
}

func (a *l2CacheAdapter) Has(key string) bool {
	return a.cache.Has(key)
}

func (a *l2CacheAdapter) Put(key string, data []byte) {
	a.cache.Put(key, data)
}

// retrievalFetcher implements DataFetcher for the prefetcher
type retrievalFetcher struct {
	rp *retrievalProvider
}

func (f *retrievalFetcher) Fetch(ctx context.Context, c cid.Cid) ([]byte, error) {
	// Try to fetch the block using the retrieval provider's existing logic
	key := mhStr(c.Hash())

	// Check if already in cache
	if f.rp.l1Cache != nil {
		if data, ok := f.rp.l1Cache.Get(key); ok {
			return data, nil
		}
	}

	if f.rp.l2Cache != nil {
		if data, ok := f.rp.l2Cache.Get(string(key)); ok {
			// Promote to L1
			if f.rp.l1Cache != nil {
				f.rp.l1Cache.Put(key, data)
			}
			return data, nil
		}
	}

	// Try local RIBS storage first
	var localData []byte
	session := f.rp.r.Session(ctx)
	err := session.View(ctx, []multihash.Multihash{c.Hash()}, func(cidx int, data []byte) {
		localData = make([]byte, len(data))
		copy(localData, data)
	})
	if err == nil && localData != nil {
		// Cache and return
		f.rp.cacheBlock(key, localData)
		return localData, nil
	}

	// Try HTTP retrieval from any available provider
	// Find which group this CID belongs to
	groups, err := f.rp.r.Storage().FindHashes(ctx, c.Hash())
	if err != nil || len(groups) == 0 {
		return nil, xerrors.Errorf("failed to find group for cid %s: %w", c, err)
	}
	foundGroup := groups[0]

	// Get retrieval candidates for this group
	candidates, err := f.rp.retrievalCandidatesForGroupCached(foundGroup)
	if err != nil {
		return nil, xerrors.Errorf("failed to get retrieval candidates for group %d: %w", foundGroup, err)
	}

	if len(candidates.candidates) == 0 {
		return nil, xerrors.Errorf("no retrieval candidates available for group %d", foundGroup)
	}

	// Try each candidate
	var retrievedData []byte
	for _, candidate := range candidates.candidates {
		addrInfo, err := f.rp.getAddrInfoCached(candidate.Provider)
		if err != nil {
			continue
		}

		if len(addrInfo.HttpMaddrs) == 0 {
			continue
		}

		u, err := ributil.MaddrsToUrl(addrInfo.HttpMaddrs)
		if err != nil {
			continue
		}

		err = f.rp.doHttpRetrieval(ctx, foundGroup, candidate.Provider, u, c, func(data []byte) {
			retrievedData = make([]byte, len(data))
			copy(retrievedData, data)
		})

		if err == nil && retrievedData != nil {
			// Cache and return
			f.rp.cacheBlock(key, retrievedData)
			return retrievedData, nil
		}
	}

	return nil, xerrors.Errorf("failed to retrieve cid %s from any provider", c)
}

// cacheBlock stores a block in the cache hierarchy (L1, and optionally L2)
func (r *retrievalProvider) cacheBlock(key mhStr, data []byte) {
	if r.l1Cache != nil {
		r.l1Cache.Put(key, data)
	} else if r.blockCache != nil {
		r.blockCache.Add(key, data)
	}
	// Note: L2 cache uses admission policy, blocks are demoted from L1 on eviction
	// For now, we only insert into L1; L2 admission happens via the ARC eviction callback
}

// Close cleans up cache resources
func (r *retrievalProvider) Close() error {
	if r.prefetcher != nil {
		r.prefetcher.Close()
	}
	if r.l2Cache != nil {
		return r.l2Cache.Close()
	}
	return nil
}

func (r *retrievalProvider) FetchBlocks(ctx context.Context, group iface.GroupKey, mh []multihash.Multihash, cb func(cidx int, data []byte)) error {
	// try cache hierarchy: L1 (memory) -> L2 (SSD) -> network
	var l1Hits, l2Hits int
	var bytesServed int64

	defer func() {
		r.metrics.AddBytesTotal(bytesServed)
	}()

	// Track access for each multihash
	for _, m := range mh {
		r.accessTracker.RecordAccess(rbstor.AccessEvent{
			Key:      m.String(),
			GroupKey: group,
		})
	}

	for i, m := range mh {
		key := mhStr(m)

		// Try L1 ARC cache first
		if r.l1Cache != nil {
			if b, ok := r.l1Cache.Get(key); ok {
				cb(i, b)
				l1Hits++
				bytesServed += int64(len(b))
				mh[i] = nil
				continue
			}
		} else if r.blockCache != nil {
			// Legacy LRU fallback
			if b, ok := r.blockCache.Get(key); ok {
				cb(i, b)
				l1Hits++
				bytesServed += int64(len(b))
				mh[i] = nil
				continue
			}
		}

		// Try L2 SSD cache
		if r.l2Cache != nil {
			if b, ok := r.l2Cache.Get(string(key)); ok {
				cb(i, b)
				l2Hits++
				bytesServed += int64(len(b))
				// Promote to L1 on L2 hit
				if r.l1Cache != nil {
					r.l1Cache.Put(key, b)
				}
				mh[i] = nil
				continue
			}
		}
	}

	cacheHits := l1Hits + l2Hits
	r.metrics.AddCacheHits(int64(cacheHits))
	r.metrics.AddCacheMisses(int64(len(mh) - cacheHits))

	if cacheHits == len(mh) {
		return nil
	}

	httpHits := 0

	// try http gateway
	{
		cc, err := r.retrievalCandidatesForGroupCached(group)
		if err != nil {
			return xerrors.Errorf("failed to get retrieval candidates: %w", err)
		}
		candidates := cc.candidates

		var hasHttpCandidates bool
		for _, candidate := range candidates {
			addrInfo, err := r.getAddrInfoCached(candidate.Provider)
			if err != nil {
				log.Warnw("failed to get addrinfo", "provider", candidate.Provider, "err", err)
				continue
			}

			if len(addrInfo.HttpMaddrs) == 0 {
				continue
			}

			_, err = ributil.MaddrsToUrl(addrInfo.HttpMaddrs)
			if err != nil {
				log.Warnw("failed to parse addrinfo", "provider", candidate.Provider, "err", err)
				continue
			}

			hasHttpCandidates = true
		}

		if hasHttpCandidates {
			r.metrics.IncHttpTries()

			for i, hashToGet := range mh {
				if hashToGet == nil {
					continue
				}

				cidToGet := cid.NewCidV1(cid.Raw, hashToGet)

				promise, err := r.retrievalPromise(ctx, cidToGet, i, cb)
				if err != nil {
					return err
				}
				if promise == nil {
					// already done
					continue
				}

				// todo could do in goroutines once FetchBlocks actually calls with multiple hashes

				var wg sync.WaitGroup
				var anySuccess bool
				var successOnce sync.Once
				ctx, cancel := context.WithCancel(ctx)

				done := make(chan struct{}, 2)

				for _, candidate := range candidates {
					candidate := candidate

					addrInfo, err := r.getAddrInfoCached(candidate.Provider)
					if err != nil {
						log.Warnw("failed to get addrinfo", "provider", candidate.Provider, "err", err)
						continue
					}

					if len(addrInfo.HttpMaddrs) == 0 {
						continue
					}

					u, err := ributil.MaddrsToUrl(addrInfo.HttpMaddrs)
					if err != nil {
						log.Warnw("failed to parse addrinfo", "provider", candidate.Provider, "err", err)
						continue
					}

					log.Debugw("attempting http retrieval", "url", u.String(), "group", group, "provider", candidate.Provider)

					wg.Add(1)
					go func() {
						defer wg.Done()

						err = r.doHttpRetrieval(ctx, group, candidate.Provider, u, cidToGet, func(data []byte) {
							successOnce.Do(func() {
								r.ongoingRequestsLk.Lock()
								delete(r.ongoingRequests, cidToGet)
								r.ongoingRequestsLk.Unlock()

								// Insert into cache hierarchy
								r.cacheBlock(mhStr(hashToGet), data)

								promise.res = data
								close(promise.done)
								cancel()
								anySuccess = true
								done <- struct{}{}
							})
						})
						_ = err // already logged in doHttpRetrieval
					}()
				}

				go func() {
					wg.Wait()
					done <- struct{}{}
				}()

				<-done

				cancel()
				if !anySuccess {
					promise.claimed = false // allow retry from another source
					continue
				}

				cb(i, promise.res)
				bytesServed += int64(len(promise.res))
				mh[i] = nil
				httpHits++
				r.metrics.IncHttpSuccess(int64(len(promise.res)))
			}

		}
	}

	if cacheHits+httpHits == len(mh) {
		log.Debugw("http retrieval success", "group", group, "cacheHits", cacheHits, "httpHits", httpHits)
		return nil
	}

	r.metrics.AddFailed(int64(len(mh) - cacheHits - httpHits))
	return nil
}

func (r *retrievalProvider) doHttpRetrieval(ctx context.Context, group iface.GroupKey, prov int64, u *url.URL, cidToGet cid.Cid, cb func([]byte)) error {
	// make a request
	// like curl -H "Accept:application/vnd.ipld.raw;" http://{SP's http retrieval URL}/ipfs/bafySomeBlockCID -o bafySomeBlockCID

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second) // todo make tunable, use mostly for ttfb
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String()+"/ipfs/"+cidToGet.String(), nil)
	if err != nil {
		cancel()
		return xerrors.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.ipld.raw;")
	req.Header.Set("User-Agent", "ribs/0.0.0")

	resp, err := http.DefaultClient.Do(req) // todo use a tuned client
	if err != nil {
		log.Warnw("http retrieval failed", "error", err, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", prov)
		return xerrors.Errorf("failed to do request: %w", err)
	}

	if resp.StatusCode != 200 {
		log.Warnw("http retrieval failed (non-200 response)", "status", resp.StatusCode, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", prov)
		return xerrors.Errorf("non-200 response: %d", resp.StatusCode)
	}

	bbuf := pool.Get(carlog.MaxEntryLen)
	defer pool.Put(bbuf)

	n, err := io.ReadFull(resp.Body, bbuf)
	if err != nil && err != io.ErrUnexpectedEOF {
		_ = resp.Body.Close()
		log.Warnw("http retrieval failed (failed to read response)", "error", err, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", prov)
		return xerrors.Errorf("failed to read response: %w", err)
	}
	bbuf = bbuf[:n]

	if err := resp.Body.Close(); err != nil {
		log.Warnw("http retrieval failed (failed to close response)", "error", err, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", prov)
		return xerrors.Errorf("failed to close response: %w", err)
	}

	checkCid, err := cidToGet.Prefix().Sum(bbuf)
	if err != nil {
		log.Warnw("http retrieval failed (failed to hash response)", "error", err, "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", prov)
		return xerrors.Errorf("failed to hash response: %w", err)
	}

	if !checkCid.Equals(cidToGet) {
		log.Warnw("http retrieval failed (response hash mismatch!!!)", "url", u.String()+"/ipfs/"+cidToGet.String(), "group", group, "provider", prov, "expected", cidToGet, "actual", checkCid)
		return xerrors.Errorf("response hash mismatch")
	}

	cbbuf := make([]byte, len(bbuf))
	copy(cbbuf, bbuf)

	cb(cbbuf)
	return nil
}

func (r *retrievalProvider) retrievalPromise(ctx context.Context, cidToGet cid.Cid, i int, cb func(cidx int, data []byte)) (*requestPromise, error) {
	r.ongoingRequestsLk.Lock()

	if or, ok := r.ongoingRequests[cidToGet]; ok {
		if !or.claimed {
			or.claimed = true
			r.ongoingRequestsLk.Unlock()
			return or, nil
		}

		r.ongoingRequestsLk.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-or.done:
		}

		if or.err != nil {
			return nil, xerrors.Errorf("retr promise error: %w", or.err)
		}

		cb(i, or.res)
		return nil, nil
	}

	promise := &requestPromise{
		done:    make(chan struct{}),
		claimed: true,
	}

	r.ongoingRequests[cidToGet] = promise
	r.ongoingRequestsLk.Unlock()

	return promise, nil
}

// CacheStats returns combined L1/L2 cache statistics
func (r *retrievalProvider) CacheStats() iface.CacheStats {
	stats := iface.CacheStats{}

	// L1 ARC cache stats
	if r.l1Cache != nil {
		stats.L1Enabled = true
		l1Stats := r.l1Cache.Stats()
		stats.L1Size = l1Stats.Size
		stats.L1Capacity = l1Stats.Capacity
		stats.L1Items = l1Stats.Items
		stats.L1T1Size = l1Stats.T1Size
		stats.L1T2Size = l1Stats.T2Size
		stats.L1B1Len = l1Stats.B1Len
		stats.L1B2Len = l1Stats.B2Len
		stats.L1P = l1Stats.P
	}

	// L2 SSD cache stats
	if r.l2Cache != nil {
		stats.L2Enabled = true
		l2Stats := r.l2Cache.Stats()
		stats.L2Size = l2Stats.Size
		stats.L2MaxSize = l2Stats.MaxSize
		stats.L2Items = l2Stats.Items
		stats.L2ProbationSize = l2Stats.ProbationSize
		stats.L2ProtectedSize = l2Stats.ProtectedSize
		stats.L2FreeSpace = l2Stats.FreeSpace
	}

	// Hit/miss counters from metrics
	if r.metrics != nil {
		stats.Hits = int64(metrics.GetCounterValue(r.metrics.cacheHit))
		stats.Misses = int64(metrics.GetCounterValue(r.metrics.cacheMiss))
	}

	return stats
}
