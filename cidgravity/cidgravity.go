package cidgravity

import (
	"sync"
	"github.com/lotus-web3/ribs/configuration"
	"golang.org/x/sync/semaphore"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("cidg")

type CIDGravity struct {
	lk sync.Mutex
	sem *semaphore.Weighted
}

func (cidg *CIDGravity) init() error {
	cidg.lk.Lock()
	defer cidg.lk.Unlock()
	if cidg.sem == nil {
		cfg := configuration.GetConfig()
		cidg.sem = semaphore.NewWeighted(cfg.CidGravity.MaxConns)
	}
	return nil
}
