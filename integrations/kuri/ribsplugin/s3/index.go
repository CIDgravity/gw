package s3

import (
	"context"
	"github.com/filecoin-project/go-hamt-ipld"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"sync"
)

type Index struct {
	node     *hamt.Node
	hamtlock sync.RWMutex
	store    cbor.IpldStore
}

func (i *Index) Set(ctx context.Context, key string, cid cid.Cid) error {
	i.hamtlock.Lock()
	defer i.hamtlock.Unlock()
	err := i.node.Set(ctx, key, cid)
	return err
}

func (i *Index) Get(ctx context.Context, key string) (cid.Cid, error) {
	var objCid cid.Cid

	i.hamtlock.RLock()
	defer i.hamtlock.RUnlock()
	err := i.node.Find(ctx, key, &objCid)
	return objCid, err
}

func (i *Index) Delete(ctx context.Context, key string) error {
	i.hamtlock.Lock()
	defer i.hamtlock.Unlock()
	return i.node.Delete(ctx, key)
}

func (i *Index) Flush(ctx context.Context) (cid.Cid, error) {
	i.hamtlock.Lock()
	defer i.hamtlock.Unlock()
	err := i.node.Flush(ctx)
	if err != nil {
		return cid.Cid{}, err
	}

	c, err := i.store.Put(ctx, i.node)
	return c, err
}
