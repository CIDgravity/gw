package s3

import (
	"context"
	"github.com/filecoin-project/go-hamt-ipld"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"sync"
)

type Index struct {
	lk    sync.RWMutex
	node  *hamt.Node
	store cbor.IpldStore
}

func (i *Index) Set(ctx context.Context, key string, cid cid.Cid) error {
	i.lk.Lock()
	defer i.lk.Unlock()
	err := i.node.Set(ctx, key, cid)
	return err
}

func (i *Index) Get(ctx context.Context, key string) (cid.Cid, error) {
	var objCid cid.Cid

	i.lk.RLock()
	defer i.lk.RUnlock()
	err := i.node.Find(ctx, key, &objCid)
	return objCid, err
}

func (i *Index) Delete(ctx context.Context, key string) error {
	i.lk.Lock()
	defer i.lk.Unlock()
	return i.node.Delete(ctx, key)
}

func (i *Index) Flush(ctx context.Context) (cid.Cid, error) {
	i.lk.Lock()
	defer i.lk.Unlock()
	err := i.node.Flush(ctx)
	if err != nil {
		return cid.Cid{}, err
	}

	c, err := i.store.Put(ctx, i.node)
	return c, err
}
