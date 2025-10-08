package cidlocation

import (
	"fmt"

	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/ipfs/go-cid"
)

var errNotFound = fmt.Errorf("not found")

type LocationIndex interface {
	GetCidLocation(cid cid.Cid) ([]iface.GroupKey, error)
	PutCidLocation(cid cid.Cid, groups []iface.GroupKey) error
}
