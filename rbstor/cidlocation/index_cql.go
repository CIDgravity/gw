package cidlocation

import (
	"errors"
	"fmt"

	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"github.com/ipfs/go-cid"
	"github.com/yugabyte/gocql"
)

type IndexCql struct {
	db cqldb.Database
}

func NewCidLocationIndex(db cqldb.Database) LocationIndex {
	return &IndexCql{db}
}

func (i *IndexCql) GetCidLocation(cid cid.Cid) ([]iface.GroupKey, error) {
	query := i.db.Query("select groups from CidGroups where cid = ?", cid.String())
	var groups []iface.GroupKey
	err := query.Scan(&groups)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get cid location query: %w", err)
	}
	return groups, nil
}

func (i *IndexCql) PutCidLocation(cid cid.Cid, groups []iface.GroupKey) error {
	query := i.db.Query("insert into CidGroups (cid, groups) values (?, ?)", cid.String(), groups)
	err := query.Exec()
	if err != nil {
		return fmt.Errorf("put cid location query: %w", err)
	}
	return nil
}
