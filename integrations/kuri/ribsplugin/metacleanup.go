package kuboribs

import (
	"context"
	"sort"
	"strings"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/ipfs/boxo/ipld/merkledag"

	"github.com/lotus-web3/ribs"
	//"golang.org/x/xerrors"
)

type ExplorerInfo struct {
	dag format.DAGService
	rbs ribs.Storage
}
func (e ExplorerInfo) getNode(c string) (*merkledag.ProtoNode, error) {
	c_, err := cid.Decode(c)
	if err != nil {
		log.Errorw("Failed to decode CID", "cid", c)
		return nil, err
	}
	rnd, err := e.dag.Get(context.TODO(), c_)
	if err != nil {
		log.Errorw("error loading CID from DAG", "error", err, "cid", c)
		if strings.HasPrefix(err.Error(), "block was not found locally") {
			return nil, nil
		}
		return nil, err
	}

	pbnd, ok := rnd.(*merkledag.ProtoNode)
	if !ok {
		log.Warnw("error loading CID: not ProtoNode", "cid", c)
		return nil, nil
	}
	return pbnd, nil
}
func (e ExplorerInfo) ListChilds(c string) (map[string]ribs.ChildInfo, error) {
	log.Debugw("ListChilds", "cid", c)
	node, err := e.getNode(c)
	if err != nil {
		log.Errorw("Failed to decode CID", "cid", c)
		return nil, err
	}
	ret := make(map[string]ribs.ChildInfo)
	if node != nil {
		for _, child := range node.Links() {
			if child.Name != "" {
				ret[child.Name] = ribs.ChildInfo{
					Cid: child.Cid.String(),
					Size: child.Size,
				}
			}
		}
	}
	return ret, nil
}
func (e ExplorerInfo) addGroups(c cid.Cid, grps map[int64]bool) (map[int64]bool, error) {
	groups, err := e.rbs.FindHashes(context.TODO(), c.Hash())
	if err != nil {
		log.Errorw("Failed to find hash for CID", "cid", c)
		return nil, err
	}
	for _, grp := range groups {
		grps[grp] = true
	}
	return grps, nil
}
func (e ExplorerInfo) ListGroups(c string) ([]int64, error) {
	log.Debugw("ListGroups", "cid", c)
	node, err := e.getNode(c)
	if err != nil {
		log.Errorw("Failed to decode CID", "cid", c)
		return nil, err
	}
	grps := make(map[int64]bool)
	if node != nil {
		grps, err = e.addGroups(node.Cid(), grps)
		if err != nil {
			log.Errorw("Failed to find hash for root CID", "cid", c)
			return nil, err
		}
		for _, child := range node.Links() {
			grps, err = e.addGroups(child.Cid, grps)
			if err != nil {
				log.Errorw("Failed to find hash for CID", "cid", child.Cid.String(), "fileCid", c)
				return nil, err
			}
		}
	} else {  // TODO: mutualize cid decode, and get some code here
		c_, err := cid.Decode(c)
		if err != nil {
			log.Errorw("Failed to decode CID", "cid", c)
			return nil, err
		}
		grps, err = e.addGroups(c_, grps)
		if err != nil {
			log.Errorw("Failed to find hash for root CID", "cid", c)
			return nil, err
		}
	}
	var ret []int64
	for grp := range grps {
		ret = append(ret, grp)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
	return ret, nil
}

func StartMeta(/* lc fx.Lifecycle, */mdb ribs.MetadataDB, r ribs.RIBS, dag format.DAGService) {
	explorer := ExplorerInfo{
		dag: dag,
		rbs: r.Storage(),
	}
	mdb.LaunchCleanupLoop(explorer)
	mdb.LaunchServer()
}
