package rbmeta

import (
	"context"
	"errors"
	gopath "path"
	"strings"
	"sync"
	"time"
	//        "github.com/ipfs/go-cid"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	logging "github.com/ipfs/go-log/v2"
	"github.com/lotus-web3/ribs/configuration"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/xerrors"
)

const maxPathByUpdate = 10

var log = logging.Logger("ribs:rbmeta")

type metaDB struct {
	conn        *mongo.Client
	db          *mongo.Database
	cMetaFile   *mongo.Collection
	cChilds     *mongo.Collection
	needCleanup bool
	cleanupSync sync.Mutex
	wakeup      *chan struct{}
}

func Open() (*metaDB, error) {
	log.Debugw("Opening mongoDB")
	cfg := configuration.GetConfig()
	uri := cfg.Ribs.MongoDBUri

	if uri == "" {
		return nil, xerrors.Errorf("Mongo URI not provided")
	}
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	db := client.Database("ribs")
	cMetaFile := db.Collection("metaFiles")
	cChilds := db.Collection("childs")

	ret := &metaDB{
		conn:        client,
		db:          db,
		cMetaFile:   cMetaFile,
		cChilds:     cChilds,
		needCleanup: true,
	}
	c := make(chan struct{}, 1)
	ret.wakeup = &c
	return ret, nil
}

func SplitFilePath(p string) (string, string, string, error) {
	if len(p) == 0 {
		p = "/"
	}

	if p[0] != '/' {
		p = "/" + p
	}
	prefixes := strings.SplitN(p, "/", 3)

	cleaned := gopath.Clean(p)
	if p[len(p)-1] == '/' && p != "/" {
		cleaned += "/"
	}
	splited := strings.SplitN(cleaned, "/", 3)
	if prefixes[1] != splited[1] {
		log.Errorw("Cleaning path error", "path", p)
		return "", "", "", xerrors.Errorf("Error cleaning path: %s", p)
	}
	if len(splited) == 2 {
		if splited[1] == "" {
			return "", "", "/", nil
		}
		return splited[1], "/", splited[1], nil
	}
	if len(splited) == 3 {
		user := splited[1]
		idx := strings.LastIndex(cleaned, "/")
		path := cleaned[:idx]
		name := cleaned[idx+1:]
		return user, path, name, nil
	}
	log.Errorw("INTERNAL: SplitFilePath", "path", p)
	return "", "", "", xerrors.Errorf("Internal error cleaning path: %s", p)
}

type resultCid struct {
	Cid     string `bson:"cid"`
	EndTime *int64 `bson:"end_ts"`
}

func (mdb *metaDB) askCleanup() {
	mdb.cleanupSync.Lock()
	defer mdb.cleanupSync.Unlock()
	if !mdb.needCleanup {
		mdb.needCleanup = true
		*mdb.wakeup <- struct{}{}
	}
}
func (mdb *metaDB) waitCleanupRequired() {
	for {
		mdb.cleanupSync.Lock()
		if mdb.needCleanup {
			mdb.needCleanup = false
			mdb.cleanupSync.Unlock()
			break
		}
		mdb.cleanupSync.Unlock()
		<-*mdb.wakeup
	}
	for {
		select {
		case <-*mdb.wakeup:
		default:
			return
		}
	}
}

func (mdb *metaDB) alreadyHasCid(user, parent, name string, c string, ts *int64) (bool, error) {
	log.Debugw("alreadyHasCid", "user", user, "parent", parent, "name", name, "cid", c, "ts", ts)
	ctx := context.TODO()
	col := mdb.cMetaFile
	filter := bson.M{"user": user, "parent": parent, "name": name}
	if ts != nil {
		filter["start_ts"] = bson.M{"$lte": *ts}
	}
	opts := options.Find()

	opts.SetSort(bson.D{{Key: "start_ts", Value: -1}})
	opts.SetProjection(bson.M{"cid": true, "end_ts": true, "_id": false})
	opts.SetLimit(1)

	cursor, err := col.Find(ctx, filter, opts)
	if err != nil {
		log.Errorw("alreadyHasCid Find()", "user", user, "parent", parent, "name", name, "cid", c, "ts", ts, "error", err)
		return false, err
	}
	res := make([]resultCid, 1)
	if err := cursor.All(ctx, &res); err != nil {
		log.Errorw("alreadyHasCid Find.All()", "user", user, "parent", parent, "name", name, "cid", c, "ts", ts, "error", err)
		return false, err
	}
	if len(res) == 1 && res[0].Cid == c && (res[0].EndTime == nil || ts != nil && *res[0].EndTime > *ts) {
		return true, nil
	}
	return false, nil
}
func (mdb *metaDB) updateCid(user, parent, name string, c string, size *uint64, isFile *bool) error {
	log.Debugw("updateCid", "user", user, "parent", parent, "name", name, "cid", c, "size", size, "isFile", isFile)
	ctx := context.TODO()
	col := mdb.cMetaFile
	nowts := time.Now().UnixNano()
	data := bson.M{"user": user, "parent": parent, "name": name, "cid": c, "cleanup_required": true, "start_ts": nowts}
	if size != nil {
		data["size"] = *size
	}
	if isFile != nil {
		data["file"] = *isFile
	}
	_, err := col.InsertOne(ctx, data)
	if err != nil {
		log.Errorw("updateCid Insert", "user", user, "parent", parent, "name", name, "cid", c, "size", size, "isFile", isFile, "error", err)
		return err
	}
	mdb.askCleanup()
	return nil
}
func (mdb *metaDB) insertChild(ctx context.Context, user, parent, name string, c ChildInfo, startTime int64) (bool, error) {
	log.Debugw("insertChild", "user", user, "parent", parent, "name", name, "info", c, "startTime", startTime)
	uptodate, err := mdb.alreadyHasCid(user, parent, name, c.Cid, &startTime)
	if err != nil {
		log.Errorw("insertChild alreadyHasCid", "user", user, "parent", parent, "name", name, "info", c, "startTime", startTime, "error", err)
		return false, err
	}
	if uptodate {
		return false, nil
	}
	col := mdb.cMetaFile
	data := bson.M{"user": user, "parent": parent, "name": name, "cid": c.Cid, "size": c.Size, "cleanup_required": true, "start_ts": startTime}
	if _, err := col.InsertOne(ctx, data); err != nil {
		log.Errorw("insertChild insertOne", "user", user, "parent", parent, "name", name, "info", c, "startTime", startTime, "error", err)
		return false, err
	}
	mdb.askCleanup()
	return true, nil

}

/* Functions that should be fast, done directly as we write */
func (mdb *metaDB) WriteFile(filepath string, c string, size uint64) error {
	log.Debugw("metaDB.WriteFile", "path", filepath, "cid", c)
	user, parent, name, err := SplitFilePath(filepath)
	if err != nil {
		log.Errorw("WriteFile split", "filepath", filepath, "error", err)
		return err
	}
	uptodate, err := mdb.alreadyHasCid(user, parent, name, c, nil)
	if err != nil {
		log.Errorw("WriteFile alreadyHasCid", "filepath", filepath, "error", err)
		return err
	}
	if uptodate {
		return nil
	}
	isFile := true
	if err := mdb.updateCid(user, parent, name, c, &size, &isFile); err != nil {
		log.Errorw("WriteFile updateCid", "filepath", filepath, "error", err)
		return err
	}
	return nil
}
func (mdb *metaDB) WriteDir(filepath string, c string) error {
	log.Debugw("metaDB.WriteDir", "path", filepath, "cid", c)
	user, parent, name, err := SplitFilePath(filepath)
	if err != nil {
		log.Errorw("WriteDir split", "filepath", filepath, "error", err)
		return err
	}
	uptodate, err := mdb.alreadyHasCid(user, parent, name, c, nil)
	if err != nil {
		log.Errorw("WriteDir alreadyHasCid", "filepath", filepath, "error", err)
		return err
	}
	if uptodate {
		return nil
	}
	isFile := false
	if err := mdb.updateCid(user, parent, name, c, nil, &isFile); err != nil {
		log.Errorw("WriteDir updateCid", "filepath", filepath, "error", err)
		return err
	}
	return nil
}

type latestEndTSResult struct {
	Id      primitive.ObjectID `bson:"_id"`
	EndTime *int64             `bson:"end_ts"`
}

func (mdb *metaDB) Remove(filepath string) error {
	log.Debugw("metaDB.Remove", "path", filepath)
	// Search for latest ID
	ctx := context.TODO()
	col := mdb.cMetaFile

	user, parent, name, err := SplitFilePath(filepath)
	if err != nil {
		log.Errorw("Remove split", "filepath", filepath, "error", err)
		return err
	}

	filter := bson.M{"user": user, "parent": parent, "name": name}
	opts := options.Find()

	opts.SetSort(bson.D{{Key: "start_ts", Value: -1}})
	opts.SetProjection(bson.M{"end_ts": 1, "_id": 1})
	opts.SetLimit(1)

	cursor, err := col.Find(ctx, filter, opts)
	if err != nil {
		log.Errorw("Remove Find", "filepath", filepath, "error", err)
		return err
	}
	res := make([]latestEndTSResult, 1)
	if err := cursor.All(ctx, &res); err != nil {
		log.Errorw("Remove Find.All", "filepath", filepath, "error", err)
		return err
	}
	if len(res) < 1 || res[0].EndTime != nil {
		// last already has an EndTime, so nothing to do
		log.Warnw("Removing file not found", "filename", filepath)
		return nil
	}
	ufilter := bson.M{"_id": res[0].Id}
	nowts := time.Now().UnixNano()
	update := bson.M{"$set": bson.M{"cleanup_required": true, "end_ts": nowts}}
	if _, err := col.UpdateOne(ctx, ufilter, update); err != nil {
		log.Errorw("Remove UpdateOne", "filepath", filepath, "error", err)
		return err
	}
	mdb.askCleanup()
	return nil
}

// FileMetadata
func (mdb *metaDB) Rename(oldName, newName string) error {
	log.Debugw("metaDB.Rename", "src", oldName, "dst", newName)
	if oldName == newName {
		log.Warnw("Trying to rename to the same name", "filename", oldName)
		return nil
	}
	ctx := context.TODO()
	col := mdb.cMetaFile

	nowts := time.Now().UnixNano()
	res := make([]FileMetadata, 1)
	// find existing file info first
	{
		user, parent, name, err := SplitFilePath(oldName)
		if err != nil {
			log.Errorw("Rename split", "filepath", oldName, "error", err)
			return err
		}

		filter := bson.M{"user": user, "parent": parent, "name": name}
		opts := options.Find()

		opts.SetSort(bson.D{{Key: "start_ts", Value: -1}})
		opts.SetLimit(1)

		cursor, err := col.Find(ctx, filter, opts)
		if err != nil {
			log.Errorw("Rename find", "filepath", oldName, "error", err)
			return err
		}
		if err := cursor.All(ctx, &res); err != nil {
			log.Errorw("Rename find.All", "filepath", oldName, "error", err)
			return err
		}
		if len(res) < 1 || res[0].EndTime != nil {
			log.Warnw("Moving file not found", "filename", oldName)
			return nil
		}
		// mark old as removed
		ufilter := bson.M{"_id": res[0].Id}
		update := bson.M{"$set": bson.M{"cleanup_required": true, "end_ts": nowts}}
		if _, err := col.UpdateOne(ctx, ufilter, update); err != nil {
			log.Errorw("Rename UpdateOne", "filepath", oldName, "error", err)
			return err
		}
	}
	{
		// create new entry, and copy all existing info
		user, parent, name, err := SplitFilePath(newName)
		if err != nil {
			log.Errorw("Rename split", "filepath", newName, "error", err)
			return err
		}
		obj := bson.M{
			"user":             user,
			"parent":           parent,
			"name":             name,
			"cid":              res[0].Cid,
			"file":             res[0].File,
			"cleanup_required": true,
			"start_ts":         nowts,
		}
		if res[0].Size != nil {
			obj["size"] = *res[0].Size
		}
		if res[0].Groups != nil {
			obj["groups"] = *res[0].Groups
		}
		if _, err := col.InsertOne(ctx, obj); err != nil {
			log.Errorw("Rename insertOne", "filepath", newName, "error", err)
			return err
		}
	}
	mdb.askCleanup()
	return nil
}

/*
type FileMetadata struct {
        // All as ref, so they are optionnal in usage
        Id         *primitive.ObjectID `bson:"_id"`
        User       *string             `bson:"user"`
        ParentPath *string             `bson:"parent"`
        Filename   *string             `bson:"name"`
        Cid        *string             `bson:"cid"`
        File       *bool               `bson:"file"`
        Size       *uint64             `bson:"size"`
        StartTime  *int64              `bson:"start_ts"`
        EndTime    *int64              `bson:"end_ts"`
        Groups     *[]int64            `bson:"groups"`
}

FileMetaChilds
	CID             <cid>
	Childs          <name> <cid>
	Groups          <list of groups, including childs>


Cleanups we should look for:
  * update cid (file or dir)
  * remove
  * rename (remove + create)

Search: path + cleanup_required
  * Get all entries with same path, and:
	* cleanup_required
	or
	* no end_ts set
    By start_ts:
	* set end_ts for previous entry
	* ensure the rest of the checks (once done looping) if cleanup_required

  * dir:
	* check we have CID -> files
	* Compare with previous dir version (empty if none)
	* Also compare with next (empty) when last and end_ts set
		* check we have all childs existing at the proper timestamp(*)
		* check we had a remove on all previous childs
			Need to have child.start_ts <= parent_ts
			Need to have child.end_ts not set, or >= parent.end_ts
		* Collect list of files at specified TS to confirm
  * missing groups
	- file, populate directly
	- dir, aggregate from childs, the check self and populate
  * missing size?
	- dir: sum childs
*/
func (mdb *metaDB) runCleanup(e Explorer) error {
	log.Debugw("Running cleanup...")
	ctx := context.TODO()
	for {
		todo, err := mdb.listOfRequiredUpdate(ctx)
		if err != nil {
			log.Errorw("Faild to list Required Updates", "error", err)
			return err
		}
		if len(todo) == 0 {
			return nil
		}
		log.Debugw("Required Updates", "count", len(todo))
		for len(todo) > 0 {
			log.Debugw("Todo...", "count", len(todo))
			others, err := mdb.cleanupPath(ctx, e, todo[0])
			if err != nil {
				log.Debugw("Cleanup failed", "parent", *todo[0].ParentPath, "name", *todo[0].Filename, "error", err)
				return err
			}
			todo = todo[1:]
			if others != nil && len(others) > 0 {
				log.Debugw("Prepend with # entries", "count", len(others))
				log.Debugw("XXX Prepend details", "info", others)
				todo = append(others, todo...)
			}
		}
	}
}
func (mdb *metaDB) cleanupPath(ctx context.Context, e Explorer, fi FileMetadata) ([]FileMetadata, error) {
	log.Debugw("cleanupPath", "user", *fi.User, "parent", *fi.ParentPath, "name", *fi.Filename)
	history, err := mdb.getPathHistory(ctx, *fi.User, *fi.ParentPath, *fi.Filename, *fi.StartTime)
	if err != nil {
		log.Errorw("cleanupPath getPathHistory", "fileinfo", fi, "error", err)
		return nil, err
	}
	if len(history) < 1 {
		log.Errorw("Failed to gather cleanup info", "fileinfo", fi)
		return nil, xerrors.Errorf("Failed to find any cleanup info: %s/%s", *fi.ParentPath, *fi.Filename)
	}
	ret := make([]FileMetadata, 0)
	log.Debugw("cleanup history", "user", *fi.User, "parent", *fi.ParentPath, "name", *fi.Filename, "history", len(history))
	for n, entry := range(history[:len(history)-1]) {
		if entry.EndTime != nil && *entry.EndTime > *history[n + 1].StartTime {
			log.Errorw("Found entry EndTime after next StartTime!", "fileinfo", fi, "NextStarTime", *history[n + 1].StartTime)
			entry.EndTime = history[n + 1].StartTime
		}
		if entry.EndTime == nil {
			err = mdb.forceEndTime(ctx, &entry, *history[n + 1].StartTime)
			if err != nil {
				log.Errorw("Failed to forceEndTime", "error", err)
				return nil, err
			}
			history[n] = entry
		}
	}
	// ensure child list set, with adequate info
	for n, entry := range(history) {
		if entry.CleanupRequired == nil || !*entry.CleanupRequired {
			continue
		}
		// first update child list
		if entry.File == nil || !*entry.File {
			// (might be?) a directory
			var prev *FileMetadata
			propagateEndTime := false
			if n > 0 {
				le := history[n-1]
				if le.EndTime == nil {
					log.Errorw("Previous entry still has no EndTime", "prev", le, "entry", entry)
				}
				// not sure how it would already have an EndTime
				// but if it does, and is our current StarTime, they are together
				if le.EndTime == nil || *le.EndTime == *entry.StartTime {
					prev = &history[n-1]
				}
			}
			if entry.EndTime != nil && (n + 1 == len(history) || *entry.EndTime != *history[n+1].StartTime) {
				propagateEndTime = true
			}
			needUpdate, err := mdb.ensureChildList(ctx, e, entry, prev, propagateEndTime)
			if err != nil {
				log.Errorw("Failed to ensureChildList", "error", err)
				return nil, err
			}
			if needUpdate != nil && len(needUpdate) > 0 {
				found := false
				for _, child := range(needUpdate) {
					for _, known := range(ret) {
						if *child.ParentPath == *known.ParentPath && *child.Filename == *known.Filename {
							found = true
							break
						}
					}
					if !found {
						log.Debugw("XXX: appending child [1]", "info", child)
						ret = append(ret, child)
					}
				}
				// child need updates, do not continue for now
				continue
			}
		}
		// now that we do have child info, get the rest done...
		// XXX: TODO: update groups
		mdb.markCleaned(ctx, entry)
	}
	// now that child lists are ok, ensure we have proper end time on each
	if len(ret) == 0 {
		ret = nil
	} else {
		log.Debugw("XXX: appending self", "info", fi)
		ret = append(ret, fi)
	}
	return ret, nil
}
func (mdb *metaDB) markCleaned(ctx context.Context, fi FileMetadata) error {
	col := mdb.cMetaFile
	log.Debugw("Making cleaned", "id", fi.Id)
	ufilter := bson.M{"_id": *fi.Id}
	update := bson.M{"$unset": bson.M{"cleanup_required": false}}
	count, err := col.UpdateOne(ctx, ufilter, update)
	if err != nil {
		log.Errorw("Marked cleaned", "error", err)
		return err
	}
	log.Debugw("Marked cleaned", "res", count)
	return nil
}
func (mdb *metaDB) listChilds(ctx context.Context, e Explorer, c string) (map[string]ChildInfo, error) {
	log.Debugw("listChilds", "cid", c)
	col := mdb.cChilds
	cursor, err := col.Find(ctx, bson.M{"cid": c}, options.Find())
	if err != nil {
		log.Errorw("listChilds Find", "cid", c, "error", err)
		return nil, err
	}
	res := make([]ChildMetadata, 1)
	if err := cursor.All(ctx, &res); err != nil {
		log.Errorw("listChilds Find.All", "cid", c, "error", err)
		return nil, err
	}
	if len(res) != 0 {
		if len(res) > 1 {
			log.Errorw("Found multiple Child list with same CID", "cid", c)
		}
		return res[0].Childs, nil
	}
	childs, err := e.ListChilds(c)
	if err != nil {
		log.Errorw("listChilds ListChilds", "cid", c, "error", err)
		return nil, err
	}
	data := bson.M{"cid": c, "childs": childs}
	if _, err := col.InsertOne(ctx, data); err != nil {
		log.Errorw("listChilds insertOne", "cid", c, "error", err)
		return nil, err
	}
	return childs, nil
}

func (mdb *metaDB) ensureChildList(ctx context.Context, e Explorer, fi FileMetadata, prev *FileMetadata, propagateEndTime bool) ([]FileMetadata, error) {
	log.Debugw("ensureChildList", "file", fi, "prev", prev, "propagateEndTime", propagateEndTime)
	ret := make([]FileMetadata, 0)
	var oldChilds map[string]ChildInfo
	if prev == nil {
		oldChilds = make(map[string]ChildInfo)
	} else {
		var err error
		oldChilds, err = mdb.listChilds(ctx, e, *prev.Cid)
		if err != nil {
			log.Errorw("ensureChildList listChilds(old)", "file", fi, "prev", prev, "propagateEndTime", propagateEndTime, "error", err)
			return nil, err
		}
	}
	childs, err := mdb.listChilds(ctx, e, *fi.Cid)
	if err != nil {
		log.Errorw("ensureChildList listchilds(current)", "file", fi, "prev", prev, "propagateEndTime", propagateEndTime, "error", err)
		return nil, err
	}
	for name, info := range(childs) {
		log.Debugw("XXX ensureChildList...", "name", name, "child", info)
		oldInfo, hasOld := oldChilds[name]
		user := *fi.User
		parent := gopath.Join(*fi.ParentPath, *fi.Filename)
		if user == "" && parent == "/" {
			user = name
		}
		log.Debugw("XXX child computed info", "user", user, "parent", parent, "name", name)
		if !(hasOld && oldInfo.Cid == info.Cid) {
			changes, err := mdb.insertChild(ctx, user, parent, name, info, *fi.StartTime)
			if err != nil {
				log.Errorw("ensureChildList insertChild", "file", fi, "prev", prev, "propagateEndTime", propagateEndTime, "error", err)
				return nil, err
			}
			if changes {
				log.Debugw("XXX child computed info[2]", "user", user, "parent", parent, "name", name)
				n := name
				st := *fi.StartTime
				ret = append(ret, FileMetadata{
					User: &user,
					ParentPath: &parent,
					Filename: &n,
					StartTime: &st,
				})
				log.Debugw("XXX Appended new child[1]", "info", ret[len(ret)-1])
			}
		}
		if propagateEndTime {
			changes, err := mdb.forceEndTimeByName(ctx, user, parent, name, info.Cid, *fi.StartTime, *fi.EndTime)
			if err != nil {
				log.Errorw("ensureChildList forceEndTimeByName", "file", fi, "prev", prev, "propagateEndTime", propagateEndTime, "error", err)
				return nil, err
			}
			if changes {
				n := name
				st := *fi.StartTime
				ret = append(ret, FileMetadata{
					User: &user,
					ParentPath: &parent,
					Filename: &n,
					StartTime: &st,
				})
				log.Debugw("XXX Appended new child[2]", "info", ret[len(ret)-1])
			}
		}
	}
	for name, info := range(oldChilds) {
		_, found := childs[name]
		if found {
			continue
		}
		user := *fi.User
		parent := gopath.Join(*fi.ParentPath, *fi.Filename)
		if user == "" {
			user = name
		}
		changes, err := mdb.forceEndTimeByName(ctx, user, parent, name, info.Cid, *prev.StartTime, *fi.StartTime)
		if err != nil {
			log.Errorw("ensureChildList forceEndTimeByName", "file", fi, "prev", prev, "propagateEndTime", propagateEndTime, "error", err)
			return nil, err
		}
		if changes {
			n := name
			st := *fi.StartTime
			ret = append(ret, FileMetadata{
				User: &user,
				ParentPath: &parent,
				Filename: &n,
				StartTime: &st,
			})
			log.Debugw("XXX Appended old child[3]", "info", ret[len(ret)-1])
		}
	}
	log.Debugw("XXX ensureChildList ret", "info", ret)
	return ret, nil
}
func (mdb *metaDB) forceEndTime(ctx context.Context, fi *FileMetadata, endtime int64) error {
	// just updating end time when already having the rest ok, not sure we need to need update...
	// but just in case, mark it for further check anyway
	log.Debugw("forceEndTime", "info", fi, "EndTime", endtime)
	col := mdb.cMetaFile
	ufilter := bson.M{"_id": *fi.Id}
	update := bson.M{"$set": bson.M{"cleanup_required": true, "end_ts": endtime}}
	if _, err := col.UpdateOne(ctx, ufilter, update); err != nil {
		log.Errorw("forceEndTime", "error", err)
		return err
	}
	cleanupReq := true
	fi.CleanupRequired = &cleanupReq
	fi.EndTime = &endtime
	return nil
}
func (mdb *metaDB) forceEndTimeByName(ctx context.Context, user, parent, name, c string, starttime, endtime int64) (bool, error) {
	log.Debugw("forceEndTimeByName", "user", user, "parent", parent, "name", name, "cid", c, "start", starttime, "endtime", endtime)
	col := mdb.cMetaFile
	filter := bson.M{
		"user": user,
		"parent": parent,
		"name": name,
		"start_ts": bson.M{"$lte": starttime},
	}
	opts := options.Find()
	opts.SetSort(bson.D{{Key: "start_ts", Value: -1}})
	opts.SetProjection(bson.M{"cid": true, "end_ts": true})
	opts.SetLimit(1)

	cursor, err := col.Find(ctx, filter, opts)
	if err != nil {
		log.Errorw("forceEndTimeByName Find", "user", user, "parent", parent, "name", name, "cid", c, "start", starttime, "endtime", endtime, "error", err)
		return false, err
	}
	res := make([]FileMetadata, 1)
	if err := cursor.All(ctx, &res); err != nil {
		log.Errorw("forceEndTimeByName Find.All", "user", user, "parent", parent, "name", name, "cid", c, "start", starttime, "endtime", endtime, "error", err)
		return false, err
	}
	if len(res) == 0 || *res[0].Cid != c {
		log.Errorw("Failed to find entry on which to apply EndTime", "user", user, "parent", parent, "name", name, "cid", c, "StartTime", starttime, "EndTime", endtime)
		return false, xerrors.Errorf("Failed to find entry on which to apply EndTime: %s %s", parent, name)
	}
	if res[0].EndTime == nil || *res[0].EndTime > endtime {
		if res[0].EndTime != nil {
			log.Errorw("Endtime already set, but later than expected", "parent", parent, "name", name, "currentEndTime", *res[0].EndTime, "newEndTime", endtime)
		}
		return true, mdb.forceEndTime(ctx, &res[0], endtime)
	}
	return false, nil
}

func (mdb *metaDB) getPathHistory(ctx context.Context, user, parent, name string, start_ts int64) ([]FileMetadata, error) {
	log.Debugw("getPathHistory", "user", user, "parent", parent, "name", name, "start", start_ts)
	col := mdb.cMetaFile
	cursor, err := col.Aggregate(ctx, []bson.M{
		{
			"$match": bson.M{
				"user": user,
				"parent": parent,
				"name": name,
				"$or": []bson.M{
					{"start_ts": bson.M{"$gte": start_ts}},
					{"end_ts": bson.M{"$exists": false}},
					{"cleanup_required": true},
				},
			},
		},
		{
			"$sort": bson.M{"start_ts": 1, },
		},
	})
	if err != nil {
		log.Errorw("getPathHistory Aggregate", "user", user, "parent", parent, "name", name, "start", start_ts, "error", err)
		return nil, err
	}
	ret := make([]FileMetadata, 0)
	if err := cursor.All(ctx, &ret); err != nil {
		log.Errorw("getPathHistory Aggregate.All", "user", user, "parent", parent, "name", name, "start", start_ts, "error", err)
		return nil, err
	}
	return ret, nil
	
}


func (mdb *metaDB) listOfRequiredUpdate(ctx context.Context) ([]FileMetadata, error) {
	// returned value include the following fields: name, parent, user, start_ts
	log.Debugw("listOfRequiredUpdate")
	col := mdb.cMetaFile
	cursor, err := col.Aggregate(ctx, []bson.M{
		{
			"$match": bson.M{
				"cleanup_required": true,
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{"parent": "$parent", "name": "$name"},
				"user": bson.M{"$first": "$user"},
				"start_ts": bson.M{"$min": "$start_ts"},
			},
		},
		{
			"$addFields": bson.M{
				"name": "$_id.name",
				"parent": "$_id.parent",
			},
		},
		{
			"$unset": bson.A{"_id"},
		},
		{
			"$sort": bson.M{"parent": -1, "name": -1},
		},
		{
			"$limit": maxPathByUpdate,
		},
	})
	if err != nil {
		log.Debugw("listOfRequiredUpdate Aggregate", "error", err)
		return nil, err
	}
	ret := make([]FileMetadata, 0)
	if err := cursor.All(ctx, &ret); err != nil {
		log.Debugw("listOfRequiredUpdate Aggregate.All", "error", err)
		return nil, err
	}

	return ret, nil
}

func (mdb *metaDB) LaunchCleanupLoop(e Explorer) error {
	go func() {
		for {
			log.Debugw("Waiting cleanup requirement...")
			mdb.waitCleanupRequired()
			mdb.runCleanup(e)
		}
	}()
	return nil
}

func (mdb *metaDB) ListFiles(user string, path string) ([]DirectoryItem, error) {
	return nil, errors.New("api not implemented")
}
func (mdb *metaDB) GetFileInfo(user string, parent string, name string) (*FileMetadata, error) {
	log.Debugw("GetFileInfo", "user", user, "parent", parent, "name", name)
	ctx := context.TODO()
	col := mdb.cMetaFile
	filter := bson.M{"user": user, "parent": parent, "name": name}
	opts := options.Find()

	opts.SetSort(bson.D{{Key: "start_ts", Value: -1}})
	opts.SetLimit(1)

	cursor, err := col.Find(ctx, filter, opts)
	if err != nil {
		log.Errorw("GetFileInfo Find()", "user", user, "parent", parent, "name", name, "error", err)
		return nil, err
	}
	res := make([]FileMetadata, 1)
	if err := cursor.All(ctx, &res); err != nil {
		log.Errorw("GetFileInfo Find.All()", "user", user, "parent", parent, "name", name, "error", err)
		return nil, err
	}
	if len(res) < 1 {
		return nil, nil
	}
	return &res[0], nil
}

/*
type MetadataDBInternal interface {
	MetadataDB

	ListFiles(user string, path string) ([]DirectoryItem, error)
	GetFileInfo(user string, parent string, name string) (*FileMetadata, error)
}
*/
