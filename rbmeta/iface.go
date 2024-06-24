package rbmeta

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type FileMetadata struct {
	// All as ref, so they are optionnal in usage
	Id              *primitive.ObjectID `bson:"_id"`
	User            *string             `bson:"user"`
	ParentPath      *string             `bson:"parent"`
	Filename        *string             `bson:"name"`
	Cid             *string             `bson:"cid"`
	File            *bool               `bson:"file"`
	Size            *uint64             `bson:"size"`
	StartTime       *int64              `bson:"start_ts"`
	EndTime         *int64              `bson:"end_ts"`
	CleanupRequired *bool               `bson:"cleanup_required"`
	Groups          *[]int64            `bson:"groups"`
}
type ChildInfo struct {
	Cid  string `bson:"cid"`
	Size uint64 `bson:"size"`
}
type ChildMetadata struct {
	Id     *primitive.ObjectID  `bson:"_id"`
	Cid    *string              `bson:"cid"`
	Childs map[string]ChildInfo `bson:"childs"`
	Groups []int64              `bson:"groups"`
}
type DirectoryItem struct {
	Filename string
	Cid      string
}

type Explorer interface {
	ListChilds(cid string) (map[string]ChildInfo, error)
	ListGroups(string) ([]int64, error)
}

type MetadataDB interface {
	/* Functions that should be fast, done directly as we write */
	WriteFile(filepath string, c string, size uint64) error
	WriteDir(filepath string, c string) error
	Remove(filepath string) error
	Rename(oldName, newName string) error
	/* Cleanup functions */
	LaunchCleanupLoop(Explorer) error
	LaunchServer() error

	/* Query Functions */
	ListFiles(user string, path string) ([]DirectoryItem, error)
	GetFileInfo(user string, parent string, name string) (*FileMetadata, error)
}
