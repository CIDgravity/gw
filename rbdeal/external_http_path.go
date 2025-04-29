package rbdeal

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/google/uuid"
	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/configuration"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
)

type LocalWebInfo struct {
	name string
	path string
	url  string
	r    *ribs
}

const EXTERNAL_LOCALWEB = "local-web"

func getLocalWebPath() (string, error) {
	cfg := configuration.GetConfig()
	p := cfg.External.Localweb.Path

	if p == "" {
		rdir, err := homedir.Expand(cfg.Ribs.DataDir)
		if err != nil {
			return "", xerrors.Errorf("XYZ: LocalWeb: failed to expand data dir: %w", err)
		}
		p = path.Join(rdir, "cardata")
	}

	return p, nil
}

func (lwi *LocalWebInfo) maybeInitExternal(r *ribs) (bool, error) {
	var err error
	cfg := configuration.GetConfig()

	lwi.name = EXTERNAL_LOCALWEB
	lwi.path, err = getLocalWebPath()
	if err != nil {
		return false, xerrors.Errorf("XYZ: LocalWeb: failed to get local web path: %w", err)
	}
	lwi.url = cfg.External.Localweb.Url

	lwi.r = r
	if lwi.url == "" {
		if lwi.path == "" && cfg.External.Localweb.BuiltinServer {
			return false, xerrors.Errorf("XYZ: LocalWeb: todo: path is not set, builtin server is enabled but url is not set: '%s' & '%s'", lwi.path, lwi.url)
		}
		return false, nil
	}

	return true, nil
}

func (lwi *LocalWebInfo) GetModuleName() string {
	return lwi.name
}
func setSizeNoop(_ int64) {}

func (lwi *LocalWebInfo) EnsureExternalPush(gid iface.GroupKey, src CarSource) error {
	// generate random uuid
	fname := fmt.Sprintf("%d-%s.car", gid, uuid.New().String())
	// ensure we don't have a file by that name on target
	target, err := getLocalWebPath()
	if err != nil {
		return xerrors.Errorf("XYZ: LocalWeb: failed to get local web path: %w", err)
	}
	target = path.Join(target, fname)
	if _, err := os.Stat(target); err == nil {
		return xerrors.Errorf("XYZ: LocalWeb: Random generated UUID for localweb cach already exists: %s", fname)
	}
	// Write the carfile there
	carfile, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return xerrors.Errorf("XYZ: opening carfile: %w", err)
	}

	ctx := context.TODO()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = src(ctx, gid, setSizeNoop, carfile)
	if err != nil {
		return xerrors.Errorf("XYZ: failed to dump carfile: %w", err)
	}
	if err := carfile.Sync(); err != nil {
		return xerrors.Errorf("XYZ: failed to sync carfile: %w", err)
	}
	if err := carfile.Close(); err != nil {
		return xerrors.Errorf("XYZ: failed to close carfile: %w", err)
	}

	// store file name in db
	err = lwi.r.db.AddExternalPath(gid, lwi.name, fname)
	if err != nil {
		return xerrors.Errorf("XYZ: LocalWeb: Failed to store localpath: %w", err)
	}

	return nil
}
func (lwi *LocalWebInfo) GetGroupExternalURL(gid iface.GroupKey, lpath string) (*string, error) {
	url := fmt.Sprintf("%s/%s", lwi.url, lpath)
	return &url, nil
}

func (lwi *LocalWebInfo) CleanExternal(gid iface.GroupKey, lpath string) error {
	// remove file in target
	target, err := getLocalWebPath()
	if err != nil {
		return xerrors.Errorf("XYZ: LocalWeb: failed to get local web path: %w", err)
	}
	target = path.Join(target, lpath)
	if err := os.Remove(target); err != nil {
		return xerrors.Errorf("XYZ: External: failed to remove carfile %s for group %d: %w", target, gid, err)
	}
	// remove entry from db
	if err := lwi.r.db.DropExternalPath(gid); err != nil {
		return xerrors.Errorf("XYZ: External: failed to remove external path from db for group %d: %w", gid, err)
	}
	return nil
}

func (lwi *LocalWebInfo) ReadCar(ctx context.Context, group iface.GroupKey, pathStr string, off int64, size int64) (io.ReadCloser, error) {
	target, err := getLocalWebPath()
	if err != nil {
		return nil, xerrors.Errorf("XYZ: LocalWeb: failed to get local web path: %w", err)
	}
	target = path.Join(target, pathStr)
	file, err := os.Open(target)
	if err != nil {
		return nil, xerrors.Errorf("XYZ: LocalWeb: failed to open carfile %s for group %d: %w", target, group, err)
	}

	// Seek to the offset
	_, err = file.Seek(off, io.SeekStart)
	if err != nil {
		file.Close()
		return nil, xerrors.Errorf("XYZ: LocalWeb: failed to seek carfile %s for group %d: %w", target, group, err)
	}

	// Wrap the file in an io.LimitedReader to restrict to 'size' bytes
	lr := io.LimitReader(file, size)

	// Return a ReadCloser that closes the underlying file when closed
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: lr,
		Closer: file,
	}, nil
}
