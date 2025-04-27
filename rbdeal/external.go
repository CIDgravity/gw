package rbdeal

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
	"os"
	"path"

	iface "github.com/lotus-web3/ribs"
	"github.com/lotus-web3/ribs/configuration"
)

type ExternalOffloader interface {
	maybeInitExternal(r *ribs) (bool, error)
	GetModuleName() string
	EnsureExternalPush(gid iface.GroupKey) error
	GetGroupExternalURL(gid iface.GroupKey, lpath string) (*string, error)
	CleanExternal(gid iface.GroupKey, lpath string) error
}

type LocalWebInfo struct {
	name string
	path string
	url  string
	r    *ribs
}

const EXTERNAL_LOCALWEB = "local-web"

func (r *ribs) maybeInitExternal() error {
	modules := []ExternalOffloader{
		&LocalWebInfo{},
	}
	for _, module := range modules {
		found, err := module.maybeInitExternal(r)
		if err != nil {
			return err
		}
		if found {
			log.Infow("XYZ: External module configured", "name", module.GetModuleName())
			r.externalOffloader = module
			return nil
		}
	}

	return nil
}
func (r *ribs) maybeEnsureEnsureExternalPush(gid iface.GroupKey) error {
	mname, err := r.db.NeedExternalModule()
	if err != nil {
		return xerrors.Errorf("XYZ: External: fail to check if external required: %w", err)
	}
	if mname != nil {
		if r.externalOffloader == nil {
			return nil
		}
		if lmod := r.externalOffloader.GetModuleName(); lmod != *mname {
			return xerrors.Errorf("XYZ: External: need %s module, %s loaded", mname, lmod)
		}
	}
	module, _, err := r.db.GetExternalPath(gid)
	if err != nil {
		return xerrors.Errorf("XYZ: External: fail to get ext path: %w", err)
	}
	if module != nil {
		return nil
	}

	if r.externalOffloader == nil {
		return nil
	}

	return (r.externalOffloader).EnsureExternalPush(gid)
}
func (r *ribs) maybeGetExternalURL(gid iface.GroupKey) (*string, error) {
	module, path, err := r.db.GetExternalPath(gid)
	if err != nil {
		return nil, xerrors.Errorf("XYZ: External: fail to get ext path: %w", err)
	}
	if module == nil {
		return nil, nil
	}
	if r.externalOffloader == nil {
		return nil, xerrors.Errorf("XYZ: External: offloaded to %s, but no module loaded", *module)
	}
	if lmod := r.externalOffloader.GetModuleName(); lmod != *module {
		return nil, xerrors.Errorf("XYZ: External: offloaded to %s, but %s module loaded", *module, lmod)
	}
	return r.externalOffloader.GetGroupExternalURL(gid, *path)
}
func (r *ribs) cleanupExternalOffload(gid iface.GroupKey) error {
	module, epath, err := r.db.GetExternalPath(gid)
	if err != nil {
		return xerrors.Errorf("XYZ: LocalWeb: Failed to get external path from db %w", err)
	}
	if epath == nil {
		// nothing to clean
		return nil
	}

	if r.externalOffloader == nil {
		return xerrors.Errorf("XYZ: External: offloaded to %s, but no module loaded", *module)
	}
	if lmod := r.externalOffloader.GetModuleName(); lmod != *module {
		return xerrors.Errorf("XYZ: External: offloaded to %s, but %s module loaded", *module, lmod)
	}
	return r.externalOffloader.CleanExternal(gid, *epath)
}

func getLocalWebPath() string {
	cfg := configuration.GetConfig()
	return cfg.External.Localweb.Path
}

func (lwi *LocalWebInfo) maybeInitExternal(r *ribs) (bool, error) {
	cfg := configuration.GetConfig()

	lwi.name = EXTERNAL_LOCALWEB
	lwi.path = getLocalWebPath()
	lwi.url = cfg.External.Localweb.Url

	lwi.r = r
	if lwi.path == "" || lwi.url == "" {
		if lwi.url == "" {
			return false, xerrors.Errorf("XYZ: LocalWeb: If localweb path is set, url must also be set: '%s' & '%s'", lwi.path, lwi.url)
		}
		if lwi.path == "" && !cfg.External.Localweb.BuiltinServer {
			return false, xerrors.Errorf("XYZ: LocalWeb: path must be set if builtin server is disabled: '%s' & '%s'", lwi.path, lwi.url)
		}
		if lwi.path != "" && cfg.External.Localweb.BuiltinServer {
			return false, xerrors.Errorf("XYZ: LocalWeb: todo: path is set but builtin server is enabled: '%s' & '%s'", lwi.path, lwi.url)
		}
		return false, nil
	}

	return true, nil
}

func (lwi *LocalWebInfo) GetModuleName() string {
	return lwi.name
}
func setSizeNoop(_ int64) {}

func (lwi *LocalWebInfo) EnsureExternalPush(gid iface.GroupKey) error {
	// generate random uuid
	guuid := uuid.New().String()
	// ensure we don't have a file by that name on target
	target := path.Join(getLocalWebPath(), guuid)
	if _, err := os.Stat(target); err == nil {
		return xerrors.Errorf("XYZ: LocalWeb: Random generated UUID for localweb cach already exists: %s", guuid)
	}
	// Write the carfile there
	carfile, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return xerrors.Errorf("XYZ: opening carfile: %w", err)
	}

	ctx := context.TODO()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = lwi.r.RBS.Storage().ReadCar(ctx, gid, setSizeNoop, carfile)
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
	err = lwi.r.db.AddExternalPath(gid, lwi.name, guuid)
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
	target := path.Join(getLocalWebPath(), lpath)
	if err := os.Remove(target); err != nil {
		return xerrors.Errorf("XYZ: External: failed to remove carfile %s for group %d: %w", target, gid, err)
	}
	// remove entry from db
	if err := lwi.r.db.DropExternalPath(gid); err != nil {
		return xerrors.Errorf("XYZ: External: failed to remove external path from db for group %d: %w", gid, err)
	}
	return nil
}
