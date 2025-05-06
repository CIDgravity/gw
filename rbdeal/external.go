package rbdeal

import (
	"context"
	iface "github.com/lotus-web3/ribs"
	"golang.org/x/xerrors"
	"io"
)

type CarSource func(context.Context, iface.GroupKey, func(int64), io.Writer) error

type ExternalOffloader interface {
	maybeInitExternal(r *ribs) (bool, error)
	GetModuleName() string
	EnsureExternalPush(gid iface.GroupKey, src CarSource) error
	GetGroupExternalURL(gid iface.GroupKey, lpath string) (*string, error)
	CleanExternal(gid iface.GroupKey, lpath string) error
	ReadCar(ctx context.Context, group iface.GroupKey, path string, off int64, size int64) (io.ReadCloser, error)
	ReadCarFile(ctx context.Context, group iface.GroupKey) (io.ReadSeekCloser, error)
}

func (r *ribs) maybeInitExternal() error {
	modules := []ExternalOffloader{
		&S3OffloadInfo{},
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

	r.RBS.StagingStorage().InstallStagingProvider(&ribsStagingProvider{r: r})

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
		// already pushed
		return nil
	}

	if r.externalOffloader == nil {
		return nil
	}

	return (r.externalOffloader).EnsureExternalPush(gid, r.RBS.Storage().ReadCar)
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

type ribsStagingProvider struct {
	r *ribs
}

// HasCar implements ribs.StagingStorageProvider.
func (r *ribsStagingProvider) HasCar(ctx context.Context, group iface.GroupKey) (bool, error) {
	module, _, err := r.r.db.GetExternalPath(group)
	if err != nil {
		return false, xerrors.Errorf("XYZ: External: fail to get ext path: %w", err)
	}
	return module != nil, nil
}

// ReadCar implements ribs.StagingStorageProvider.
func (r *ribsStagingProvider) ReadCar(ctx context.Context, group iface.GroupKey, off int64, size int64) (io.ReadCloser, error) {
	module, path, err := r.r.db.GetExternalPath(group)
	if err != nil {
		return nil, xerrors.Errorf("XYZ: External: fail to get ext path: %w", err)
	}

	if module == nil {
		return nil, xerrors.Errorf("XYZ: External: group %d has no external path", group)
	}

	if path == nil {
		return nil, xerrors.Errorf("XYZ: External: group %d has no external path", group)
	}

	if r.r.externalOffloader == nil {
		return nil, xerrors.Errorf("XYZ: External: offloaded to %s, but no module loaded", *module)
	}

	if lmod := r.r.externalOffloader.GetModuleName(); lmod != *module {
		return nil, xerrors.Errorf("XYZ: External: offloaded to %s, but %s module loaded", *module, lmod)
	}

	return r.r.externalOffloader.ReadCar(ctx, group, *path, off, size)
}

// Upload implements ribs.StagingStorageProvider.
func (r *ribsStagingProvider) Upload(ctx context.Context, group iface.GroupKey, size int64, src func(writer io.Writer) error) error {
	return (r.r.externalOffloader).EnsureExternalPush(group, func(ctx context.Context, gk iface.GroupKey, f func(int64), w io.Writer) error {
		return src(w)
	})
}
