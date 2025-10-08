package rbdeal

import (
	"context"
	"io"

	"github.com/CIDgravity/filecoin-gateway/iface"
)

type meteredExternalOffloader struct {
	ExternalOffloader
	metrics *ExternalStorageModuleMetrics
}

func (m *meteredExternalOffloader) EnsureExternalPush(gid iface.GroupKey, src CarSource) error {
	err := m.ExternalOffloader.EnsureExternalPush(gid, src)
	if err != nil {
		m.metrics.uploadErr.Inc()
	}
	return err
}

func (m *meteredExternalOffloader) CleanExternal(gid iface.GroupKey, lpath string) error {
	m.metrics.deleteReqs.Inc()
	err := m.ExternalOffloader.CleanExternal(gid, lpath)
	if err != nil {
		m.metrics.deleteErr.Inc()
	}
	return err
}

func (m *meteredExternalOffloader) ReadCar(ctx context.Context, group iface.GroupKey, path string, off int64, size int64) (io.ReadCloser, error) {
	m.metrics.readReqs.Inc()
	res, err := m.ExternalOffloader.ReadCar(ctx, group, path, off, size)
	if err != nil {
		m.metrics.readErr.Inc()
	}
	return res, err
}

func (m *meteredExternalOffloader) ReadCarFile(ctx context.Context, group iface.GroupKey) (io.ReadSeekCloser, error) {
	m.metrics.readReqs.Inc()
	res, err := m.ExternalOffloader.ReadCarFile(ctx, group)
	if err != nil {
		m.metrics.readErr.Inc()
	}
	return res, err
}
