package test

import (
	"context"
	"testing"

	"github.com/test-go/testify/require"
)

type YugabyteHarness struct {
	ch *containerHarness
}

func NewYugabyteHarness() *YugabyteHarness {
	yh := &YugabyteHarness{
		ch: newContainerHarness(),
	}
	yh.ch.startYugabyte()
	return yh
}

func (yh *YugabyteHarness) Stop() {
	yh.ch.stop()
}

func (yh *YugabyteHarness) GetYugabyteHost(t *testing.T) string {
	host, err := (*yh.ch.yugabyte).Host(context.Background())
	require.NoError(t, err)
	return host
}

func (yh *YugabyteHarness) GetYugabyteCqlPort(t *testing.T) int {
	port, err := (*yh.ch.yugabyte).MappedPort(context.Background(), "9042/tcp")
	require.NoError(t, err)
	return port.Int()
}
