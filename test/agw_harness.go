package test

import (
	"context"

	"github.com/docker/go-connections/nat"
)

type FgwHarness struct {
	ch *containerHarness
}

func NewFgwHarness() *FgwHarness {
	return NewFgwHarnessWithEnv(nil)
}

// NewFgwHarnessWithEnv starts the gateway with extra/overridden environment
// variables and additional exposed ports.
func NewFgwHarnessWithEnv(extraEnv map[string]string, extraPorts ...string) *FgwHarness {
	ah := &FgwHarness{
		ch: newContainerHarness(),
	}
	ah.ch.startYugabyte()
	ah.ch.startFilecoinGateway(extraEnv, extraPorts...)
	return ah
}

func (ah *FgwHarness) Stop() {
	ah.ch.stop()
}

func (ah *FgwHarness) GetS3Endpoint() string {
	return ah.GetEndpoint("8078")
}

// GetEndpoint returns an http endpoint for an exposed container port.
func (ah *FgwHarness) GetEndpoint(port string) string {
	endpoint, err := (*ah.ch.gw).PortEndpoint(context.Background(), nat.Port(port), "http")
	if err != nil {
		log.Fatalf("failed to load endpoint for port %s: %s", port, err)
	}
	return endpoint
}
