package test

import "context"

type FgwHarness struct {
	ch *containerHarness
}

func NewFgwHarness() *FgwHarness {
	ah := &FgwHarness{
		ch: newContainerHarness(),
	}
	ah.ch.startYugabyte()
	ah.ch.startFilecoinGateway()
	return ah
}

func (ah *FgwHarness) Stop() {
	ah.ch.stop()
}

func (ah *FgwHarness) GetS3Endpoint() string {
	edpoint, err := (*ah.ch.gw).PortEndpoint(context.Background(), "8078", "http")
	if err != nil {
		log.Fatalf("failed to load s3 endpoint: %s", err)
	}
	return edpoint
}
