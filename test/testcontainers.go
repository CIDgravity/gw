package test

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
)

const yugabyteImage = "yugabytedb/yugabyte:2.25.2.0-b359"
const YugabytePort = 9043

type Harness struct {
	yugabyte *testcontainers.Container
}

func NewHarness() *Harness {
	harness := &Harness{}
	harness.startYugabyte()
	return harness
}

func (th *Harness) Stop() {
	th.stopYugabyte()
}

func (th *Harness) startYugabyte() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        yugabyteImage,
		ExposedPorts: []string{"9043:9042"},
		WaitingFor:   wait.ForLog("Data placement constraint successfully verified"),
		Cmd:          []string{"bin/yugabyted", "start", "--background=false"},
	}
	yugabyte, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("failed to start yugabyte container: %s", err)
	}
	th.yugabyte = &yugabyte
}

func (th *Harness) stopYugabyte() {
	err := testcontainers.TerminateContainer(*th.yugabyte)
	if err != nil {
		log.Fatalf("failed to stop yugabyte container: %s", err)
	}
}
