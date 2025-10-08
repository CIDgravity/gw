package test

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	logging "github.com/ipfs/go-log/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const yugabyteImage = "yugabytedb/yugabyte:2.25.2.0-b359"

var log = logging.Logger("gw/test")

type containerHarness struct {
	yugabyte *testcontainers.Container
	gw       *testcontainers.Container
	net      *testcontainers.DockerNetwork
}

func newContainerHarness() *containerHarness {
	net, err := network.New(context.Background())
	if err != nil {
		log.Fatalf("Failed to create a docker network %s", err)
	}

	harness := &containerHarness{
		net: net,
	}
	return harness
}

func (ch *containerHarness) stop() {
	if ch.yugabyte != nil {
		ch.terminateContainer(*ch.yugabyte)
	}
	if ch.gw != nil {
		ch.terminateContainer(*ch.gw)
	}
}

func (ch *containerHarness) terminateContainer(ct testcontainers.Container) {
	err := testcontainers.TerminateContainer(ct)
	if err != nil {
		log.Fatalf("Failed to terminate the container %s", err)
	}
}

func (ch *containerHarness) startYugabyte() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        yugabyteImage,
		ExposedPorts: []string{"9042", "5433"},
		WaitingFor: wait.ForAll(
			wait.ForExec([]string{"/bin/bash", "-c", "bin/ysqlsh -h `hostname -i` -U yugabyte -tAc 'select 1' -d yugabyte"}),
			wait.ForExec([]string{"/bin/bash", "-c", "bin/ycqlsh -k filecoingw_test -e 'SELECT cluster_name FROM system.local' `hostname -i`"}),
			wait.ForLog("Data placement constraint successfully verified"),
		).WithStartupTimeoutDefault(10 * time.Minute),
		Cmd:      []string{"bin/yugabyted", "start", "--background=false"},
		Networks: []string{ch.net.Name},
		Env: map[string]string{
			"YSQL_DB":       "filecoingw_test",
			"YCQL_KEYSPACE": "filecoingw_test",
		},
		ConfigModifier: func(config *container.Config) {
			config.Hostname = "yugabyte"
		},
	}
	yugabyte, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		log.Fatalf("failed to start yugabyte container: %s", err)
	}
	ch.yugabyte = &yugabyte
}

func (ch *containerHarness) startFilecoinGateway() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:        FindModuleRoot(),
			Dockerfile:     "Dockerfile",
			KeepImage:      true,
			BuildLogWriter: os.Stdout},
		ExposedPorts: []string{"8078", "2112"},
		WaitingFor:   wait.ForLog("Daemon is ready").WithStartupTimeout(2 * time.Minute),
		Networks:     []string{ch.net.Name},
		Cmd:          []string{"sh", "-c", "./kuri init && ./kuri daemon"},
		Env: map[string]string{
			"RIBS_YUGABYTE_CQL_HOSTS":       "yugabyte",
			"RIBS_YUGABYTE_CQL_KEYSPACE":    "filecoingw_test",
			"RIBS_YUGABYTE_SQL_HOST":        "yugabyte",
			"RIBS_YUGABYTE_SQL_DB":          "filecoingw_test",
			"RIBS_LOGLEVEL":                 "ribs:.*=debug,gw/.*=debug,ribs:rbdeal=info",
			"EXTERNAL_LOCALWEB_PATH":        "/data",
			"EXTERNAL_LOCALWEB_URL":         "http://127.0.0.1",
			"RIBS_S3API_AUTH_ENABLED":       "true",
			"RIBS_S3API_ROOT_ACCESS_KEY_ID": "test-access-key",
			"RIBS_S3API_ROOT_SECRET_KEY":    "test-secret-key",
		},
		ConfigModifier: func(config *container.Config) {
			config.Hostname = "fgw"
		},
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Consumers: []testcontainers.LogConsumer{&taggingLogConsumer{"fgw"}},
		},
	}
	gw, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("failed to start Aurora Gateway container: %s", err)
	}
	ch.gw = &gw
}

// Depending on how tests were run, the working directory might be inconsistent.
// This tries "..", "../..", ... until it finds go.mod.
func FindModuleRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)

		if parent == dir {
			log.Fatal("module root not found")
		}
		dir = parent
	}
}
