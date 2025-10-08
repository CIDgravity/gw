package cqldb

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	gocqlnative "github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/cassandra"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	logging "github.com/ipfs/go-log/v2"
	"github.com/yugabyte/gocql"
)

var log = logging.Logger("gw/db/cql")

//go:embed migrations
var migrationsfs embed.FS

type yugabyteCqlDb struct {
	*gocql.Session
	cluster *gocql.ClusterConfig
	ctx     context.Context
}

func NewYugabyteCqlDb(config configuration.YugabyteCqlConfig) (Database, error) {
	err := runMigrations(config)
	if err != nil {
		return nil, err
	}

	hosts := strings.Split(config.Hosts, ",")
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = config.Port
	cluster.Keyspace = config.Keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = time.Duration(config.Timeout) * time.Second
	cluster.ConnectTimeout = time.Duration(config.ConnectTimeout) * time.Second
	cluster.SocketKeepalive = time.Duration(config.SocketKeepalive) * time.Second

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: config.User,
		Password: config.Pass,
	}

	if config.ForceHosts {
		cluster.AddressTranslator = gocql.AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
			log.Infof("Translating from %s", addr)
			return net.ParseIP(hosts[0]).To4(), port
		})
	}
	session, err := cluster.CreateSession()

	if err != nil {
		return nil, fmt.Errorf("create cql session: %w", err)
	}

	db := &yugabyteCqlDb{
		Session: session,
		cluster: cluster,
		ctx:     context.Background(),
	}
	return db, nil
}

func runMigrations(config configuration.YugabyteCqlConfig) error {
	hosts := strings.Split(config.Hosts, ",")
	cluster := gocqlnative.NewCluster(hosts...)
	cluster.Port = config.Port

	cluster.Timeout = time.Duration(config.Timeout) * time.Second
	cluster.ConnectTimeout = time.Duration(config.ConnectTimeout) * time.Second
	cluster.SocketKeepalive = time.Duration(config.SocketKeepalive) * time.Second

	cluster.Authenticator = gocqlnative.PasswordAuthenticator{
		Username: config.User,
		Password: config.Pass,
	}
	cluster.Keyspace = config.Keyspace
	session, err := cluster.CreateSession()

	if err != nil {
		return fmt.Errorf("create cql migrate session: %w", err)
	}

	migrations, err := iofs.New(migrationsfs, "migrations")
	if err != nil {
		return fmt.Errorf("create cql migrations source: %w", err)
	}

	driver, err := cassandra.WithInstance(session, &cassandra.Config{
		KeyspaceName: config.Keyspace,
	})
	if err != nil {
		return fmt.Errorf("create cql migrations driver: %w", err)
	}

	mig, err := migrate.NewWithInstance("iofs", migrations, "cassandra", driver)
	if err != nil {
		return err
	}

	if err := mig.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("run cql migrations: %w", err)
	}
	return nil
}
