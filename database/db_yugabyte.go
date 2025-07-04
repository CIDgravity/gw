package database

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/yugabytedb"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/lib/pq"
	"github.com/lotus-web3/ribs/configuration"
)

//go:embed migrations
var migrationsfs embed.FS

type YugabyteDB struct {
	*sql.DB
}

func NewYugabyteDB(config configuration.YugabyteSqlConfig) (*YugabyteDB, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable", config.User, config.Pass, config.Host, config.Port, config.Db))
	if err != nil {
		return nil, fmt.Errorf("open yugabyte sql db: %w", err)
	}

	yugabyte := &YugabyteDB{
		db,
	}
	err = yugabyte.runMigrations()
	if err != nil {
		return nil, err
	}

	return yugabyte, nil
}

func (y *YugabyteDB) Start() error {
	return nil
}

func (y *YugabyteDB) runMigrations() error {
	migrations, err := iofs.New(migrationsfs, "migrations")
	if err != nil {
		return fmt.Errorf("create migrations source: %w", err)
	}

	driver, err := yugabytedb.WithInstance(y.DB, &yugabytedb.Config{})
	if err != nil {
		return fmt.Errorf("create postgres migrations driver: %w", err)
	}

	mig, err := migrate.NewWithInstance("iofs", migrations, "yugabytedb", driver)
	if err != nil {
		return err
	}

	if err := mig.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}
