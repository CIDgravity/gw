package sqldb

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/yugabytedb"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/lib/pq"
)

//go:embed migrations
var migrationsfs embed.FS

type YugabyteDB struct {
	*sql.DB
}

const dbStartupMaxAttempts = 30

func retryDBStartup(name string, fn func() error) error {
	backoff := 2 * time.Second
	var err error

	for attempt := 1; attempt <= dbStartupMaxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if attempt == dbStartupMaxAttempts {
			break
		}
		time.Sleep(backoff)
		if backoff < 15*time.Second {
			backoff *= 2
			if backoff > 15*time.Second {
				backoff = 15 * time.Second
			}
		}
	}

	return fmt.Errorf("%s after %d attempts: %w", name, dbStartupMaxAttempts, err)
}

func NewYugabyteDB(config configuration.YugabyteSqlConfig) (*YugabyteDB, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable", config.User, config.Pass, config.Host, config.Port, config.Db))
	if err != nil {
		return nil, fmt.Errorf("open yugabyte sql db: %w", err)
	}

	// Configure connection pool settings from config (with sensible defaults)
	maxOpenConns := config.MaxOpenConns
	if maxOpenConns <= 0 {
		maxOpenConns = 100
	}
	maxIdleConns := config.MaxIdleConns
	if maxIdleConns <= 0 {
		maxIdleConns = 25
	}
	connMaxLifetime := time.Duration(config.ConnMaxLifetimeMins) * time.Minute
	if connMaxLifetime <= 0 {
		connMaxLifetime = 30 * time.Minute
	}
	connMaxIdleTime := time.Duration(config.ConnMaxIdleTimeMins) * time.Minute
	if connMaxIdleTime <= 0 {
		connMaxIdleTime = 5 * time.Minute
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)

	yugabyte := &YugabyteDB{
		db,
	}
	err = retryDBStartup("initialize yugabyte sql", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("ping yugabyte sql: %w", err)
		}
		return yugabyte.runMigrations()
	})
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
