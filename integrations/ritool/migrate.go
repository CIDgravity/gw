package main

import (
	"fmt"
	"sort"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/database/cqldb"
	"github.com/CIDgravity/filecoin-gateway/database/sqldb"
	"github.com/CIDgravity/filecoin-gateway/migrate"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var migrateCmd = &cli.Command{
	Name:  "migrate",
	Usage: "migrate an old (pebble+sqlite) .ribsdata to the Yugabyte-backed format",
	Description: `Reads an old .ribsdata (treated as read-only) and produces a migrated
.ribsdata directory plus populated Yugabyte SQL/CQL databases.

The target databases are configured with the usual RIBS_YUGABYTE_SQL_* and
RIBS_YUGABYTE_CQL_* environment variables; schema migrations are applied
automatically on connect (the CQL keyspace must already exist).

Progress is checkpointed in the destination directory: re-running the same
command resumes an interrupted migration. Without the state file, the
migrated SQL tables are cleared and recopied.`,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "source",
			Usage:    "old .ribsdata directory (never written to)",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dest",
			Usage:    "output .ribsdata directory",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "copy",
			Usage: "copy file data instead of hardlinking",
		},
		&cli.IntFlag{
			Name:  "workers",
			Usage: "concurrent CQL insert workers",
			Value: 16,
		},
		&cli.IntFlag{
			Name:  "batch-size",
			Usage: "index entries per CQL batch",
			Value: 1024,
		},
		&cli.Int64Flag{
			Name:  "checkpoint-every",
			Usage: "index entries between resume checkpoints",
			Value: 1 << 20,
		},
		&cli.IntFlag{
			Name:  "verify-sample",
			Usage: "verify every Nth block index entry after migrating (1 = all, 0 = skip index verification)",
			Value: 128,
		},
		&cli.BoolFlag{
			Name:  "verify-only",
			Usage: "only verify a previously completed migration",
		},
		&cli.BoolFlag{
			Name:  "skip-verify",
			Usage: "skip verification entirely",
		},
	},
	Action: func(cctx *cli.Context) error {
		_ = logging.SetLogLevel("gw/migrate", "INFO")

		if err := configuration.LoadConfig(); err != nil {
			return xerrors.Errorf("loading configuration: %w", err)
		}
		cfg := configuration.GetConfig()

		sqlDB, err := sqldb.NewYugabyteDB(cfg.YugabyteSql)
		if err != nil {
			return xerrors.Errorf("connecting to yugabyte sql: %w", err)
		}
		cqlDB, err := cqldb.NewYugabyteCqlDb(cfg.YugabyteCql)
		if err != nil {
			return xerrors.Errorf("connecting to yugabyte cql: %w", err)
		}

		opts := migrate.Options{
			SourceDir:       cctx.String("source"),
			DestDir:         cctx.String("dest"),
			SQL:             sqlDB,
			CQL:             cqlDB,
			CopyFiles:       cctx.Bool("copy"),
			Workers:         cctx.Int("workers"),
			BatchSize:       cctx.Int("batch-size"),
			CheckpointEvery: cctx.Int64("checkpoint-every"),
		}

		if !cctx.Bool("verify-only") {
			summary, err := migrate.Run(cctx.Context, opts)
			if err != nil {
				return err
			}
			printSummary(summary)
		}

		if cctx.Bool("skip-verify") {
			return nil
		}

		fmt.Printf("verifying migration (sampling every %d blocks; --skip-verify to skip)...\n", cctx.Int("verify-sample"))

		report, err := migrate.Verify(cctx.Context, opts, migrate.VerifyOptions{
			SampleEvery: cctx.Int("verify-sample"),
		})
		if err != nil {
			return xerrors.Errorf("verification failed to run: %w", err)
		}
		printVerifyReport(report)

		if !report.OK() {
			return xerrors.Errorf("verification found %d problems", len(report.Problems))
		}
		return nil
	},
}

func printSummary(s *migrate.Summary) {
	fmt.Println("=== migration summary ===")
	if s.Resumed {
		fmt.Println("resumed from previous state (per-phase counts cover this run only)")
	}

	tables := make([]string, 0, len(s.SQLRows))
	for t := range s.SQLRows {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, t := range tables {
		fmt.Printf("  sql %-16s %d rows\n", t, s.SQLRows[t])
	}

	fmt.Printf("  tree: %d files (%d linked, %d copied, %d bytes)\n", s.Tree.Files, s.Tree.Linked, s.Tree.Copied, s.Tree.Bytes)
	fmt.Printf("  index: %d multihashes, %d (multihash, group) pairs", s.Index.Multihashes, s.Index.Pairs)
	if s.Index.MissingSize > 0 {
		fmt.Printf(" (%d without size entries)", s.Index.MissingSize)
	}
	fmt.Println()
	fmt.Printf("  took: %s\n", s.Duration)
}

func printVerifyReport(r *migrate.VerifyReport) {
	fmt.Println("=== verification report ===")
	tables := make([]string, 0, len(r.TableCounts))
	for t := range r.TableCounts {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, t := range tables {
		c := r.TableCounts[t]
		status := "ok"
		if c[0] != c[1] {
			status = "MISMATCH"
		}
		fmt.Printf("  sql %-16s source=%-8d dest=%-8d %s\n", t, c[0], c[1], status)
	}
	fmt.Printf("  tree: %d files checked\n", r.TreeFiles)
	fmt.Printf("  index: %d multihashes (%d pairs) scanned, %d checked against CQL\n", r.IndexMultihashes, r.IndexPairs, r.IndexChecked)

	if r.OK() {
		fmt.Println("  result: OK")
		return
	}
	fmt.Printf("  result: %d PROBLEMS\n", len(r.Problems))
	for _, p := range r.Problems {
		fmt.Printf("    - %s\n", p)
	}
}
