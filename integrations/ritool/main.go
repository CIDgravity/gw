package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:  "ribs",
		Usage: "ribs repository manipulation commands",

		Commands: []*cli.Command{
			headCmd,
			carlogCmd,
			idxLevelCmd,
			ldbcidCmd,
			groupCmd,
			claimsExtendCmd,
			loadtestCmd,
			migrateCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
