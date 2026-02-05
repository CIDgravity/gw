package main

import (
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
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err)
	}
}
