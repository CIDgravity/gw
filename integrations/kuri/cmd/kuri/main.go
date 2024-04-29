package main

import (
	"os"
	"fmt"

	"github.com/ipfs/kubo/cmd/ipfs/kubo"
	"github.com/ipfs/kubo/plugin/loader"

	"github.com/lotus-web3/ribs/ributil"
	"github.com/lotus-web3/ribs/configuration"
	kuboribs "github.com/lotus_web3/ribs/integrations/kuri/ribsplugin"
)

func main() {
	os.Exit(mainRet())
}

func mainRet() (exitCode int) {
	mw := ributil.MemoryWatchdog()
	defer mw()

	if err := configuration.LoadConfig(); err != nil {
		fmt.Fprintln(os.Stderr, "Configuration load failed: %w\n", err)
	}
	return kubo.Start(kubo.BuildEnv(func(loader *loader.PluginLoader) error {
		return loader.Load(kuboribs.Plugin)
	}))
}
