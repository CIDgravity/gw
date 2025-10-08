package main

import (
	"fmt"
	"os"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/integrations/kuri/ribsplugin"
	"github.com/ipfs/kubo/cmd/ipfs/kubo"
	"github.com/ipfs/kubo/plugin/loader"

	"github.com/CIDgravity/filecoin-gateway/ributil"
)

func main() {
	os.Exit(mainRet())
}

func mainRet() (exitCode int) {
	mw := ributil.MemoryWatchdog()
	defer mw()

	if err := configuration.LoadConfig(); err != nil {
		fmt.Fprintln(os.Stderr, "Configuration load failed: %w", err)
	} else {
		fmt.Fprintln(os.Stderr, "Configuration loaded")
	}
	return kubo.Start(kubo.BuildEnv(func(loader *loader.PluginLoader) error {
		return loader.Load(kuboribs.Plugin)
	}))
}
