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

	// an invalid configuration must not boot a half-configured node
	if err := configuration.LoadConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration load failed: %s\n", err)
		return 1
	}
	fmt.Fprintln(os.Stderr, "Configuration loaded")

	exitCode = kubo.Start(kubo.BuildEnv(func(loader *loader.PluginLoader) error {
		return loader.Load(kuboribs.Plugin)
	}))
	return exitCode
}
