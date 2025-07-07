package main

import (
	"fmt"
	"os"

	"github.com/ipfs/kubo/cmd/ipfs/kubo"
	"github.com/ipfs/kubo/plugin/loader"

	"github.com/aurorainfra/gw/configuration"
	kuboribs "github.com/aurorainfra/gw/integrations/kuri/ribsplugin"
	"github.com/aurorainfra/gw/ributil"
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
