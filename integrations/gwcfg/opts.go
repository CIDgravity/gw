package main

import (
	"flag"
	"time"
)

type Opts struct {
	envFile       string
	cidgravityUrl string
	faucetUrl     string

	walletTimeout  time.Duration
	walletLocation string
}

func loadOpts() Opts {
	var opts Opts
	flag.StringVar(&opts.envFile, "f", "settings.env", "path to environment file")
	flag.StringVar(&opts.cidgravityUrl, "cidgravity", "https://app.cidgravity.com", "CIDGravity API URL")
	flag.StringVar(&opts.faucetUrl, "faucet", "http://localhost:8080/fil", "Faucet URL")
	flag.DurationVar(&opts.walletTimeout, "wallet-timeout", 5*60*time.Second, "Timeout for waiting for wallet to be visible on chain")
	flag.StringVar(&opts.walletLocation, "wallet", "default", "Wallet location")
	flag.Parse()
	return opts
}
