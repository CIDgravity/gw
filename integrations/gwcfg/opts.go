package main

import (
	"flag"
	"time"
)

type Opts struct {
	envFile       string
	cidgravityUrl string
	cidgravitySvc string
	faucetUrl     string
	lotusGateway  string

	walletTimeout  time.Duration
	walletLocation string
}

func loadOpts() Opts {
	var opts Opts
	flag.StringVar(&opts.envFile, "f", "settings.env", "path to environment file")
	flag.StringVar(&opts.cidgravityUrl, "cidgravity", "https://api.cidgravity.com", "CIDGravity API URL")
	flag.StringVar(&opts.cidgravitySvc, "cidgravity-service", "https://service.cidgravity.com", "CIDGravity service URL for authenticated onboarding-policy setup")
	flag.StringVar(&opts.faucetUrl, "faucet", "https://faucet.cidgravity.com", "Faucet URL")
	flag.StringVar(&opts.lotusGateway, "lotus", "https://api.chain.love/rpc/v1", "Lotus Gateway API URL (ws or http)")
	flag.DurationVar(&opts.walletTimeout, "wallet-timeout", 5*60*time.Second, "Timeout for waiting for wallet to be visible on chain")
	flag.StringVar(&opts.walletLocation, "wallet", "default", "Wallet location (path or 'default')")
	flag.Parse()
	return opts
}
