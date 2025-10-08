package test

import "github.com/testcontainers/testcontainers-go"

func WithNetworks(networks ...string) testcontainers.CustomizeRequestOption {

	return func(req *testcontainers.GenericContainerRequest) error {
		req.Networks = networks
		return nil
	}
}
