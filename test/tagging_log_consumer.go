package test

import (
	"fmt"

	"github.com/testcontainers/testcontainers-go"
)

type taggingLogConsumer struct {
	ctName string
}

func (tc *taggingLogConsumer) Accept(l testcontainers.Log) {
	fmt.Printf("%s: %s", tc.ctName, string(l.Content))
}
