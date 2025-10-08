package main

import (
	"fmt"
	"os"

	"github.com/CIDgravity/filecoin-gateway/bsst"
	"github.com/CIDgravity/filecoin-gateway/carlog"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteMapEncodersToFile("./carlog/cbor_gen.go", "carlog", carlog.Head{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = gen.WriteMapEncodersToFile("./bsst/cbor_gen.go", "bsst", bsst.BSSTHeader{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
