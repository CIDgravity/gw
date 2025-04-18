package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/multiformats/go-multihash"
	"github.com/ipfs/go-cid"
)

// Struct for storing multihash and type
type MHInfo struct {
	MH   string `json:"mh"`
	Type string `json:"type"`
}

// Struct for storing CID version 1 and 0
type CIDDetails struct {
	V1   string  `json:"v1"`
	V0   *string `json:"v0,omitempty"` // Use pointer to handle null values
	Type string  `json:"type"`
}

// Main struct to hold all the data
type CIDInfo struct {
	Group uint64    `json:"group,omitempty"`
	Size uint32    `json:"size,omitempty"`
	Prefix string    `json:"prefix"`
	MH    MHInfo    `json:"mh"`
	CID   CIDDetails `json:"cid"`
}

func main() {
	// Standard flag parsing
	indexPath := flag.String("index", "", "Path to the pebble index")
	flag.Parse()

	// Set the log output to stderr
	log.SetOutput(os.Stderr)

	// Ensure required flags are provided
	if *indexPath == "" {
		log.Fatal("ERROR: Missing required flags: --index")
	}

	// Open the Pebble DB
	db, err := pebble.Open(*indexPath, &pebble.Options{})
	if err != nil {
		// Explicit error message and program halt if opening Pebble DB fails
		log.Fatalf("ERROR: Failed to open Pebble DB at path %s: %s", *indexPath, err)
	}

	// Ensure that the DB gets closed when done
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("WARNING: Failed to close Pebble DB: %s", err)
		}
	}()

	// Iterate through all keys
	iter := db.NewIter(nil)
	defer iter.Close() // Close the iterator once we're done

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()
        var mhBytes []byte
        var group uint64
        var size  uint32
        var prefix []byte

        // Extract data for i: entries
        if len(key) > 2 && key[0] == 'i' && key[1] == ':' {
            prefix = []byte("i:")
            groupBytes := key[len(key)-8:]
            group = binary.BigEndian.Uint64(groupBytes)
            mhBytes = key[len(prefix) : len(key)-8]

        // Extract data for s: entries
        } else if len(key) > 2 && key[0] == 's' && key[1] == ':' {
            prefix = []byte("s:")
            mhBytes = key[len(prefix):]
            sizeBytes := key[:4]
            size = binary.BigEndian.Uint32(sizeBytes)
            groupBytes := val[4:]
            if len(groupBytes) != 8 {
                log.Printf("WARNING: Expected 8 bytes for group, but got %d bytes for key: %x", len(groupBytes), key)
            } else {
                group = binary.BigEndian.Uint64(groupBytes)
            }
         }


         mh, err := multihash.Cast(mhBytes)
        if err != nil {
            log.Printf("WARNING: Failed to cast multihash for key %s, mhBytes: %x, error: %s", key, mhBytes, err)
            continue
        }

        mhinfo, err := multihash.Decode(mhBytes)
        if err != nil {
            log.Printf("WARNING: Failed to decode multihash: %s", err)
            continue
        }

		// Generate the CID v1
		v1 := cid.NewCidV1(cid.Raw, mh)

		// Generate CID v0 if the multihash is SHA2-256
		var v0 *string
		if mhinfo.Code == multihash.SHA2_256 {
			v0Cid := cid.NewCidV0(mh)
			v0Str := v0Cid.String()
			v0 = &v0Str
		}

		// Format and Print
		cidInfo := CIDInfo{
			Group: group,
            Prefix: string(prefix),
            Size: size,
			MH: MHInfo{
				MH:   mh.String(),
				Type: multihash.Codes[mhinfo.Code],
			},
			CID: CIDDetails{
				V1:   v1.String(),
				V0:   v0,
				Type: multihash.Codes[mhinfo.Code],
			},
		}
		jsonData, err := json.Marshal(cidInfo)
		if err != nil {
			log.Printf("WARNING: Failed to marshal JSON: %s", err)
			continue
		}
		fmt.Println(string(jsonData))
	}
}

