package main

import (
	"encoding/binary"
	"flag"
	"github.com/cockroachdb/pebble"
	"github.com/multiformats/go-multihash"
	"github.com/yugabyte/gocql"
	"log"
)

func main() {
	indexPath := flag.String("index", "", "Path to the pebble index")
	yugabyteHost := flag.String("host", "localhost", "Yugabyte host")
	port := flag.Int("port", 9042, "Yugabyte port")
	keyspace := flag.String("keyspace", "auroragw", "Yugabyte keyspace")
	flag.Parse()

	if *indexPath == "" {
		log.Fatal("ERROR: Missing required flags: --index")
	}

	cluster := gocql.NewCluster(*yugabyteHost)
	cluster.Port = *port
	cluster.Keyspace = *keyspace
	cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("ERROR: Failed to create session: %s", err)
	}
	defer session.Close()

	statement := `INSERT INTO MultihashToGroup (Multihash, Group, Size) VALUES (?, ?, ?)`

	db, err := pebble.Open(*indexPath, &pebble.Options{})
	if err != nil {
		log.Fatalf("ERROR: Failed to open Pebble DB at path %s: %s", *indexPath, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("WARNING: Failed to close Pebble DB: %s", err)
		}
	}()

	iter, err := db.NewIter(nil)
	if err != nil {
		panic(err)
	}
	defer iter.Close()
	iter.SetBounds([]byte("i:"), append([]byte("i:"), 0xff))

	counter := 1

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		groupBytes := key[len(key)-8:]
		group := binary.BigEndian.Uint64(groupBytes)
		mhBytes := key[2 : len(key)-8]
		mh, err := multihash.Cast(mhBytes)
		if err != nil {
			log.Fatalf("ERROR: Failed to cast multihash: %s", err)
		}
		log.Printf("Processing entry %d: %s", counter, mh)
		size := findSize(db, mh)

		err = session.Query(statement, mh, group, size).Exec()
		if err != nil {
			log.Fatalf("ERROR: Failed to execute query: %s", err)
		}
		counter++
	}
}

func findSize(db *pebble.DB, mh []byte) int32 {
	sizeKey := append([]byte("s:"), mh...)
	val, closer, err := db.Get(sizeKey)
	if err != nil {
		log.Fatalf("ERROR: Failed to get size: %s", err)
	}

	err = closer.Close()
	if err != nil {
		log.Fatalf("ERROR: Failed to close closer: %s", err)
	}

	return int32(binary.BigEndian.Uint32(val))
}
