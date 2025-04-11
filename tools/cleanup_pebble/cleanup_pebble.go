package main

import (
    "bufio"
    "bytes"
    "encoding/binary"
    "flag"
    "fmt"
    "log"
    "os"
    "strings"
    "context"
    "github.com/cockroachdb/pebble"
    "github.com/ipfs/go-cid"
    "github.com/multiformats/go-multihash"
    "github.com/lotus-web3/ribs"
    "github.com/lotus-web3/ribs/rbstor"
)

func main() {
    // Standard flag parsing
    indexPath := flag.String("index", "", "Path to the pebble index")
    goodCIDsFile := flag.String("good-cids", "", "File containing good CIDs, one per line, good CIDs are the only CIDs supposed to be inthe group, all other CIDs will be flagged and removed from pebble")
    groupKey := flag.Uint64("group", 0, "Group key to scan for stray references")
    outputFile := flag.String("output", "", "File to write stray CIDs to (optional, defaults to stdout)")
    reallyDoIt := flag.Bool("really-do-it", false, "If true, delete the entries, STOP any processes accessing pebble before using it")
    flag.Parse()

    log.SetFlags(0)
    // Ensure required flags are provided
    if *indexPath == "" || *goodCIDsFile == "" {
        log.Fatal("ERROR: Missing required flags: --index and --good-cids")
    }

    // Open the Pebble DB
    db, err := pebble.Open(*indexPath, &pebble.Options{})
    dbClosed := false
    if err != nil {
        // Explicit error message and program halt if opening Pebble DB fails
        log.Fatalf("ERROR: Failed to open Pebble DB at path %s: %s", *indexPath, err)
    }

    defer func() {
        if !dbClosed {
            if err := db.Close(); err != nil {
                log.Printf("WARNING: Failed to close Pebble DB: %s", err)
            }
        }
    }()

    // Load good CIDs into a map of multihashes
    goodMHs, err := loadGoodMultihashes(*goodCIDsFile)
    if err != nil {
        log.Fatalf("ERROR: Failed to load good CIDs: %s", err)
    }

    log.Printf("INFO: Good  CIDs found in %s: %d", *goodCIDsFile, len(goodMHs))

    // Find stray references
    strayMHs, err := findStrayReferences(db, goodMHs, *groupKey)
    if err != nil {
        log.Fatalf("ERROR: Failed to find stray references: %s", err)
    }

    // Close the DB here since it's no longer needed
    if err := db.Close(); err != nil {
        fmt.Fprintf(os.Stderr, "WARNING: Error closing the database: %s", err)
    }
    dbClosed = true

    log.Printf("INFO: Stray CIDS found in pebble for group(%d): %d", *groupKey ,len(strayMHs))
    if len(strayMHs) == 0 {
        log.Printf("INFO: Nothing to do")
    } else {


        // Output stray CIDs
        var output *os.File
        if *outputFile != "" {
            output, err = os.Create(*outputFile)
            if err != nil {
                log.Fatalf("ERROR: Failed to create output file: %s", err)
            }
            defer output.Close()
        } else {
            output = os.Stdout
        }

        for _, mh := range strayMHs {
            fmt.Fprintln(output, cid.NewCidV1(cid.Raw, mh).String())
        }

        // Open the pebble index
        index, err := rbstor.NewPebbleIndex(*indexPath)
        if err != nil {
            log.Fatalf("ERROR: Failed to open Pebble index at path %s: %s", *indexPath, err)
        }
        defer index.Close()

        // Count existing entries before deletion
        existingCount, err := countExistingEntries(index, strayMHs, ribs.GroupKey(*groupKey)) // Count the entries before deletion
        if err != nil {
            log.Fatalf("ERROR: Failed to count existing entries: %s", err)
        }
        log.Printf("INFO: Stray CIDS going to be removed from group(%d): %d", *groupKey, existingCount)

        if *reallyDoIt {
            ctx := context.Background()

            // Perform the deletion
            err = index.DropGroup(ctx, strayMHs, ribs.GroupKey(*groupKey))
            if err != nil {
                log.Fatalf("ERROR: Failed to delete stray references: %s", err)
            }

            // Sync to ensure data is persisted
            err = index.Sync(ctx)
            if err != nil {
                log.Fatalf("ERROR: Failed to sync index after deletion: %s", err)
            }

            // Count again after deletion
            existingCount, err = countExistingEntries(index, strayMHs, ribs.GroupKey(*groupKey))
            if err != nil {
                log.Fatalf("ERROR: Failed to count existing entries: %s", err)
            }
            log.Printf("INFO: Stray CIDs from still present in pebble for group(%d) after deletion (if not 0 investigate): %d", *groupKey, existingCount)
        } else {
            log.Println("INFO: No deletion performed. STOP CIDgravity-gw BEFORE RUNNING THIS COMMAND, and use --really-do-it to perform the deletion")
        }
    }
}

func loadGoodMultihashes(filename string) (map[string]struct{}, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    goodMHs := make(map[string]struct{})
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if line == "" {
            continue
        }

        // Parse CID to get its multihash
        c, err := cid.Decode(line)
        if err != nil {
            return nil, fmt.Errorf("ERROR: Failed to decode CID %s: %w", line, err)
        }
        mh := c.Hash()
        goodMHs[string(mh)] = struct{}{}
    }

    if err := scanner.Err(); err != nil {
        return nil, err
    }

    return goodMHs, nil
}

// Modify findStrayReferences to return strayMHs directly
func findStrayReferences(db *pebble.DB, goodMHs map[string]struct{}, groupKeyUint uint64) ([]multihash.Multihash, error) {
    strayMHs := multihash.NewSet()

    // Prepare the group key bytes for comparison
    groupBytes := make([]byte, 8)
    binary.BigEndian.PutUint64(groupBytes, groupKeyUint)

    // Iterate over all keys that start with "i:"
    iter := db.NewIter(nil)
    prefix := []byte("i:")
    for iter.SeekGE(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
        key := iter.Key()

        // Key format: "i:[mh bytes][i64BE groupIdx]"
        if len(key) <= len(prefix)+8 {
            continue // Key too short, skip
        }

        // Extract the group key bytes (last 8 bytes)
        keyGroupBytes := key[len(key)-8:]

        // Check if this key is for our target group
        if bytes.Equal(keyGroupBytes, groupBytes) {
            // Extract the multihash bytes
            mhBytes := key[len(prefix) : len(key)-8]

            // Check if this multihash is in our good list
            if _, ok := goodMHs[string(mhBytes)]; !ok {
                // This is a stray reference, add to strayMHs directly
                mh, err := multihash.Cast(mhBytes)
                if err != nil {
                    return nil, fmt.Errorf("ERROR: Failed to cast multihash: %w", err)
                }
                strayMHs.Add(mh)
            }
        }
    }

    if err := iter.Error(); err != nil {
        return nil, fmt.Errorf("ERROR: Iterator error: %w", err)
    }

    if err := iter.Close(); err != nil {
        return nil, fmt.Errorf("ERROR: Closing iterator: %w", err)
    }

    return strayMHs.All(), nil
}

// countExistingEntries now uses the GetSizes method to count existing entries
func countExistingEntries(index *rbstor.PebbleIndex, mhList []multihash.Multihash, groupKey ribs.GroupKey) (int, error) {
    existingCount := 0

    // Use GetGroups to check for each multihash in the list
    err := index.GetGroups(context.Background(), mhList, func(cidx int, gk ribs.GroupKey) (bool, error) {
        if gk == groupKey {
            existingCount++

            // return false to stop processing this entry as we found the proper CID
            return false, nil
        }
        // Always return true to continue for all stray CIDs
        return true, nil
    })
    if err != nil {
        return 0, fmt.Errorf("ERROR: Failed to count existing entries: %w", err)
    }
    return existingCount, nil
}

