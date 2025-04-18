package main

import (
    "bufio"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "os"
    "github.com/cockroachdb/pebble"
    "github.com/multiformats/go-multihash"
    "strings"
    "encoding/binary"
)

type Entry struct {
    Group  uint64    `json:"group"`  // The group field
    Prefix string `json:"prefix"` // Prefix field
    Mh     struct {
        Mh string `json:"mh"` // Mh.mh field
    } `json:"mh"`
}

const (
    red   = "\033[31m"
    green = "\033[32m"
    reset = "\033[0m"
)

func main() {
    // Standard flag parsing
    indexPath := flag.String("index", "", "Path to the pebble index")
    entriesFile := flag.String("entries-file", "", "File containing entries to delete in the format: JSON (prefix + mh.mh)")
    reallyDoIt := flag.Bool("really-do-it", false, "If true, delete the entries, STOP any processes accessing pebble before using it")
    flag.Parse()

    log.SetFlags(0)
    // Ensure required flags are provided
    if *indexPath == "" || *entriesFile == "" {
        log.Fatal("ERROR: Missing required flags: --index and --entries-file")
    }

    fmt.Println("Loading file entries")
    entries, err := loadEntriesFromFile(*entriesFile)
    if err != nil {
        log.Fatalf("Error loading entries from file: %v", err)
    }

    // Open the Pebble DB
    db, err := pebble.Open(*indexPath, &pebble.Options{})
    dbClosed := false
    if err != nil {
        log.Fatalf("ERROR: Failed to open Pebble DB at path %s: %s", *indexPath, err)
    }

    defer func() {
        if !dbClosed {
            if err := db.Close(); err != nil {
                log.Printf("WARNING: Failed to close Pebble DB: %s", err)
            }
        }
    }()

    // Get the number of entries in the DB before the search
    fmt.Println("Calculating pebble size")
    dbEntriesBefore := countEntriesInDB(db)

    // Variables for tracking found and not found
    var foundCount, deletedCount int

    // Iterate through each entry and search for the exact key using db.Get
    fmt.Println("Processing entries")
    for _, entry := range entries {
        prefix := entry.Prefix
        mhHashStr := entry.Mh.Mh

        // Convert mh.mh (multihash) to bytes
        mhBytes, err := multihash.FromHexString(mhHashStr)
        if err != nil {
            log.Printf("ERROR: Invalid multihash string %s: %v", mhHashStr, err)
            continue
        }

        // Form the full key to search for (prefix + mhBytes)
        searchKey := append([]byte(prefix), mhBytes...)

        // Add group for i: entries
        if prefix == "i:" {
            groupBytes := make([]byte, 8)
            binary.BigEndian.PutUint64(groupBytes, entry.Group)
            searchKey = append(searchKey, groupBytes...)
        }

        fmt.Printf("Searching for key %s %s: ", prefix, mhHashStr)

        // Use db.Get to retrieve the exact key's value
        _, closer, err := db.Get(searchKey)
        if err != nil {
            if err == pebble.ErrNotFound {
                // Show NOTFOUND in red
                fmt.Printf("%sNOT FOUND%s\n", red, reset)
            } else {
                log.Printf("ERROR: Failed to retrieve key %s: %v", searchKey, err)
            }
        } else {
            foundCount++

            if *reallyDoIt {
                fmt.Printf("%sDELETED%s\n", green, reset)
                err := db.Delete(searchKey, nil)  // Uncomment to delete
                if err == nil {
                    deletedCount++
                } else {
                    fmt.Printf("%sFOUND BUT CANNOT BE DELETED%s\n", red, reset)
                }
            } else {
                fmt.Printf("%sFOUND%s\n", green, reset)
            }
        }

        // Close the closer once done
        if closer != nil {
            closer.Close()
        }
    }

    // Get the number of entries in the DB after the search
    fmt.Println("Calculating pebble size")
    dbEntriesAfter := countEntriesInDB(db)

    // Display the summary with proper alignment
    fmt.Printf("\nSummary:\n")
    fmt.Printf("%-35s %d\n", "Entries in file", len(entries))
    // Change the color based on the found count relative to total entries
    color := green
    if foundCount < len(entries) {
        color = red
    }

    fmt.Printf("%-35s %s%d/%d%s\n", "Entries Found in pebble", color, foundCount, len(entries), reset)

    color = green
    if deletedCount != foundCount {
        color = red
    }
    fmt.Printf("%-35s %s%d/%d%s\n", "Entries Deleted from pebble", color, deletedCount, foundCount, reset)
    fmt.Printf("%-35s %d\n", "Entries in pebble before deletion", dbEntriesBefore)
    fmt.Printf("%-35s %d\n", "Entries in pebble after deletion", dbEntriesAfter)
    fmt.Printf("%-35s %d\n", "Diff pebble count (before-after)", dbEntriesBefore-dbEntriesAfter)

    // If no entries were deleted and --really-do-it was not set, show a message
    if !*reallyDoIt && deletedCount == 0 {
        fmt.Println("No deletion performed. STOP CIDgravity-gw BEFORE RUNNING THIS COMMAND, and use --really-do-it to perform the deletion")
    }

    // Close the DB here since it's no longer needed
    if err := db.Close(); err != nil {
        fmt.Fprintf(os.Stderr, "WARNING: Error closing the database: %s", err)
    }
    dbClosed = true
}

// Function to read the file and load the entries into a slice
func loadEntriesFromFile(filePath string) ([]Entry, error) {
    // Open the file
    file, err := os.Open(filePath)
    if err != nil {
        return nil, fmt.Errorf("failed to open file: %v", err)
    }
    defer file.Close()

    var entries []Entry
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := strings.TrimSpace(scanner.Text())
        if line != "" {
            var entry Entry
            if err := json.Unmarshal([]byte(line), &entry); err != nil {
                return nil, fmt.Errorf("failed to unmarshal line: %v. Lines must be in the format produced by dump_pebble", err)
            }
            entries = append(entries, entry)
        }
    }

    if err := scanner.Err(); err != nil {
        return nil, fmt.Errorf("failed to read file: %v", err)
    }

    return entries, nil
}

// Function to count the number of entries in the database by iterating over it
func countEntriesInDB(db *pebble.DB) int {
    iter := db.NewIter(&pebble.IterOptions{})
    defer iter.Close()

    count := 0
    for iter.First(); iter.Valid(); iter.Next() {
        count++
    }

    return count
}
