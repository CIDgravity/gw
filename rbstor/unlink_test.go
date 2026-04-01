package rbstor

import (
	"context"
	"os"
	"testing"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Set required environment variables for test configuration
	os.Setenv("RIBS_DATA", os.TempDir())
	os.Setenv("RIBS_MAX_LOCAL_GROUP_COUNT", "64")
	_ = configuration.LoadConfig()
}

func TestUnlinkBasic(t *testing.T) {
	ctx := context.Background()
	td := t.TempDir()

	// Setup test RBS with YugabyteDB
	rbs, cleanup := setupTestRBS(t, td)
	defer cleanup()
	require.NoError(t, rbs.Start())

	// Create a session and batch
	session := rbs.Session(ctx)
	batch := session.Batch(ctx)

	// Create some test blocks
	block1 := blocks.NewBlock([]byte("test data 1"))
	block2 := blocks.NewBlock([]byte("test data 2"))
	block3 := blocks.NewBlock([]byte("test data 3"))

	testBlocks := []blocks.Block{block1, block2, block3}

	// Put the blocks
	err := batch.Put(ctx, testBlocks)
	require.NoError(t, err)

	// Flush the batch
	err = batch.Flush(ctx)
	require.NoError(t, err)

	// Verify blocks exist by reading them
	var found1, found2, found3 bool
	err = session.View(ctx, []multihash.Multihash{block1.Cid().Hash(), block2.Cid().Hash(), block3.Cid().Hash()},
		func(cidx int, data []byte) {
			switch cidx {
			case 0:
				found1 = true
			case 1:
				found2 = true
			case 2:
				found3 = true
			}
		})
	require.NoError(t, err)
	assert.True(t, found1, "block1 should exist")
	assert.True(t, found2, "block2 should exist")
	assert.True(t, found3, "block3 should exist")

	// Now unlink block1 and block2
	batch2 := session.Batch(ctx)
	err = batch2.Unlink(ctx, []multihash.Multihash{block1.Cid().Hash(), block2.Cid().Hash()})
	require.NoError(t, err)

	// Flush the unlink batch
	err = batch2.Flush(ctx)
	require.NoError(t, err)

	// Verify blocks are no longer retrievable
	found1, found2, found3 = false, false, false
	err = session.View(ctx, []multihash.Multihash{block1.Cid().Hash(), block2.Cid().Hash(), block3.Cid().Hash()},
		func(cidx int, data []byte) {
			switch cidx {
			case 0:
				found1 = true
			case 1:
				found2 = true
			case 2:
				found3 = true
			}
		})
	require.NoError(t, err)
	assert.False(t, found1, "block1 should be unlinked")
	assert.False(t, found2, "block2 should be unlinked")
	assert.True(t, found3, "block3 should still exist")
}

func TestUnlinkNonExistent(t *testing.T) {
	ctx := context.Background()
	td := t.TempDir()

	// Setup test RBS with YugabyteDB
	rbs, cleanup := setupTestRBS(t, td)
	defer cleanup()
	require.NoError(t, rbs.Start())

	// Create a session and batch
	session := rbs.Session(ctx)
	batch := session.Batch(ctx)

	// Try to unlink a non-existent block (random CID)
	randomCid := cid.NewCidV1(cid.Raw, make([]byte, 32))
	err := batch.Unlink(ctx, []multihash.Multihash{randomCid.Hash()})
	require.NoError(t, err, "Unlinking non-existent blocks should not error")

	// Flush should also succeed
	err = batch.Flush(ctx)
	require.NoError(t, err)
}

func TestUnlinkAfterPutInSameBatch(t *testing.T) {
	ctx := context.Background()
	td := t.TempDir()

	// Setup test RBS with YugabyteDB
	rbs, cleanup := setupTestRBS(t, td)
	defer cleanup()
	require.NoError(t, rbs.Start())

	// Create a session and batch
	session := rbs.Session(ctx)
	batch := session.Batch(ctx)

	// Create a test block
	block := blocks.NewBlock([]byte("test data"))

	// Put the block
	err := batch.Put(ctx, []blocks.Block{block})
	require.NoError(t, err)

	// Unlink it in the same batch (before flush)
	err = batch.Unlink(ctx, []multihash.Multihash{block.Cid().Hash()})
	require.NoError(t, err)

	// Flush - per spec, Put should win over Unlink in conflicts
	err = batch.Flush(ctx)
	require.NoError(t, err)

	// The block should still exist because Put wins over Unlink
	var found bool
	err = session.View(ctx, []multihash.Multihash{block.Cid().Hash()},
		func(cidx int, data []byte) {
			found = true
		})
	require.NoError(t, err)
	assert.True(t, found, "block should exist (Put wins over Unlink)")
}
