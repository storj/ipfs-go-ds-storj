// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package db_test

import (
	"testing"

	"github.com/ipfs/go-cid"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/ipfs-go-ds-storj/pack"
	"storj.io/ipfs-go-ds-storj/testutil"
	"storj.io/private/dbutil"
)

const (
	unpackedBlockSize = 17
	packingBlockSize  = 19
	packedBlockSize   = 23
)

// PutBlock adds a block to the datastore.
type PutBlock struct {
	CID  string
	Data []byte
}

// Check runs the test step.
func (step PutBlock) Check(ctx *testcontext.Context, t testing.TB, db *db.DB) {
	err := db.PutBlock(ctx, step.CID, step.Data)
	require.NoError(t, err)
}

// UpdateBlockPackStatus updates the block pack status.
type UpdateBlockPackStatus struct {
	CID       string
	NewStatus pack.Status
}

// Check runs the test step.
func (step UpdateBlockPackStatus) Check(ctx *testcontext.Context, t testing.TB, db *db.DB) {
	err := db.TestingUpdateBlockPackStatus(ctx, step.CID, int(step.NewStatus))
	require.NoError(t, err)
}

// PutRandomBlock adds a block with size random data to the datastore.
func PutRandomBlock(ctx *testcontext.Context, t testing.TB, db *db.DB, size int) string {
	data := testrand.BytesInt(size)

	cid, err := cid.V0Builder{}.Sum(data)
	require.NoError(t, err)

	b32cid := dshelp.MultihashToDsKey(cid.Hash()).Name()

	PutBlock{
		CID:  b32cid,
		Data: data,
	}.Check(ctx, t, db)

	return b32cid
}

// PutRandomBlocks adds Count blocks to the datastore and updates their status.
func PutRandomBlocks(ctx *testcontext.Context, t testing.TB, db *db.DB, count int, status pack.Status) (cids []string) {
	if count <= 0 {
		return
	}

	var size int
	switch status {
	case pack.Unpacked:
		size = unpackedBlockSize
	case pack.Packing:
		size = packingBlockSize
	case pack.Packed:
		size = packedBlockSize
	default:
		t.Fatalf("unexpected status: %d", status)
	}

	for i := 0; i < count; i++ {
		cid := PutRandomBlock(ctx, t, db, size)
		cids = append(cids, cid)

		if status != pack.Unpacked {
			UpdateBlockPackStatus{
				CID:       cid,
				NewStatus: status,
			}.Check(ctx, t, db)
		}
	}

	return cids
}

func TestGetNotPackedBlocksTotalSize(t *testing.T) {
	testutil.RunDBTest(t, func(t *testing.T, ctx *testcontext.Context, tempDB *dbutil.TempDatabase, db *db.DB) {
		for _, tt := range []struct {
			name                 string
			unpackedCount        int
			packingCount         int
			packedCount          int
			expectedUnpackedSize int
			expectedPackingSize  int
		}{
			{
				name:                 "no blocks",
				expectedUnpackedSize: 0,
				expectedPackingSize:  0,
			},
			{
				name:                 "only unpacked blocks",
				unpackedCount:        2,
				expectedUnpackedSize: 2 * unpackedBlockSize,
				expectedPackingSize:  0,
			},
			{
				name:                 "only packing blocks",
				packingCount:         3,
				expectedUnpackedSize: 0,
				expectedPackingSize:  3 * packingBlockSize,
			},
			{
				name:                 "only packed blocks",
				packedCount:          4,
				expectedUnpackedSize: 0,
				expectedPackingSize:  0,
			},
			{
				name:                 "unpacked and packing blocks",
				unpackedCount:        2,
				packingCount:         3,
				expectedUnpackedSize: 2 * unpackedBlockSize,
				expectedPackingSize:  3 * packingBlockSize,
			},
			{
				name:                 "unpacked and packed blocks",
				unpackedCount:        2,
				packedCount:          4,
				expectedUnpackedSize: 2 * unpackedBlockSize,
				expectedPackingSize:  0,
			},
			{
				name:                 "packing and packed blocks",
				packingCount:         3,
				packedCount:          4,
				expectedUnpackedSize: 0,
				expectedPackingSize:  3 * packingBlockSize,
			},
			{
				name:                 "all unpacked, packing, and packed blocks",
				unpackedCount:        2,
				packingCount:         3,
				packedCount:          4,
				expectedUnpackedSize: 2 * unpackedBlockSize,
				expectedPackingSize:  3 * packingBlockSize,
			},
		} {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				defer DeleteAll{}.Check(ctx, t, db)

				PutRandomBlocks(ctx, t, db, tt.packedCount, pack.Packed)
				PutRandomBlocks(ctx, t, db, tt.packingCount, pack.Packing)
				PutRandomBlocks(ctx, t, db, tt.unpackedCount, pack.Unpacked)

				unpackedSize, packingSize, err := db.GetNotPackedBlocksTotalSize(ctx)
				require.NoError(t, err)

				assert.Equal(t, int64(tt.expectedUnpackedSize), unpackedSize)
				assert.Equal(t, int64(tt.expectedPackingSize), packingSize)
			})
		}
	})
}

func TestQueryPackingBlocksData(t *testing.T) {
	testutil.RunDBTest(t, func(t *testing.T, ctx *testcontext.Context, tempDB *dbutil.TempDatabase, db *db.DB) {
		for _, tt := range []struct {
			name          string
			unpackedCount int
			packingCount  int
			packedCount   int
			maxSize       int
			maxBlocks     int
			expectedCount int
		}{
			{
				name:          "no blocks",
				expectedCount: 0,
			},
			{
				name:          "only unpacked blocks",
				unpackedCount: 2,
				expectedCount: 0,
			},
			{
				name:          "only packing blocks",
				packingCount:  3,
				expectedCount: 3,
			},
			{
				name:          "only packed blocks",
				packedCount:   4,
				expectedCount: 0,
			},
			{
				name:          "unpacked and packing blocks",
				unpackedCount: 2,
				packingCount:  3,
				expectedCount: 3,
			},
			{
				name:          "unpacked and packed blocks",
				unpackedCount: 2,
				packedCount:   4,
				expectedCount: 0,
			},
			{
				name:          "packing and packed blocks",
				packingCount:  3,
				packedCount:   4,
				expectedCount: 3,
			},
			{
				name:          "all unpacked, packing, and packed blocks",
				unpackedCount: 2,
				packingCount:  3,
				packedCount:   4,
				expectedCount: 3,
			},
			{
				name:          "max blocks less than packing blocks",
				packingCount:  3,
				maxBlocks:     2,
				expectedCount: 2,
			},
			{
				name:          "max size less than total size of packing blocks (round)",
				packingCount:  3,
				maxSize:       2 * packingBlockSize,
				expectedCount: 2,
			},
			{
				name:          "max size less than total size of packing blocks (+1)",
				packingCount:  3,
				maxSize:       2*packingBlockSize + 1,
				expectedCount: 2,
			},
			{
				name:          "max size less than total size of packing blocks (-1)",
				packingCount:  3,
				maxSize:       1*packingBlockSize - 1,
				expectedCount: 0,
			},
			{
				name:          "max size and max blocks less than available packing blocks (max size < max blocks)",
				packingCount:  3,
				maxBlocks:     2,
				maxSize:       2*packingBlockSize - 1,
				expectedCount: 1,
			},
			{
				name:          "max size and max blocks less than available packing blocks (max size > max blocks)",
				packingCount:  3,
				maxBlocks:     1,
				maxSize:       2 * packingBlockSize,
				expectedCount: 1,
			},
		} {
			tt := tt

			maxSize := pack.DefaultMaxSize.Int()
			if tt.maxSize > 0 {
				maxSize = tt.maxSize
			}

			maxBlocks := pack.DefaultMaxBlocks
			if tt.maxBlocks > 0 {
				maxBlocks = tt.maxBlocks
			}

			t.Run(tt.name, func(t *testing.T) {
				defer DeleteAll{}.Check(ctx, t, db)

				PutRandomBlocks(ctx, t, db, tt.packedCount, pack.Packed)
				cids := PutRandomBlocks(ctx, t, db, tt.packingCount, pack.Packing)
				PutRandomBlocks(ctx, t, db, tt.unpackedCount, pack.Unpacked)

				result := make(map[string][]byte)
				err := db.QueryPackingBlocksData(ctx, maxSize, maxBlocks, result)
				require.NoError(t, err)

				assert.Len(t, result, tt.expectedCount)

				for _, cid := range cids {
					assert.Contains(t, cids, cid)
				}

				for _, data := range result {
					assert.Len(t, data, packingBlockSize)
				}
			})
		}
	})
}
