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
func PutRandomBlocks(ctx *testcontext.Context, t testing.TB, db *db.DB, count int, status pack.Status) {
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
		if status != pack.Unpacked {
			UpdateBlockPackStatus{
				CID:       cid,
				NewStatus: status,
			}.Check(ctx, t, db)
		}
	}
}

func TestGetNotPackedBlocksTotalSize(t *testing.T) {
	testutil.RunDBTest(t, func(t *testing.T, ctx *testcontext.Context, tempDB *dbutil.TempDatabase, db *db.DB) {
		for _, tt := range []struct {
			name          string
			unpackedCount int
			packingCount  int
			packedCount   int
		}{
			{
				name: "no blocks",
			},
			{
				name:          "only unpacked blocks",
				unpackedCount: 3,
			},
			{
				name:         "only packing blocks",
				packingCount: 3,
			},
			{
				name:        "only packed blocks",
				packedCount: 3,
			},
			{
				name:          "unpacked and packing blocks",
				unpackedCount: 3,
				packingCount:  3,
			},
			{
				name:          "unpacked and packed blocks",
				unpackedCount: 3,
				packedCount:   3,
			},
			{
				name:         "packing and packed blocks",
				packingCount: 3,
				packedCount:  3,
			},
			{
				name:          "all unpacked, packing, and packed blocks",
				unpackedCount: 3,
				packingCount:  3,
				packedCount:   3,
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

				assert.Equal(t, int64(tt.unpackedCount*unpackedBlockSize), unpackedSize)
				assert.Equal(t, int64(tt.packingCount*packingBlockSize), packingSize)
			})
		}
	})
}
