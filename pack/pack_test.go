// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package pack_test

import (
	"fmt"
	"testing"

	ds "github.com/ipfs/go-datastore"
	bs "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	storjds "storj.io/ipfs-go-ds-storj"
	"storj.io/ipfs-go-ds-storj/pack"
	"storj.io/ipfs-go-ds-storj/testutil"
	"storj.io/storj/private/testplanet"
)

func TestPack_HappyPath(t *testing.T) {
	runPackTest(t, func(ctx *testcontext.Context, storj *storjds.Datastore) {})
}

func TestPack_ContinueInterrupted(t *testing.T) {
	runPackTest(t, func(ctx *testcontext.Context, storj *storjds.Datastore) {
		// Simulate interrupted packing by setting the next batch to packing status
		blocks, err := storj.DB().QueryNextPack(ctx, storj.MinPackSize, storj.MaxPackSize)
		require.NoError(t, err)
		require.Len(t, blocks, 8)
	})
}

func runPackTest(t *testing.T, initPackStatus func(ctx *testcontext.Context, storj *storjds.Datastore)) {
	testutil.RunDatastoreTest(t, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
		storj = storj.WithPackSize(1*memory.MiB.Int(), 2*memory.MiB.Int())

		var keys []ds.Key
		for i := 0; i < 10; i++ {
			keys = append(keys, ds.KeyWithNamespaces([]string{"blocks", fmt.Sprintf("block%d", i)}))
		}

		var blobs [][]byte
		for i := 0; i < 10; i++ {
			blobs = append(blobs, testrand.Bytes(256*memory.KiB))
		}

		for i, key := range keys {
			err := storj.Put(ctx, key, blobs[i])
			require.NoError(t, err)
		}

		initPackStatus(ctx, storj)

		// Sync starts the pack chore
		err := storj.Sync(ctx, bs.BlockPrefix)
		require.NoError(t, err)

		storj.TriggerWaitPacker()

		var objectKey string

		for i, key := range keys {
			block, err := storj.DB().GetBlock(ctx, key.Name())
			require.NoError(t, err, i)
			if i < 8 {
				assert.Equal(t, pack.Packed, pack.Status(block.PackStatus), i)
				assert.Nil(t, block.Data, i)
				assert.NotEmpty(t, block.PackObject, i)
				assert.NotZero(t, block.PackOffset, i)
				objectKey = block.PackObject
			} else {
				assert.Equal(t, pack.Unpacked, pack.Status(block.PackStatus), i)
				assert.Equal(t, blobs[i], block.Data, i)
				assert.Empty(t, block.PackObject, i)
				assert.Zero(t, block.PackOffset, i)
			}
		}

		project, err := planet.Uplinks[0].GetProject(ctx, planet.Satellites[0])
		require.NoError(t, err)

		obj, err := project.StatObject(ctx, storj.Bucket, objectKey)
		require.NoError(t, err)
		require.Greater(t, obj.System.ContentLength, 2*memory.MiB.Int64())
		require.Equal(t, "application/zip", obj.Custom["content-type"])

		for i, key := range keys {
			data, err := storj.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, blobs[i], data)
		}

	})

}
