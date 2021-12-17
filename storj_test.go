// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"io/ioutil"
	"os"
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
)

func TestIPFSSuite(t *testing.T) {
	runTest(t, "basic operations", func(t *testing.T, storj *StorjDS) {
		dstest.SubtestBasicPutGet(t, storj)
	})
	runTest(t, "not found operations", func(t *testing.T, storj *StorjDS) {
		dstest.SubtestNotFounds(t, storj)
	})
	runTest(t, "many puts and gets, query", func(t *testing.T, storj *StorjDS) {
		dstest.SubtestManyKeysAndQuery(t, storj)
	})
	runTest(t, "return sizes", func(t *testing.T, storj *StorjDS) {
		dstest.SubtestReturnSizes(t, storj)
	})
	runTest(t, "batch", func(t *testing.T, storj *StorjDS) {
		dstest.RunBatchTest(t, storj)
	})
	runTest(t, "batch delete", func(t *testing.T, storj *StorjDS) {
		dstest.RunBatchDeleteTest(t, storj)
	})
	runTest(t, "batch put and delete", func(t *testing.T, storj *StorjDS) {
		dstest.RunBatchPutAndDeleteTest(t, storj)
	})
}

func runTest(t *testing.T, name string, f func(t *testing.T, storj *StorjDS)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		uplnk := planet.Uplinks[0]
		bucket := "testbucket"

		access, err := uplnk.Access[sat.ID()].Serialize()
		require.NoError(t, err)

		err = uplnk.CreateBucket(ctx, sat, bucket)
		require.NoError(t, err)

		dbFile, err := ioutil.TempFile(os.TempDir(), "storjds-db-")
		require.NoError(t, err)
		defer func() {
			err := os.Remove(dbFile.Name())
			require.NoError(t, err)
		}()

		storj, err := NewStorjDatastore(Config{
			DBPath:      dbFile.Name(),
			Bucket:      bucket,
			AccessGrant: access,
		})
		require.NoError(t, err)

		defer func() {
			err := storj.Close()
			require.NoError(t, err)
		}()

		f(t, storj)
	})
}
