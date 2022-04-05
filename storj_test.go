// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds_test

import (
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
	storjds "github.com/kaloyan-raev/ipfs-go-ds-storj"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/testutil"
	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
)

func TestIPFSSuite(t *testing.T) {
	testutil.RunDatastoreTest(t, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
		t.Run("basic operations", func(t *testing.T) {
			dstest.SubtestBasicPutGet(t, storj)
		})
		t.Run("not found operations", func(t *testing.T) {
			dstest.SubtestNotFounds(t, storj)
		})
		t.Run("query prefix", func(t *testing.T) {
			dstest.SubtestPrefix(t, storj)
		})
		t.Run("query order", func(t *testing.T) {
			dstest.SubtestOrder(t, storj)
		})
		t.Run("quert limit", func(t *testing.T) {
			dstest.SubtestLimit(t, storj)
		})
		t.Run("query filter", func(t *testing.T) {
			dstest.SubtestFilter(t, storj)
		})
		t.Run("many puts and gets, query", func(t *testing.T) {
			dstest.SubtestManyKeysAndQuery(t, storj)
		})
		t.Run("query return sizes", func(t *testing.T) {
			dstest.SubtestReturnSizes(t, storj)
		})
		t.Run("basic sync", func(t *testing.T) {
			dstest.SubtestBasicSync(t, storj)
		})
		t.Run("batch", func(t *testing.T) {
			dstest.RunBatchTest(t, storj)
		})
		t.Run("batch delete", func(t *testing.T) {
			dstest.RunBatchDeleteTest(t, storj)
		})
		t.Run("batch put and delete", func(t *testing.T) {
			dstest.RunBatchPutAndDeleteTest(t, storj)
		})
	})
}
