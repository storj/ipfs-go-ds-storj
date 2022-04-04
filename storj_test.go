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
	// testutil.RunTest(t, "all tests",
	// 	func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
	// 		dstest.SubtestAll(t, storj)
	// 	})
	testutil.RunTest(t, "basic operations",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestBasicPutGet(t, storj)
		})
	testutil.RunTest(t, "not found operations",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestNotFounds(t, storj)
		})
	testutil.RunTest(t, "query prefix",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestPrefix(t, storj)
		})
	testutil.RunTest(t, "query order",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestOrder(t, storj)
		})
	testutil.RunTest(t, "quert limit",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestLimit(t, storj)
		})
	testutil.RunTest(t, "query filter",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestFilter(t, storj)
		})
	testutil.RunTest(t, "many puts and gets, query",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestManyKeysAndQuery(t, storj)
		})
	testutil.RunTest(t, "query return sizes",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestReturnSizes(t, storj)
		})
	testutil.RunTest(t, "basic sync",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.SubtestBasicSync(t, storj)
		})
	testutil.RunTest(t, "batch",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.RunBatchTest(t, storj)
		})
	testutil.RunTest(t, "batch delete",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.RunBatchDeleteTest(t, storj)
		})
	testutil.RunTest(t, "batch put and delete",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore) {
			dstest.RunBatchPutAndDeleteTest(t, storj)
		})
}
