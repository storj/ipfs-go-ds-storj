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
	testutil.RunTest(t, "basic operations",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.StorjDS) {
			dstest.SubtestBasicPutGet(t, storj)
		})
	testutil.RunTest(t, "not found operations",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.StorjDS) {
			dstest.SubtestNotFounds(t, storj)
		})
	testutil.RunTest(t, "many puts and gets, query",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.StorjDS) {
			dstest.SubtestManyKeysAndQuery(t, storj)
		})
	testutil.RunTest(t, "return sizes",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.StorjDS) {
			dstest.SubtestReturnSizes(t, storj)
		})
	testutil.RunTest(t, "batch",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.StorjDS) {
			dstest.RunBatchTest(t, storj)
		})
	testutil.RunTest(t, "batch delete",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.StorjDS) {
			dstest.RunBatchDeleteTest(t, storj)
		})
	testutil.RunTest(t, "batch put and delete",
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.StorjDS) {
			dstest.RunBatchPutAndDeleteTest(t, storj)
		})
}
