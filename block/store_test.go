// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package block_test

import (
	"testing"

	dstest "github.com/ipfs/go-datastore/test"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/block"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/testutil"
)

func TestIPFSSuite(t *testing.T) {
	testutil.RunBlockstoreTest(t, func(t *testing.T, blocks *block.Store) {
		t.Run("basic operations", func(t *testing.T) {
			dstest.SubtestBasicPutGet(t, blocks)
		})
		t.Run("not found operations", func(t *testing.T) {
			dstest.SubtestNotFounds(t, blocks)
		})
		t.Run("query prefix", func(t *testing.T) {
			dstest.SubtestPrefix(t, blocks)
		})
		t.Run("query order", func(t *testing.T) {
			dstest.SubtestOrder(t, blocks)
		})
		t.Run("quert limit", func(t *testing.T) {
			dstest.SubtestLimit(t, blocks)
		})
		t.Run("query filter", func(t *testing.T) {
			dstest.SubtestFilter(t, blocks)
		})
		t.Run("many puts and gets, query", func(t *testing.T) {
			dstest.SubtestManyKeysAndQuery(t, blocks)
		})
		t.Run("query return sizes", func(t *testing.T) {
			dstest.SubtestReturnSizes(t, blocks)
		})
		t.Run("basic sync", func(t *testing.T) {
			dstest.SubtestBasicSync(t, blocks)
		})
	})
}
