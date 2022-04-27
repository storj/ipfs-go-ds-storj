// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package bloom_test

import (
	"testing"
	"time"

	"github.com/ipfs/bbloom"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/ipfs-go-ds-storj/bloom"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/ipfs-go-ds-storj/testutil"
	"storj.io/private/dbutil"
)

func TestBloomUpdater(t *testing.T) {
	testutil.RunDBTest(t, func(t *testing.T, ctx *testcontext.Context, tempDB *dbutil.TempDatabase, db *db.DB) {
		if tempDB.Implementation != dbutil.Cockroach {
			t.Skipf("%s not supported", tempDB.Implementation)
		}

		// Required for the proper execution of the changefeed
		_, err := db.Exec(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		require.NoError(t, err)

		bf, err := bbloom.New(float64(1024), float64(7))
		require.NoError(t, err)

		updater := bloom.NewUpdater(tempDB.ConnStr, bf)
		defer ctx.Check(updater.Close)

		updater.Run(ctx)

		b58Hash := "QmVAXDpe8PKRxScTdZ6nMYpBVwU9EV5CwefQhyBVrWPvzb"
		multihash, err := mh.FromB58String(b58Hash)
		require.NoError(t, err)

		require.False(t, bf.HasTS(multihash))

		err = db.PutBlock(ctx, dshelp.MultihashToDsKey(multihash).Name(), nil)
		require.NoError(t, err)

		// Check for up to a second if the bloom filter has been updated
		start := time.Now()
		for {
			if bf.HasTS(multihash) {
				return
			}
			if time.Since(start) > 1*time.Second {
				assert.Fail(t, "bloom filter not updated")
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}
