// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package bloom_test

import (
	"testing"
	"time"

	"github.com/ipfs/bbloom"
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

		bf, err := bbloom.New(float64(1024), float64(7))
		require.NoError(t, err)

		updater := bloom.NewUpdater(tempDB.ConnStr, bf)
		defer ctx.Check(updater.Close)

		updater.Run(ctx)

		require.False(t, bf.HasTS([]byte("abc")))

		err = db.PutBlock(ctx, "abc", nil)
		require.NoError(t, err)

		// Check for up to a second if the bloom filter has been updated
		start := time.Now()
		for {
			if bf.HasTS([]byte("abc")) {
				return
			}
			if time.Since(start) > 10*time.Millisecond {
				assert.Fail(t, "bloom filter not updated")
				return
			}
			time.Sleep(1 * time.Second)
		}
	})
}
