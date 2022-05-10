// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package db_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/ipfs-go-ds-storj/testutil"
	"storj.io/private/dbutil"
)

func TestGetCreatedTime(t *testing.T) {
	testutil.RunDBTest(t, func(t *testing.T, ctx *testcontext.Context, tempDB *dbutil.TempDatabase, db *db.DB) {
		created, err := db.GetCreatedTime(ctx)
		require.NoError(t, err)
		assert.WithinDuration(t, time.Now(), created, 1*time.Second)
	})
}

// DeleteAll deletes all data from database.
type DeleteAll struct{}

// Check runs the test.
func (step DeleteAll) Check(ctx *testcontext.Context, t testing.TB, db *db.DB) {
	err := db.TestingDeleteAll(ctx)
	require.NoError(t, err)
}
