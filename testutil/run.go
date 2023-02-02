// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package testutil

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/testcontext"
	storjds "storj.io/ipfs-go-ds-storj"
	"storj.io/ipfs-go-ds-storj/block"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/ipfs-go-ds-storj/pack"
	"storj.io/private/dbutil"
	"storj.io/private/dbutil/tempdb"
	"storj.io/storj/private/testplanet"
	"storj.io/uplink"
)

func RunDatastoreTest(t *testing.T, f func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Timeout:          10 * time.Minute,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		var storj *storjds.Datastore
		var tempDB *dbutil.TempDatabase
		var err error
		defer func() {
			var dbErr, dsErr error
			if tempDB != nil {
				dbErr = tempDB.Close()
			}
			if storj != nil {
				dsErr = storj.Close()
			}
			require.NoError(t, errs.Combine(dbErr, dsErr))
		}()

		sat := planet.Satellites[0]
		uplnk := planet.Uplinks[0]
		bucket := "testbucket"

		dbURI := dbURI(t, sat.Metabase.DB.Implementation())

		tempDB, err = tempdb.OpenUnique(ctx, dbURI, "ipfs-go-ds-storj")
		require.NoError(t, err)

		db := db.Wrap(tempDB.DB)

		err = db.MigrateToLatest(ctx)
		require.NoError(t, err)

		access, err := uplnk.Access[sat.ID()].Serialize()
		require.NoError(t, err)

		err = uplnk.CreateBucket(ctx, sat, bucket)
		require.NoError(t, err)

		storj, err = storjds.OpenDatastore(ctx, db, storjds.Config{
			Bucket:      bucket,
			AccessGrant: access,
		})
		require.NoError(t, err)

		f(t, ctx, planet, storj)
	})
}

func RunBlockstoreTest(t *testing.T, f func(t *testing.T, blocks *block.Store)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
		Timeout:          10 * time.Minute,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		uplnk := planet.Uplinks[0]
		bucket := "testbucket"

		dbURI := dbURI(t, sat.Metabase.DB.Implementation())

		tempDB, err := tempdb.OpenUnique(ctx, dbURI, "ipfs-go-ds-storj")
		require.NoError(t, err)
		defer ctx.Check(tempDB.Close)

		db := db.Wrap(tempDB.DB)

		err = db.MigrateToLatest(ctx)
		require.NoError(t, err)

		err = uplnk.CreateBucket(ctx, sat, bucket)
		require.NoError(t, err)

		project, err := uplink.OpenProject(ctx, uplnk.Access[sat.ID()])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		blocks := block.NewStore("/", db, pack.NewStore(project, bucket))
		defer ctx.Check(blocks.Close)

		f(t, blocks)
	})
}

func RunDBTest(t *testing.T, f func(t *testing.T, ctx *testcontext.Context, tempDB *dbutil.TempDatabase, db *db.DB)) {
	for _, impl := range []dbutil.Implementation{dbutil.Postgres, dbutil.Cockroach} {
		impl := impl
		t.Run(strings.Title(impl.String()), func(t *testing.T) {
			t.Parallel()

			ctx := testcontext.New(t)
			defer ctx.Cleanup()

			dbURI := dbURI(t, impl)

			tempDB, err := tempdb.OpenUnique(ctx, dbURI, "ipfs-go-ds-storj")
			require.NoError(t, err)
			defer ctx.Check(tempDB.Close)

			db := db.Wrap(tempDB.DB)

			err = db.MigrateToLatest(ctx)
			require.NoError(t, err)

			f(t, ctx, tempDB, db)
		})
	}
}

func dbURI(t *testing.T, impl dbutil.Implementation) (dbURI string) {
	switch impl {
	case dbutil.Postgres:
		dbURI, set := os.LookupEnv("STORJ_TEST_POSTGRES")
		if !set {
			t.Skip("skipping test suite; STORJ_TEST_POSTGRES is not set.")
		}
		return dbURI
	case dbutil.Cockroach:
		dbURI, set := os.LookupEnv("STORJ_TEST_COCKROACH")
		if !set {
			t.Skip("skipping test suite; STORJ_TEST_COCKROACH is not set.")
		}
		return dbURI
	default:
		t.Errorf("unsupported database implementation %q", impl)
		return ""
	}
}
