// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package testutil

import (
	"errors"
	"fmt"
	"os"
	"testing"

	storjds "github.com/kaloyan-raev/ipfs-go-ds-storj"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/db"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"

	"storj.io/common/testcontext"
	"storj.io/private/dbutil"
	"storj.io/private/dbutil/tempdb"
	"storj.io/storj/private/testplanet"
)

func RunTest(t *testing.T, name string, f func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		var storj *storjds.Datastore
		var tempDB *dbutil.TempDatabase
		defer func() {
			var err1, err2 error
			if tempDB != nil {
				err1 = tempDB.Close()
			}
			if storj != nil {
				storj.Close()
			}
			require.NoError(t, errs.Combine(err1, err2))
		}()

		sat := planet.Satellites[0]
		uplnk := planet.Uplinks[0]
		bucket := "testbucket"

		dbURI, err := dbURI(sat.Metabase.DB.Implementation())
		require.NoError(t, err)

		tempDB, err = tempdb.OpenUnique(ctx, dbURI, "ipfs-go-ds-storj")
		require.NoError(t, err)

		db := db.Wrap(tempDB.DB)

		err = db.MigrateToLatest(ctx)
		require.NoError(t, err)

		access, err := uplnk.Access[sat.ID()].Serialize()
		require.NoError(t, err)

		err = uplnk.CreateBucket(ctx, sat, bucket)
		require.NoError(t, err)

		storj, err = storjds.NewDatastore(ctx, storjds.Config{
			Bucket:      bucket,
			AccessGrant: access,
		}, db)
		require.NoError(t, err)

		f(t, ctx, planet, storj)
	})
}

func dbURI(impl dbutil.Implementation) (string, error) {
	switch impl {
	case dbutil.Postgres:
		dbURI, set := os.LookupEnv("STORJ_TEST_POSTGRES")
		if !set {
			return "", errors.New("STORJ_TEST_POSTGRES is not set")
		}
		return dbURI, nil
	case dbutil.Cockroach:
		dbURI, set := os.LookupEnv("STORJ_TEST_COCKROACH")
		if !set {
			return "", errors.New("STORJ_TEST_COCKROACH is not set")
		}
		return dbURI, nil
	default:
		return "", fmt.Errorf("unsupported database implementation %q", impl)
	}
}
