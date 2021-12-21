// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package testutil

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	storjds "github.com/kaloyan-raev/ipfs-go-ds-storj"
	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/private/dbutil/pgutil"
	"storj.io/storj/private/testplanet"
)

func RunTest(t *testing.T, name string, f func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet, storj *storjds.Datastore)) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 4,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		dbURI, set := os.LookupEnv("STORJ_TEST_POSTGRES")
		if !set {
			t.Skipf("skipping test suite; STORJ_TEST_POSTGRES is not set.")
		}

		sat := planet.Satellites[0]
		uplnk := planet.Uplinks[0]
		bucket := "testbucket"

		access, err := uplnk.Access[sat.ID()].Serialize()
		require.NoError(t, err)

		err = uplnk.CreateBucket(ctx, sat, bucket)
		require.NoError(t, err)

		conn, err := pgx.Connect(ctx, dbURI)
		require.NoError(t, err)
		defer func() {
			err := conn.Close(ctx)
			require.NoError(t, err)
		}()

		schemaName := pgutil.CreateRandomTestingSchemaName(6)
		_, err = conn.Exec(ctx, "CREATE SCHEMA "+pgutil.QuoteSchema(schemaName))
		require.NoError(t, err)

		storj, err := storjds.NewDatastore(storjds.Config{
			DBURI:       pgutil.ConnstrWithSchema(dbURI, schemaName),
			Bucket:      bucket,
			AccessGrant: access,
		})
		require.NoError(t, err)

		defer func() {
			err := storj.Close()
			require.NoError(t, err)
		}()

		f(t, ctx, planet, storj)
	})
}
