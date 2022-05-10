// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.
package db

import (
	"context"
)

// TestingDeleteAll deletes all data from the database.
func (db *DB) TestingDeleteAll(ctx context.Context) (err error) {
	_, err = db.ExecContext(ctx, `
		TRUNCATE blocks, datastore
	`)
	return Error.Wrap(err)
}
