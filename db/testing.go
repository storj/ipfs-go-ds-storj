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

// TestingUpdateBlockPackStatus updates the block pack status.
func (db *DB) TestingUpdateBlockPackStatus(ctx context.Context, cid string, newStatus int) (err error) {
	_, err = db.ExecContext(ctx, `
		UPDATE blocks
		SET pack_status = $2
		WHERE cid = $1;
	`, cid, newStatus)
	return Error.Wrap(err)
}
