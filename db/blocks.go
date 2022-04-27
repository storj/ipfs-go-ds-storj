// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.
package db

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
)

const (
	unpackedStatus = "0"
	packingStatus  = "1"
	packedStatus   = "2"
)

type Block struct {
	CID        string
	Size       int
	Data       []byte
	Deleted    bool
	PackStatus int
	PackObject string
	PackOffset int
}

func (db *DB) PutBlock(ctx context.Context, cid string, value []byte) (err error) {
	defer mon.Task()(&ctx)(&err)

	result, err := db.ExecContext(ctx, `
		INSERT INTO blocks (cid, size, data)
		VALUES ($1, $2, $3)
		ON CONFLICT(cid)
		DO UPDATE SET deleted = false
	`, cid, len(value), value)
	if err != nil {
		return Error.Wrap(err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return Error.Wrap(err)
	}
	if affected != 1 {
		return Error.New("expected 1 row inserted in db, but did %d", affected)
	}

	return nil
}

func (db *DB) GetBlock(ctx context.Context, cid string) (block *Block, err error) {
	defer mon.Task()(&ctx)(&err)

	block = &Block{
		CID: cid,
	}

	err = db.QueryRowContext(ctx, `
		SELECT
			size, data, deleted,
			pack_status, pack_object, pack_offset
		FROM blocks
		WHERE cid = $1
	`, block.CID).Scan(
		&block.Size, &block.Data, &block.Deleted,
		&block.PackStatus, &block.PackObject, &block.PackOffset,
	)
	if err != nil {
		if isNotFound(err) {
			return nil, ds.ErrNotFound
		}
		return nil, Error.Wrap(err)
	}

	if block.Deleted {
		return nil, ds.ErrNotFound
	}

	return block, nil
}

func (db *DB) HasBlock(ctx context.Context, cid string) (exists bool, err error) {
	defer mon.Task()(&ctx)(&err)

	var deleted bool
	err = db.QueryRowContext(ctx, `
		SELECT deleted
		FROM blocks
		WHERE cid = $1
	`, cid).Scan(
		&deleted,
	)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, Error.Wrap(err)
	}

	return !deleted, nil
}

func (db *DB) GetBlockSize(ctx context.Context, cid string) (size int, err error) {
	defer mon.Task()(&ctx)(&err)

	var deleted bool
	err = db.QueryRowContext(ctx, `
		SELECT size, deleted
		FROM blocks
		WHERE cid = $1
	`, cid).Scan(
		&size, &deleted,
	)
	if err != nil {
		if isNotFound(err) {
			return -1, ds.ErrNotFound
		}
		return -1, Error.Wrap(err)
	}

	if deleted {
		return -1, ds.ErrNotFound
	}

	return size, nil
}

func (db *DB) DeleteBlock(ctx context.Context, cid string) (err error) {
	defer mon.Task()(&ctx)(&err)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Error.Wrap(err)
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, tx.Rollback())
			return
		}
		err = tx.Commit()
	}()

	_, err = tx.ExecContext(ctx, `
		DELETE FROM blocks
		WHERE
			cid = $1 AND
			pack_status = 0;
	`, cid)

	_, err = tx.ExecContext(ctx, `
		UPDATE blocks
		SET deleted = true
		WHERE
			cid = $1 AND
			pack_status > 0;
	`, cid)

	return Error.Wrap(err)
}

func (db *DB) QueryNextPack(ctx context.Context, minSize, maxSize int) (blocks map[string][]byte, err error) {
	defer mon.Task()(&ctx)(&err)

	result, err := db.ExecContext(ctx, `
		WITH next_pack AS (
			SELECT b.cid, sum(b2.size) AS sums
			FROM blocks b
			INNER JOIN blocks b2 ON b.pack_status=b2.pack_status AND b2.created <= b.created
			WHERE b.pack_status = `+unpackedStatus+`
			GROUP BY b.cid
			HAVING sum(b2.size) <= $1
			ORDER BY b.created ASC
		)
		UPDATE blocks
		SET pack_status = `+packingStatus+`
		WHERE 
			$2 <= (SELECT max(sums) FROM next_pack) AND
			cid IN (SELECT cid FROM next_pack)
	`, maxSize, minSize)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return nil, Error.Wrap(err)
	}

	log.Desugar().Debug("QueryNextPack", zap.Int64("Affected Rows", affected))

	rows, err := db.QueryContext(ctx, `
		SELECT cid, data
		FROM blocks
		WHERE
			pack_status = `+packingStatus+`
	`)
	if err != nil {
		return nil, Error.Wrap(err)
	}
	defer rows.Close()

	blocks = make(map[string][]byte)
	for rows.Next() {
		var cid string
		var data []byte
		if err := rows.Scan(&cid, &data); err != nil {
			return nil, Error.Wrap(err)
		}
		blocks[cid] = data
	}
	if err = rows.Err(); err != nil {
		return nil, Error.Wrap(err)
	}

	log.Desugar().Debug("QueryNextPack", zap.Int("Pending Blocks", len(blocks)))

	return blocks, nil
}

func (db *DB) UpdatePackedBlocks(ctx context.Context, packObjectKey string, cidOffs map[string]int) (err error) {
	defer mon.Task()(&ctx)(&err)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Error.Wrap(err)
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, tx.Rollback())
			return
		}
		err = tx.Commit()
	}()

	for cid, off := range cidOffs {
		result, err := tx.ExecContext(ctx, `
			UPDATE blocks
			SET
				pack_status = `+packedStatus+`, 
				pack_object = $1,
				pack_offset = $2,
				data = NULL
			WHERE
				cid = $3 AND
				pack_status = `+packingStatus+`
		`, packObjectKey, off, cid)
		if err != nil {
			return Error.Wrap(err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return Error.Wrap(err)
		}
		if affected != 1 {
			return Error.New("unexpected number of blocks updated db: want 1, got %d", affected)
		}

		log.Desugar().Debug("UpdatePackedBlocks: updated block status as packed", zap.String("CID", cid), zap.Int("Offset", off))
	}

	return nil
}
