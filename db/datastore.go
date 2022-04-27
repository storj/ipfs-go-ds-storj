// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.
package db

import (
	"context"
	"fmt"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

func (db *DB) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	defer mon.Task()(&ctx)(&err)

	result, err := db.ExecContext(ctx, `
		INSERT INTO datastore (key, data)
		VALUES ($1, $2)
		ON CONFLICT(key)
		DO UPDATE SET data = $2
	`, key.String(), value)
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

func (db *DB) Get(ctx context.Context, key ds.Key) (data []byte, err error) {
	defer mon.Task()(&ctx)(&err)

	err = db.QueryRowContext(ctx, `
		SELECT data
		FROM datastore
		WHERE key = $1
	`, key.String()).Scan(&data)
	if err != nil {
		if isNotFound(err) {
			return nil, ds.ErrNotFound
		}
		return nil, Error.Wrap(err)
	}
	return data, nil
}

func (db *DB) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	defer mon.Task()(&ctx)(&err)

	err = db.QueryRowContext(ctx, `
		SELECT exists(
			SELECT 1
			FROM datastore
			WHERE key = $1
		)
	`, key.String()).Scan(&exists)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, Error.Wrap(err)
	}
	return exists, nil
}

func (db *DB) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	defer mon.Task()(&ctx)(&err)

	err = db.QueryRowContext(ctx, `
		SELECT octet_length(data)
		FROM datastore
		WHERE key = $1
	`, key.String()).Scan(&size)
	if err != nil {
		if isNotFound(err) {
			return -1, ds.ErrNotFound
		}
		return -1, Error.Wrap(err)
	}
	return size, nil
}

func (db *DB) Delete(ctx context.Context, key ds.Key) (err error) {
	defer mon.Task()(&ctx)(&err)

	_, err = db.ExecContext(ctx, `
		DELETE FROM datastore
		WHERE key = $1
	`, key.String())

	return Error.Wrap(err)
}

func (db *DB) QueryDatastore(ctx context.Context, q dsq.Query) (result dsq.Results, err error) {
	defer mon.Task()(&ctx)(&err)

	var sql string
	if q.KeysOnly && q.ReturnsSizes {
		sql = "SELECT key, octet_length(data) FROM datastore"
	} else if q.KeysOnly {
		sql = "SELECT key FROM datastore"
	} else {
		sql = "SELECT key, data FROM datastore"
	}

	if q.Prefix != "" {
		// normalize
		prefix := ds.NewKey(q.Prefix).String()
		if prefix != "/" {
			sql += fmt.Sprintf(` WHERE key LIKE '%s%%' ORDER BY key`, prefix+"/")
		}
	}

	// only apply limit and offset if we do not have to naive filter/order the results
	if len(q.Filters) == 0 && len(q.Orders) == 0 {
		if q.Limit != 0 {
			sql += fmt.Sprintf(" LIMIT %d", q.Limit)
		}
		if q.Offset != 0 {
			sql += fmt.Sprintf(" OFFSET %d", q.Offset)
		}
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}

	it := dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			if !rows.Next() {
				if rows.Err() != nil {
					return dsq.Result{Error: rows.Err()}, false
				}
				return dsq.Result{}, false
			}

			var key string
			var size int
			var data []byte

			if q.KeysOnly && q.ReturnsSizes {
				err := rows.Scan(&key, &size)
				if err != nil {
					return dsq.Result{Error: err}, false
				}
				return dsq.Result{Entry: dsq.Entry{Key: key, Size: size}}, true
			} else if q.KeysOnly {
				err := rows.Scan(&key)
				if err != nil {
					return dsq.Result{Error: err}, false
				}
				return dsq.Result{Entry: dsq.Entry{Key: key}}, true
			}

			err := rows.Scan(&key, &data)
			if err != nil {
				return dsq.Result{Error: err}, false
			}
			entry := dsq.Entry{Key: key, Value: data}
			if q.ReturnsSizes {
				entry.Size = len(data)
			}
			return dsq.Result{Entry: entry}, true
		},
		Close: func() error {
			rows.Close()
			return nil
		},
	}

	res := dsq.ResultsFromIterator(q, it)

	for _, f := range q.Filters {
		res = dsq.NaiveFilter(res, f)
	}

	res = dsq.NaiveOrder(res, q.Orders...)

	// if we have filters or orders, offset and limit won't have been applied in the query
	if len(q.Filters) > 0 || len(q.Orders) > 0 {
		if q.Offset != 0 {
			res = dsq.NaiveOffset(res, q.Offset)
		}
		if q.Limit != 0 {
			res = dsq.NaiveLimit(res, q.Limit)
		}
	}

	return res, nil
}
