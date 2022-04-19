// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.
package block

import (
	"context"
	"fmt"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/ipfs-go-ds-storj/pack"
)

var mon = monkit.Package()

// Error is the error class for Storj block datastore.
var Error = errs.Class("block")

type Store struct {
	mount  string
	db     *db.DB
	packs  *pack.Store
	packer *pack.Chore
}

func NewStore(mount string, db *db.DB, packs *pack.Store) *Store {
	return &Store{
		mount:  mount,
		db:     db,
		packs:  packs,
		packer: pack.NewChore(db, packs),
	}
}

func (storj *Store) WithPackInterval(interval time.Duration) *Store {
	storj.packer.WithInterval(interval)
	return storj
}

func (storj *Store) WithPackSize(min, max int) *Store {
	storj.packer.WithPackSize(min, max)
	return storj
}

func (storj *Store) TriggerWaitPacker() {
	storj.packer.TriggerWait()
}

func (store *Store) Close() error {
	return store.packer.Close()
}

func (store *Store) Sync(ctx context.Context, prefix ds.Key) (err error) {
	defer mon.Task()(&ctx)(&err)

	// Run the packer in a separate context to avoid canceling it prematurely.
	store.packer.Run(context.Background())

	return nil
}

func (store *Store) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	defer mon.Task()(&ctx)(&err)

	return store.db.PutBlock(ctx, store.cid(key), value)
}

func (store *Store) Get(ctx context.Context, key ds.Key) (data []byte, err error) {
	defer mon.Task()(&ctx)(&err)

	block, err := store.db.GetBlock(ctx, store.cid(key))
	if err != nil {
		return nil, err
	}

	switch pack.Status(block.PackStatus) {
	case pack.Unpacked, pack.Packing:
		return block.Data, nil
	case pack.Packed:
		return store.packs.ReadBlock(ctx, block.PackObject, block.PackOffset, block.Size)
	default:
		return nil, Error.New("unknown pack status: %d", block.PackStatus)
	}
}

func (store *Store) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	defer mon.Task()(&ctx)(&err)

	return store.db.HasBlock(ctx, store.cid(key))
}

func (store *Store) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	defer mon.Task()(&ctx)(&err)

	return store.db.GetBlockSize(ctx, store.cid(key))
}

func (store *Store) Delete(ctx context.Context, key ds.Key) (err error) {
	defer mon.Task()(&ctx)(&err)

	return store.db.DeleteBlock(ctx, store.cid(key))
}

func (store *Store) Query(ctx context.Context, q dsq.Query) (result dsq.Results, err error) {
	defer mon.Task()(&ctx)(&err)

	var sql string
	if q.KeysOnly && q.ReturnsSizes {
		sql = "SELECT cid, size FROM blocks"
	} else if q.KeysOnly {
		sql = "SELECT cid FROM blocks"
	} else {
		sql = "SELECT cid, size, data, pack_status, pack_object, pack_offset FROM blocks"
	}

	prefix := strings.TrimPrefix(ds.NewKey(q.Prefix).String(), store.mount)
	if len(prefix) > 0 {
		sql += fmt.Sprintf(` WHERE cid LIKE '%s%%' AND deleted = false ORDER BY cid`, prefix+"/")
	} else {
		sql += " WHERE deleted = false"
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

	rows, err := store.db.Query(ctx, sql)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	it := dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			if !rows.Next() {
				if rows.Err() != nil {
					return dsq.Result{Error: rows.Err()}, false
				}
				return dsq.Result{}, false
			}

			var (
				cid        string
				size       int
				data       []byte
				packStatus int
				packObject string
				packOffset int
			)

			if q.KeysOnly && q.ReturnsSizes {
				err := rows.Scan(&cid, &size)
				if err != nil {
					return dsq.Result{Error: err}, false
				}
				return dsq.Result{Entry: dsq.Entry{Key: store.entryKey(cid), Size: size}}, true
			}

			if q.KeysOnly {
				err := rows.Scan(&cid)
				if err != nil {
					return dsq.Result{Error: err}, false
				}
				return dsq.Result{Entry: dsq.Entry{Key: store.entryKey(cid)}}, true
			}

			err := rows.Scan(&cid, &size, &data, &packStatus, &packObject, &packOffset)
			if err != nil {
				return dsq.Result{Error: err}, false
			}

			entry := dsq.Entry{Key: store.entryKey(cid)}
			if q.ReturnsSizes {
				entry.Size = size
			}

			switch pack.Status(packStatus) {
			case pack.Unpacked, pack.Packing:
				entry.Value = data
			case pack.Packed:
				entry.Value, err = store.packs.ReadBlock(ctx, packObject, packOffset, size)
				if err != nil {
					return dsq.Result{Error: Error.Wrap(err)}, false
				}
			default:
				return dsq.Result{Error: Error.New("unknown pack status: %d", packStatus)}, false
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

func (store *Store) cid(key ds.Key) string {
	return strings.TrimPrefix(key.String(), "/")
}

func (store *Store) entryKey(cid string) string {
	return ds.KeyWithNamespaces([]string{store.mount, cid}).String()
}
