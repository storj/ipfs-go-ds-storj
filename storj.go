// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/db"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/pack"
	"github.com/zeebo/errs"

	"storj.io/uplink"
)

// Error is the error class for Storj datastore.
var Error = errs.Class("storjds")

type Datastore struct {
	Config
	logFile *os.File
	logger  *log.Logger
	db      *db.DB
	project *uplink.Project
	packer  *pack.Chore
}

type Config struct {
	DBURI        string
	AccessGrant  string
	Bucket       string
	LogFile      string
	PackInterval time.Duration
	MinPackSize  int
	MaxPackSize  int
}

func NewDatastore(ctx context.Context, conf Config, db *db.DB) (*Datastore, error) {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds) // default stdout logger
	var logFile *os.File

	if len(conf.LogFile) > 0 {
		var err error
		logFile, err = os.OpenFile(conf.LogFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, Error.New("failed to create log file: %v", err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
	}

	logger.Println("NewStorjDatastore")

	access, err := uplink.ParseAccess(conf.AccessGrant)
	if err != nil {
		return nil, Error.New("failed to parse access grant: %v", err)
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, Error.New("failed to open Storj project: %s", err)
	}

	return &Datastore{
		Config:  conf,
		logFile: logFile,
		logger:  logger,
		db:      db,
		project: project,
		packer:  pack.NewChore(logger, db, project, conf.Bucket).WithInterval(conf.PackInterval).WithPackSize(conf.MinPackSize, conf.MaxPackSize),
	}, nil
}

func (storj *Datastore) WithInterval(interval time.Duration) *Datastore {
	storj.packer.WithInterval(interval)
	return storj
}

func (storj *Datastore) WithPackSize(min, max int) *Datastore {
	storj.packer.WithPackSize(min, max)
	return storj
}

func (storj *Datastore) DB() *db.DB {
	return storj.db
}

func (storj *Datastore) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	storj.logger.Printf("Put requested for key %s and data of %d bytes\n", key, len(value))
	defer func() {
		if err == nil {
			storj.logger.Printf("Put for key %s returned\n", key)
		} else {
			storj.logger.Printf("Put for key %s returned error: %v\n", key, err)
		}
	}()

	result, err := storj.db.Exec(ctx, `
		INSERT INTO blocks (cid, size, data)
		VALUES ($1, $2, $3)
		ON CONFLICT(cid)
		DO UPDATE SET deleted = false
	`, storjKey(key), len(value), value)
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

func (storj *Datastore) Sync(ctx context.Context, prefix ds.Key) (err error) {
	storj.logger.Printf("Sync requested for prefix '%s'\n", prefix)
	defer func() {
		if err == nil {
			storj.logger.Printf("Sync for prefix '%s' returned\n", prefix)
		} else {
			storj.logger.Printf("Sync for prefix '%s' returned error: %v\n", prefix, err)
		}
	}()

	storj.packer.Run(ctx)

	return nil
}

func (storj *Datastore) Get(ctx context.Context, key ds.Key) (data []byte, err error) {
	storj.logger.Printf("Get requested for key %s\n", key)
	defer func() {
		if err == nil {
			storj.logger.Printf("Get for key %s returned %d bytes of data\n", key, len(data))
		} else {
			storj.logger.Printf("Get for key %s returned error: %v\n", key, err)
		}
	}()

	block, err := storj.GetBlock(ctx, key)
	if err != nil {
		// do not wrap error to avoid wrapping ds.ErrNotFound
		return nil, err
	}

	if block.Deleted {
		return nil, ds.ErrNotFound
	}

	switch pack.Status(block.PackStatus) {
	case pack.Unpacked, pack.Packing:
		return block.Data, nil
	case pack.Packed:
		return storj.readDataFromPack(ctx, block.PackObject, block.PackOffset, block.Size)
	default:
		return nil, Error.New("unknown pack status: %d", block.PackStatus)
	}
}

func (storj *Datastore) readDataFromPack(ctx context.Context, packObject string, packOffset, size int) ([]byte, error) {
	download, err := storj.project.DownloadObject(ctx, storj.Bucket, packObject, &uplink.DownloadOptions{
		Offset: int64(packOffset),
		Length: int64(size),
	})
	if err != nil {
		return nil, Error.Wrap(err)
	}
	defer func() {
		err = errs.Combine(err, download.Close())
	}()

	data, err := ioutil.ReadAll(download)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return data, nil
}

func (storj *Datastore) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	storj.logger.Printf("Has requested for key %s\n", key)
	defer func() {
		if err == nil {
			storj.logger.Printf("Has for key %s returned: %t\n", key, exists)
		} else {
			storj.logger.Printf("Has for key %s returned error: %v\n", key, err)
		}
	}()

	var deleted bool
	err = storj.db.QueryRow(ctx, `
		SELECT deleted
		FROM blocks
		WHERE cid = $1
	`, storjKey(key)).Scan(
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

func (storj *Datastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	// This may be too noisy if BloomFilterSize of IPFS config is set to 0.
	storj.logger.Printf("GetSize requested for key %s\n", key)
	defer func() {
		if err == nil {
			storj.logger.Printf("GetSize for key %s returned: %d\n", key, size)
		} else {
			storj.logger.Printf("GetSize for key %s returned error: %v\n", key, err)
		}
	}()

	var deleted bool
	err = storj.db.QueryRow(ctx, `
		SELECT size, deleted
		FROM blocks
		WHERE cid = $1
	`, storjKey(key)).Scan(
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

func (storj *Datastore) Delete(ctx context.Context, key ds.Key) (err error) {
	storj.logger.Printf("Delete requested for key %s\n", key)
	defer func() {
		if err == nil {
			storj.logger.Printf("Delete for key %s returned\n", key)
		} else {
			storj.logger.Printf("Delete for key %s returned error: %v\n", key, err)
		}
	}()

	tx, err := storj.db.BeginTx(ctx, nil)
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

	cid := storjKey(key)

	_, err = tx.Exec(ctx, `
		DELETE FROM blocks
		WHERE
			cid = $1 AND
			pack_status = 0;
	`, cid)

	_, err = tx.Exec(ctx, `
		UPDATE blocks
		SET deleted = true
		WHERE
			cid = $1 AND
			pack_status > 0;
	`, cid)

	return Error.Wrap(err)
}

func (storj *Datastore) Query(ctx context.Context, q dsq.Query) (result dsq.Results, err error) {
	storj.logger.Printf("Query requested: %s\n", q)
	defer func() {
		if err == nil {
			storj.logger.Println("Query returned")
		} else {
			storj.logger.Printf("Query returned error: %v\n", err)
		}
	}()

	// TODO: implement orders and filters
	if q.Orders != nil || q.Filters != nil {
		return nil, Error.New("filters or orders are not supported")
	}

	// Storj stores a "/foo" key as "foo" so we need to trim the leading "/"
	q.Prefix = strings.TrimPrefix(q.Prefix, "/")

	// TODO: optimize with prepared statements
	query := "SELECT cid, size, data, pack_status, pack_object, pack_offset FROM blocks"
	if len(q.Prefix) > 0 {
		query += fmt.Sprintf(" WHERE key LIKE '%s%%' AND deleted = false ORDER BY key", q.Prefix)
	} else {
		query += " WHERE deleted = false"
	}
	if q.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", q.Limit)
	}
	if q.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", q.Offset)
	}

	rows, err := storj.db.Query(ctx, query)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			storj.logger.Println("Query closed")
			return nil
		},
		Next: func() (dsq.Result, bool) {
			if !rows.Next() {
				storj.logger.Println("Query completed")
				return dsq.Result{}, false
			}

			var (
				key        string
				size       int
				data       []byte
				packStatus int
				packObject string
				packOffset int
			)

			err := rows.Scan(&key, &size, &data, &packStatus, &packObject, &packOffset)
			if err != nil {
				return dsq.Result{Error: Error.Wrap(err)}, false
			}

			entry := dsq.Entry{Key: "/" + key}

			if !q.KeysOnly {
				switch pack.Status(packStatus) {
				case pack.Unpacked, pack.Packing:
					// TODO: optimize to not read this column from DB if keys only
					entry.Value = data
				case pack.Packed:
					entry.Value, err = storj.readDataFromPack(ctx, packObject, packOffset, size)
					if err != nil {
						return dsq.Result{Error: Error.Wrap(err)}, false
					}
				default:
					return dsq.Result{Error: Error.New("unknown pack status: %d", packStatus)}, false
				}
			}
			if q.ReturnsSizes {
				// TODO: optimize to not read this column from DB
				entry.Size = size
			}

			return dsq.Result{Entry: entry}, true
		},
	}), nil
}

func (storj *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	storj.logger.Println("Batch")

	return &storjBatch{
		storj: storj,
		ops:   make(map[ds.Key]batchOp),
	}, nil
}

func (storj *Datastore) TriggerWaitPacker() {
	storj.packer.TriggerWait()
}

func (storj *Datastore) Close() error {
	storj.logger.Println("Close")

	err := errs.Combine(
		storj.packer.Close(),
		storj.project.Close(),
		storj.db.Close(),
	)

	if storj.logFile != nil {
		err = errs.Combine(err, storj.logFile.Close())
	}

	return Error.Wrap(err)
}

type Block struct {
	CID        string
	Size       int
	Data       []byte
	Deleted    bool
	PackStatus int
	PackObject string
	PackOffset int
}

func (storj *Datastore) GetBlock(ctx context.Context, key ds.Key) (*Block, error) {
	cid := storjKey(key)

	block := Block{
		CID: cid,
	}

	err := storj.db.QueryRow(ctx, `
		SELECT
			size, data, deleted,
			pack_status, pack_object, pack_offset
		FROM blocks
		WHERE cid = $1
	`, cid).Scan(
		&block.Size, &block.Data, &block.Deleted,
		&block.PackStatus, &block.PackObject, &block.PackOffset,
	)
	if err != nil {
		if isNotFound(err) {
			return nil, ds.ErrNotFound
		}
		return nil, Error.Wrap(err)
	}

	return &block, nil
}

func storjKey(ipfsKey ds.Key) string {
	return strings.TrimPrefix(ipfsKey.String(), "/")
}

func isNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

type storjBatch struct {
	storj *Datastore
	ops   map[ds.Key]batchOp
}

type batchOp struct {
	value  []byte
	delete bool
}

func (b *storjBatch) Put(ctx context.Context, key ds.Key, value []byte) error {
	b.storj.logger.Printf("BatchPut --- key: %s --- bytes: %d\n", key, len(value))

	b.ops[key] = batchOp{
		value:  value,
		delete: false,
	}

	return nil
}

func (b *storjBatch) Delete(ctx context.Context, key ds.Key) error {
	b.storj.logger.Printf("BatchDelete --- key: %s\n", key)

	b.ops[key] = batchOp{
		value:  nil,
		delete: true,
	}

	return nil
}

func (b *storjBatch) Commit(ctx context.Context) error {
	b.storj.logger.Println("BatchCommit")

	for key, op := range b.ops {
		var err error
		if op.delete {
			err = b.storj.Delete(ctx, key)
		} else {
			err = b.storj.Put(ctx, key, op.value)
		}
		if err != nil {
			return Error.Wrap(err)
		}
	}

	return nil
}

var _ ds.Batching = (*Datastore)(nil)
