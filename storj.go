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
	"github.com/kaloyan-raev/ipfs-go-ds-storj/pack"
	"github.com/zeebo/errs"

	"storj.io/uplink"
)

type StorjDS struct {
	Config
	logFile *os.File
	logger  *log.Logger
	db      *sql.DB
	project *uplink.Project
	packer  *pack.Chore
}

type Config struct {
	DBPath       string
	AccessGrant  string
	Bucket       string
	LogFile      string
	PackInterval time.Duration
	MinPackSize  int
	MaxPackSize  int
}

func NewStorjDatastore(conf Config) (*StorjDS, error) {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds) // default stdout logger
	var logFile *os.File

	if len(conf.LogFile) > 0 {
		var err error
		logFile, err = os.OpenFile(conf.LogFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to create log file: %s", err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
	}

	logger.Println("NewStorjDatastore")

	if len(conf.DBPath) == 0 {
		conf.DBPath = "cache.db"
	}
	db, err := sql.Open("sqlite3", conf.DBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open cache database: %s", err)
	}

	// Avoid "database is locked" errors when sqlite db is accessed concurrently.
	db.SetMaxOpenConns(1)

	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE blocks (
			cid TEXT NOT NULL,
			size INTEGER NOT NULL,
			created TIMESTAMP NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
			data BLOB,
			deleted INTEGER NOT NULL DEFAULT false,
			pack_object TEXT NOT NULL DEFAULT "",
			pack_offset INTEGER NOT NULL DEFAULT 0,
			pack_status INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY ( cid )
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache database schema: %s", err)
	}

	access, err := uplink.ParseAccess(conf.AccessGrant)
	if err != nil {
		return nil, fmt.Errorf("failed to parse access grant: %s", err)
	}

	project, err := uplink.OpenProject(context.Background(), access)
	if err != nil {
		return nil, fmt.Errorf("failed to open Storj project: %s", err)
	}

	return &StorjDS{
		Config:  conf,
		logFile: logFile,
		logger:  logger,
		db:      db,
		project: project,
		packer:  pack.NewChore(logger, db, project, conf.Bucket).WithInterval(conf.PackInterval).WithPackSize(conf.MinPackSize, conf.MaxPackSize),
	}, nil
}

func (storj *StorjDS) DB() *sql.DB {
	return storj.db
}

func (storj *StorjDS) Put(key ds.Key, value []byte) error {
	storj.logger.Printf("Put --- key: %s --- bytes: %d\n", key, len(value))

	result, err := storj.db.ExecContext(context.Background(), `
		INSERT INTO blocks (cid, size, data)
		VALUES ($1, $2, $3)
		ON CONFLICT(cid)
		DO UPDATE SET deleted = false
	`, storjKey(key), len(value), value)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected != 1 {
		return fmt.Errorf("expected 1 row inserted in db, but did %d", affected)
	}

	return nil
}

func (storj *StorjDS) Sync(prefix ds.Key) error {
	storj.logger.Printf("Sync --- prefix: %s\n", prefix)

	storj.packer.Run(context.Background())

	return nil
}

func (storj *StorjDS) Get(key ds.Key) (data []byte, err error) {
	storj.logger.Printf("Get --- key: %s\n", key)

	ctx := context.Background()

	block, err := storj.GetBlock(ctx, key)
	if err != nil {
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
		return nil, fmt.Errorf("unknown pack status: %d", block.PackStatus)
	}
}

func (storj *StorjDS) readDataFromPack(ctx context.Context, packObject string, packOffset, size int) ([]byte, error) {
	download, err := storj.project.DownloadObject(ctx, storj.Bucket, packObject, &uplink.DownloadOptions{
		Offset: int64(packOffset),
		Length: int64(size),
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errs.Combine(err, download.Close())
	}()

	data, err := ioutil.ReadAll(download)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (storj *StorjDS) Has(key ds.Key) (exists bool, err error) {
	storj.logger.Printf("Has --- key: %s\n", key)

	var deleted bool
	err = storj.db.QueryRowContext(context.Background(), `
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
		return false, err
	}

	return !deleted, nil
}

func (storj *StorjDS) GetSize(key ds.Key) (size int, err error) {
	// Commented because this method is invoked very often and it is noisy.
	// storj.logger.Printf("GetSize --- key: %s\n", key)

	var deleted bool
	err = storj.db.QueryRowContext(context.Background(), `
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
		return -1, err
	}

	if deleted {
		return -1, ds.ErrNotFound
	}

	return size, nil
}

func (storj *StorjDS) Delete(key ds.Key) error {
	storj.logger.Printf("Delete --- key: %s\n", key)

	cid := storjKey(key)

	_, err := storj.db.ExecContext(context.Background(), `
		DELETE FROM blocks
		WHERE
			cid = $1 AND
			pack_status = 0;

		UPDATE blocks
		SET deleted = true
		WHERE
			cid = $2 AND
			pack_status > 0;
	`, cid, cid)

	return err
}

func (storj *StorjDS) Query(q dsq.Query) (dsq.Results, error) {
	storj.logger.Printf("Query --- %s\n", q)

	// TODO: implement orders and filters
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("storjds: filters or orders are not supported")
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

	rows, err := storj.db.Query(query)
	if err != nil {
		return nil, err
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			return nil
		},
		Next: func() (dsq.Result, bool) {
			if !rows.Next() {
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
				return dsq.Result{Error: err}, false
			}

			entry := dsq.Entry{Key: "/" + key}

			if !q.KeysOnly {
				switch pack.Status(packStatus) {
				case pack.Unpacked, pack.Packing:
					// TODO: optimize to not read this column from DB if keys only
					entry.Value = data
				case pack.Packed:
					entry.Value, err = storj.readDataFromPack(context.Background(), packObject, packOffset, size)
					if err != nil {
						return dsq.Result{Error: err}, false
					}
				default:
					return dsq.Result{Error: fmt.Errorf("unknown pack status: %d", packStatus)}, false
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

func (storj *StorjDS) Batch() (ds.Batch, error) {
	storj.logger.Println("Batch")

	return &storjBatch{
		storj: storj,
		ops:   make(map[ds.Key]batchOp),
	}, nil
}

func (storj *StorjDS) TriggerWaitPacker() {
	storj.packer.TriggerWait()
}

func (storj *StorjDS) Close() error {
	storj.logger.Println("Close")

	err := errs.Combine(
		storj.packer.Close(),
		storj.project.Close(),
		storj.db.Close(),
	)

	if storj.logFile != nil {
		err = errs.Combine(err, storj.logFile.Close())
	}

	return err
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

func (storj *StorjDS) GetBlock(ctx context.Context, key ds.Key) (*Block, error) {
	cid := storjKey(key)

	block := Block{
		CID: cid,
	}

	err := storj.db.QueryRowContext(ctx, `
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
		return nil, err
	}

	return &block, nil
}

func storjKey(ipfsKey ds.Key) string {
	return strings.TrimPrefix(ipfsKey.String(), "/")
}

func isNotFound(err error) bool {
	return errors.Is(err, uplink.ErrObjectNotFound) || errors.Is(err, sql.ErrNoRows)
}

type storjBatch struct {
	storj *StorjDS
	ops   map[ds.Key]batchOp
}

type batchOp struct {
	value  []byte
	delete bool
}

func (b *storjBatch) Put(key ds.Key, value []byte) error {
	b.storj.logger.Printf("BatchPut --- key: %s --- bytes: %d\n", key, len(value))

	b.ops[key] = batchOp{
		value:  value,
		delete: false,
	}

	return nil
}

func (b *storjBatch) Delete(key ds.Key) error {
	b.storj.logger.Printf("BatchDelete --- key: %s\n", key)

	b.ops[key] = batchOp{
		value:  nil,
		delete: true,
	}

	return nil
}

func (b *storjBatch) Commit() error {
	b.storj.logger.Println("BatchCommit")

	for key, op := range b.ops {
		var err error
		if op.delete {
			err = b.storj.Delete(key)
		} else {
			err = b.storj.Put(key, op.value)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

var _ ds.Batching = (*StorjDS)(nil)
