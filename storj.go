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
	"github.com/kaloyan-raev/ipfs-go-ds-storj/dbx"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/pack"
	"github.com/zeebo/errs"

	"storj.io/uplink"
)

type StorjDS struct {
	Config
	logFile *os.File
	logger  *log.Logger
	db      *dbx.DB
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
	logger := log.New(os.Stdout, "", log.LstdFlags) // default stdout logger
	var logFile *os.File

	if len(conf.LogFile) > 0 {
		var err error
		logFile, err = os.OpenFile(conf.LogFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to create log file: %s", err)
		}
		logger = log.New(logFile, "", log.LstdFlags)
	}

	logger.Println("NewStorjDatastore")

	if len(conf.DBPath) == 0 {
		conf.DBPath = "cache.db"
	}
	db, err := dbx.Open("sqlite3", conf.DBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open cache database: %s", err)
	}

	_, err = db.ExecContext(context.Background(), db.Schema())
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

func (storj *StorjDS) DB() *dbx.DB {
	return storj.db
}

func (storj *StorjDS) Put(key ds.Key, value []byte) error {
	storj.logger.Printf("Put --- key: %s --- bytes: %d\n", key, len(value))

	_, err := storj.db.Create_Block(context.Background(),
		dbx.Block_Cid(storjKey(key)),
		dbx.Block_Size(len(value)),
		dbx.Block_Create_Fields{
			Data: dbx.Block_Data(value),
		},
	)

	return err
}

func (storj *StorjDS) Sync(prefix ds.Key) error {
	storj.logger.Printf("Sync --- prefix: %s\n", prefix)

	storj.packer.Run(context.Background())

	return nil
}

func (storj *StorjDS) Get(key ds.Key) (data []byte, err error) {
	storj.logger.Printf("Get --- key: %s\n", key)

	ctx := context.Background()

	block, err := storj.db.Get_Block_By_Cid(ctx, dbx.Block_Cid(storjKey(key)))
	if err != nil {
		if isNotFound(err) {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}

	switch pack.Status(block.PackStatus) {
	case pack.Unpacked:
		return block.Data, nil
	case pack.Packing:
		if block.Deleted {
			return nil, ds.ErrNotFound
		}
		return block.Data, nil
	case pack.Packed:
		if block.Deleted {
			return nil, ds.ErrNotFound
		}

		download, err := storj.project.DownloadObject(ctx, storj.Bucket, block.PackObject, &uplink.DownloadOptions{
			Offset: int64(block.PackOffset),
			Length: int64(block.Size),
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
	default:
		return nil, fmt.Errorf("unknown pack status: %d", block.PackStatus)
	}
}

func (storj *StorjDS) Has(key ds.Key) (exists bool, err error) {
	storj.logger.Printf("Has --- key: %s\n", key)

	found, err := storj.db.Has_Block_By_Cid(context.Background(), dbx.Block_Cid(storjKey(key)))
	if err != nil {
		return false, err
	}

	return found, nil
}

func (storj *StorjDS) GetSize(key ds.Key) (size int, err error) {
	// Commented because this method is invoked very often and it is noisy.
	// storj.logger.Printf("GetSize --- key: %s\n", key)

	block, err := storj.db.Get_Block_Size_By_Cid(context.Background(), dbx.Block_Cid(storjKey(key)))
	if err != nil {
		if isNotFound(err) {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}

	return block.Size, nil
}

func (storj *StorjDS) Delete(key ds.Key) error {
	storj.logger.Printf("Delete --- key: %s\n", key)

	_, err := storj.db.Delete_Block_By_Cid(context.Background(), dbx.Block_Cid(storjKey(key)))
	if err != nil {
		return err
	}

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
	query := "SELECT cid, size, data FROM blocks"
	if len(q.Prefix) > 0 {
		query += fmt.Sprintf(" WHERE key LIKE '%s%%' ORDER BY key", q.Prefix)
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
				key  string
				size int
				data []byte
			)

			err := rows.Scan(&key, &size, &data)
			if err != nil {
				return dsq.Result{Error: err}, false
			}

			entry := dsq.Entry{Key: "/" + key}

			if !q.KeysOnly {
				// TODO: optimize to not read this column from DB
				entry.Value = data
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
