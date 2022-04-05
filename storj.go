// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/block"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/db"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/logger"
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
	blocks  *block.Store
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
	logger := logger.Default
	var logFile *os.File

	if len(conf.LogFile) > 0 {
		var err error
		logFile, err = os.OpenFile(conf.LogFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, Error.New("failed to create log file: %v", err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
	}

	db = db.WithLogger(logger)

	logger.Println("NewStorjDatastore")

	access, err := uplink.ParseAccess(conf.AccessGrant)
	if err != nil {
		return nil, Error.New("failed to parse access grant: %v", err)
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, Error.New("failed to open Storj project: %s", err)
	}

	packs := pack.NewStore(logger, project, conf.Bucket)
	blocks := block.NewStore("/blocks", logger, db, packs).
		WithPackInterval(conf.PackInterval).
		WithPackSize(conf.MinPackSize, conf.MaxPackSize)

	return &Datastore{
		Config:  conf,
		logFile: logFile,
		logger:  logger,
		db:      db,
		project: project,
		blocks:  blocks,
	}, nil
}

func (storj *Datastore) WithPackInterval(interval time.Duration) *Datastore {
	storj.blocks.WithPackInterval(interval)
	return storj
}

func (storj *Datastore) WithPackSize(min, max int) *Datastore {
	storj.blocks.WithPackSize(min, max)
	return storj
}

func (storj *Datastore) TriggerWaitPacker() {
	storj.blocks.TriggerWaitPacker()
}

func (storj *Datastore) DB() *db.DB {
	return storj.db
}

func (storj *Datastore) Blockstore() *block.Store {
	return storj.blocks
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

	if isBlockKey(key) {
		return storj.blocks.Put(ctx, trimFirstNamespace(key), value)
	}

	return storj.db.Put(ctx, key, value)
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

	if prefix.String() == "/" || isBlockKey(prefix) {
		return storj.blocks.Sync(ctx, trimFirstNamespace(prefix))
	}

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

	if isBlockKey(key) {
		return storj.blocks.Get(ctx, trimFirstNamespace(key))
	}

	return storj.db.Get(ctx, key)
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

	if isBlockKey(key) {
		return storj.blocks.Has(ctx, trimFirstNamespace(key))
	}

	return storj.db.Has(ctx, key)
}

func (storj *Datastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	// This may be too noisy if BloomFilterSize of IPFS config is set to 0.
	// storj.logger.Printf("GetSize requested for key %s\n", key)
	// defer func() {
	// 	if err == nil {
	// 		storj.logger.Printf("GetSize for key %s returned: %d\n", key, size)
	// 	} else {
	// 		storj.logger.Printf("GetSize for key %s returned error: %v\n", key, err)
	// 	}
	// }()

	if isBlockKey(key) {
		return storj.blocks.GetSize(ctx, trimFirstNamespace(key))
	}

	return storj.db.GetSize(ctx, key)
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

	if isBlockKey(key) {
		return storj.blocks.Delete(ctx, trimFirstNamespace(key))
	}

	return storj.db.Delete(ctx, key)
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

	if strings.HasPrefix(q.Prefix, "/blocks") {
		return storj.blocks.Query(ctx, q)
	}

	return storj.db.QueryDatastore(ctx, q)
}

func (storj *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	storj.logger.Println("Batch")

	return &storjBatch{
		storj: storj,
		ops:   make(map[ds.Key]batchOp),
	}, nil
}

func (storj *Datastore) Close() error {
	storj.logger.Println("Close")

	err := errs.Combine(
		storj.project.Close(),
		storj.blocks.Close(),
		storj.db.Close(),
	)

	if storj.logFile != nil {
		err = errs.Combine(err, storj.logFile.Close())
	}

	return Error.Wrap(err)
}

func isBlockKey(key ds.Key) bool {
	ns := key.Namespaces()
	if len(ns) < 1 {
		return false
	}
	return ns[0] == "blocks"
}

func trimFirstNamespace(key ds.Key) ds.Key {
	ns := key.Namespaces()
	if len(ns) < 1 {
		return key
	}
	return ds.KeyWithNamespaces(ns[1:])
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
