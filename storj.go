// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"context"
	"os"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/ipfs-go-ds-storj/block"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/ipfs-go-ds-storj/pack"
	"storj.io/uplink"
)

// Error is the error class for Storj datastore.
var Error = errs.Class("storjds")

type Datastore struct {
	Config
	logFile *os.File
	log     *zap.Logger
	db      *db.DB
	project *uplink.Project
	blocks  *block.Store
}

type Config struct {
	DBURI        string
	AccessGrant  string
	Bucket       string
	LogFile      string
	LogLevel     string
	PackInterval time.Duration
	MinPackSize  int
	MaxPackSize  int
}

func NewDatastore(ctx context.Context, log *zap.Logger, db *db.DB, conf Config) (*Datastore, error) {
	log.Info("New Datastore")

	access, err := uplink.ParseAccess(conf.AccessGrant)
	if err != nil {
		return nil, Error.New("failed to parse access grant: %v", err)
	}

	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, Error.New("failed to open Storj project: %s", err)
	}

	packs := pack.NewStore(log, project, conf.Bucket)
	blocks := block.NewStore("/blocks", log, db, packs).
		WithPackInterval(conf.PackInterval).
		WithPackSize(conf.MinPackSize, conf.MaxPackSize)

	return &Datastore{
		Config:  conf,
		log:     log,
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
	storj.log.Debug("Put requested", zap.Stringer("Key", key), zap.Int("Bytes", len(value)))
	defer func() {
		if err != nil {
			storj.log.Error("Put returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			storj.log.Debug("Put returned", zap.Stringer("Key", key))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.Put(ctx, trimFirstNamespace(key), value)
	}

	return storj.db.Put(ctx, key, value)
}

func (storj *Datastore) Sync(ctx context.Context, prefix ds.Key) (err error) {
	storj.log.Debug("Sync requested", zap.Stringer("Prefix", prefix))
	defer func() {
		if err != nil {
			storj.log.Error("Sync returned error", zap.Stringer("Prefix", prefix), zap.Error(err))
		} else {
			storj.log.Debug("Sync returned", zap.Stringer("Prefix", prefix))
		}
	}()

	if prefix.String() == "/" || isBlockKey(prefix) {
		return storj.blocks.Sync(ctx, trimFirstNamespace(prefix))
	}

	return nil
}

func (storj *Datastore) Get(ctx context.Context, key ds.Key) (data []byte, err error) {
	storj.log.Debug("Get requested", zap.Stringer("Key", key))
	defer func() {
		if err != nil {
			storj.log.Error("Get returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			storj.log.Debug("Get returned", zap.Stringer("Key", key), zap.Int("Bytes", len(data)))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.Get(ctx, trimFirstNamespace(key))
	}

	return storj.db.Get(ctx, key)
}

func (storj *Datastore) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	storj.log.Debug("Has requested", zap.Stringer("Key", key))
	defer func() {
		if err != nil {
			storj.log.Error("Has returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			storj.log.Debug("Has returned", zap.Stringer("Key", key), zap.Bool("Exists", exists))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.Has(ctx, trimFirstNamespace(key))
	}

	return storj.db.Has(ctx, key)
}

func (storj *Datastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	// This may be too noisy if BloomFilterSize of IPFS config is set to 0.
	storj.log.Debug("GetSize requested", zap.Stringer("Key", key))
	defer func() {
		if err != nil {
			storj.log.Error("GetSize returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			storj.log.Debug("GetSize returned", zap.Stringer("Key", key), zap.Int("Size", size))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.GetSize(ctx, trimFirstNamespace(key))
	}

	return storj.db.GetSize(ctx, key)
}

func (storj *Datastore) Delete(ctx context.Context, key ds.Key) (err error) {
	storj.log.Debug("Delete requested", zap.Stringer("Key", key))
	defer func() {
		if err != nil {
			storj.log.Error("Delete returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			storj.log.Debug("Delete returned", zap.Stringer("Key", key))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.Delete(ctx, trimFirstNamespace(key))
	}

	return storj.db.Delete(ctx, key)
}

func (storj *Datastore) Query(ctx context.Context, q dsq.Query) (result dsq.Results, err error) {
	storj.log.Debug("Query requested", zap.Stringer("Query", q))
	defer func() {
		if err != nil {
			storj.log.Error("Query returned error", zap.Stringer("Query", q), zap.Error(err))
		} else {
			storj.log.Debug("Query returned", zap.Stringer("Query", q))
		}
	}()

	if strings.HasPrefix(q.Prefix, "/blocks") {
		return storj.blocks.Query(ctx, q)
	}

	return storj.db.QueryDatastore(ctx, q)
}

func (storj *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	storj.log.Debug("Batch")

	return &storjBatch{
		storj: storj,
		ops:   make(map[ds.Key]batchOp),
	}, nil
}

func (storj *Datastore) Close() error {
	storj.log.Debug("Close")

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
	b.storj.log.Debug("BatchPut", zap.Stringer("Key", key), zap.Int("Bytes", len(value)))

	b.ops[key] = batchOp{
		value:  value,
		delete: false,
	}

	return nil
}

func (b *storjBatch) Delete(ctx context.Context, key ds.Key) error {
	b.storj.log.Debug("BatchDelete", zap.Stringer("Key", key))

	b.ops[key] = batchOp{
		value:  nil,
		delete: true,
	}

	return nil
}

func (b *storjBatch) Commit(ctx context.Context) error {
	b.storj.log.Debug("BatchCommit")

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
