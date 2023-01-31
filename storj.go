// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"context"
	"errors"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	bs "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"storj.io/ipfs-go-ds-storj/block"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/ipfs-go-ds-storj/pack"
	"storj.io/uplink"
)

var mon = monkit.Package()

var log = logging.Logger("storjds")

// Error is the error class for Storj datastore.
var Error = errs.Class("storjds")

type Datastore struct {
	Config
	db      *db.DB
	project *uplink.Project
	packs   *pack.Store
	blocks  *block.Store
	packer  *pack.Chore

	root   context.Context
	cancel context.CancelFunc
	group  *errgroup.Group
}

type Config struct {
	DBURI             string
	AccessGrant       string
	Bucket            string
	PackInterval      time.Duration
	MinPackSize       int
	MaxPackSize       int
	MaxPackBlocks     int
	DebugAddr         string
	UpdateBloomFilter bool
}

func OpenDatastore(ctx context.Context, db *db.DB, conf Config) (*Datastore, error) {
	log.Desugar().Info("New Datastore")

	ds := &Datastore{}
	ds.root, ds.cancel = context.WithCancel(ctx)
	ds.group, ds.root = errgroup.WithContext(ds.root)

	access, err := uplink.ParseAccess(conf.AccessGrant)
	if err != nil {
		return nil, Error.New("failed to parse access grant: %v", err)
	}

	project, err := uplink.Config{
		UserAgent: "ipfs-go-ds-storj",
	}.OpenProject(ctx, access)
	if err != nil {
		return nil, Error.New("failed to open Storj project: %s", err)
	}

	packs := pack.NewStore(project, conf.Bucket)
	blocks := block.NewStore(bs.BlockPrefix.String(), db, packs)
	packer := pack.NewChore(db, packs).
		WithInterval(conf.PackInterval).
		WithPackSize(conf.MinPackSize, conf.MaxPackSize, conf.MaxPackBlocks)

	ds.Config = conf
	ds.db = db
	ds.project = project
	ds.packs = packs
	ds.blocks = blocks
	ds.packer = packer

	ds.group.Go(func() error {
		packer.Run(ctx)
		return nil
	})

	return ds, nil
}

func (storj *Datastore) Close() error {
	log.Desugar().Debug("Close")
	storj.cancel()

	return Error.Wrap(errs.Combine(
		storj.group.Wait(),
		storj.project.Close(),
		storj.packer.Close(),
	))
}

func (storj *Datastore) WithPackInterval(interval time.Duration) *Datastore {
	storj.PackInterval = interval
	storj.packer.WithInterval(interval)
	return storj
}

func (storj *Datastore) WithPackSize(minSize, maxSize, maxBlocks int) *Datastore {
	storj.MinPackSize = minSize
	storj.MaxPackSize = maxSize
	storj.MaxPackBlocks = maxBlocks
	storj.packer.WithPackSize(minSize, maxSize, maxBlocks)
	return storj
}

func (storj *Datastore) TriggerWaitPacker() {
	storj.packer.TriggerWait()
}

func (storj *Datastore) DB() *db.DB {
	return storj.db
}

func (storj *Datastore) Blockstore() *block.Store {
	return storj.blocks
}

func (storj *Datastore) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Put requested", zap.Stringer("Key", key), zap.Int("Bytes", len(value)))
	defer func() {
		if err != nil {
			log.Desugar().Error("Put returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			log.Desugar().Debug("Put returned", zap.Stringer("Key", key))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.Put(ctx, trimFirstNamespace(key), value)
	}

	return storj.db.Put(ctx, key, value)
}

func (storj *Datastore) Sync(ctx context.Context, prefix ds.Key) (err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Sync requested", zap.Stringer("Prefix", prefix))
	defer func() {
		if err != nil {
			log.Desugar().Error("Sync returned error", zap.Stringer("Prefix", prefix), zap.Error(err))
		} else {
			log.Desugar().Debug("Sync returned", zap.Stringer("Prefix", prefix))
		}
	}()

	if prefix.String() == "/" || isBlockKey(prefix) {
		return storj.blocks.Sync(ctx, trimFirstNamespace(prefix))
	}

	return nil
}

func (storj *Datastore) Get(ctx context.Context, key ds.Key) (data []byte, err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Get requested", zap.Stringer("Key", key))
	defer func() {
		if err != nil && !errors.Is(err, ds.ErrNotFound) {
			log.Desugar().Error("Get returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			log.Desugar().Debug("Get returned", zap.Stringer("Key", key), zap.Int("Bytes", len(data)), zap.Error(err))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.Get(ctx, trimFirstNamespace(key))
	}

	return storj.db.Get(ctx, key)
}

func (storj *Datastore) Has(ctx context.Context, key ds.Key) (exists bool, err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Has requested", zap.Stringer("Key", key))
	defer func() {
		if err != nil {
			log.Desugar().Error("Has returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			log.Desugar().Debug("Has returned", zap.Stringer("Key", key), zap.Bool("Exists", exists))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.Has(ctx, trimFirstNamespace(key))
	}

	return storj.db.Has(ctx, key)
}

func (storj *Datastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	defer mon.Task()(&ctx)(&err)

	// This may be too noisy if BloomFilterSize of IPFS config is set to 0.
	// log.Desugar().Debug("GetSize requested", zap.Stringer("Key", key))
	// defer func() {
	// 	if err != nil && !errors.Is(err, ds.ErrNotFound) {
	// 		log.Desugar().Error("GetSize returned error", zap.Stringer("Key", key), zap.Error(err))
	// 	} else {
	// 		log.Desugar().Debug("GetSize returned", zap.Stringer("Key", key), zap.Int("Size", size), zap.Error(err))
	// 	}
	// }()

	if isBlockKey(key) {
		return storj.blocks.GetSize(ctx, trimFirstNamespace(key))
	}

	return storj.db.GetSize(ctx, key)
}

func (storj *Datastore) Delete(ctx context.Context, key ds.Key) (err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Delete requested", zap.Stringer("Key", key))
	defer func() {
		if err != nil {
			log.Desugar().Error("Delete returned error", zap.Stringer("Key", key), zap.Error(err))
		} else {
			log.Desugar().Debug("Delete returned", zap.Stringer("Key", key))
		}
	}()

	if isBlockKey(key) {
		return storj.blocks.Delete(ctx, trimFirstNamespace(key))
	}

	return storj.db.Delete(ctx, key)
}

func (storj *Datastore) Query(ctx context.Context, q dsq.Query) (result dsq.Results, err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Query requested", zap.Stringer("Query", q))
	defer func() {
		if err != nil {
			log.Desugar().Error("Query returned error", zap.Stringer("Query", q), zap.Error(err))
		} else {
			log.Desugar().Debug("Query returned", zap.Stringer("Query", q))
		}
	}()

	if strings.HasPrefix(q.Prefix, bs.BlockPrefix.String()) {
		return storj.blocks.Query(ctx, q)
	}

	return storj.db.QueryDatastore(ctx, q)
}

func (storj *Datastore) Batch(ctx context.Context) (batch ds.Batch, err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Batch")

	return &storjBatch{
		storj: storj,
		ops:   make(map[ds.Key]batchOp),
	}, nil
}

func isBlockKey(key ds.Key) bool {
	return bs.BlockPrefix == key || bs.BlockPrefix.IsAncestorOf(key)
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

func (b *storjBatch) Put(ctx context.Context, key ds.Key, value []byte) (err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("BatchPut", zap.Stringer("Key", key), zap.Int("Bytes", len(value)))

	b.ops[key] = batchOp{
		value:  value,
		delete: false,
	}

	return nil
}

func (b *storjBatch) Delete(ctx context.Context, key ds.Key) (err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("BatchDelete", zap.Stringer("Key", key))

	b.ops[key] = batchOp{
		value:  nil,
		delete: true,
	}

	return nil
}

func (b *storjBatch) Commit(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("BatchCommit")

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
