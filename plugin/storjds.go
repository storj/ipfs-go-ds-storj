// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package plugin

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/ipfs/bbloom"
	ds "github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	logging "github.com/ipfs/go-log/v2"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	storjds "storj.io/ipfs-go-ds-storj"
	"storj.io/ipfs-go-ds-storj/bloom"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/private/dbutil"
	"storj.io/private/debug"
)

var _ plugin.PluginDatastore = (*StorjPlugin)(nil)
var _ plugin.PluginDaemonInternal = (*StorjPlugin)(nil)

var log = logging.Logger("storjds").Named("plugin")

// Error is the error class for Storj datastore plugin.
var Error = errs.Class("storjds")

var Plugins = []plugin.Plugin{
	&StorjPlugin{},
}

type StorjPlugin struct {
	root   context.Context
	cancel context.CancelFunc
	group  *errgroup.Group
}

func (plugin StorjPlugin) Name() string {
	return "storj-datastore-plugin"
}

func (plugin StorjPlugin) Version() string {
	return "0.3.0"
}

func (plugin StorjPlugin) Init(env *plugin.Environment) error {
	return nil
}

func (plugin StorjPlugin) DatastoreTypeName() string {
	return "storjds"
}

func (plugin StorjPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		dbURI, ok := m["dbURI"].(string)
		if !ok || len(dbURI) == 0 {
			return nil, Error.New("no dbURI specified")
		}

		bucket, ok := m["bucket"].(string)
		if !ok || len(bucket) == 0 {
			return nil, Error.New("no bucket specified")
		}

		accessGrant, ok := m["accessGrant"].(string)
		if !ok || len(accessGrant) == 0 {
			return nil, Error.New("no accessGrant specified")
		}

		// Optional.

		var packInterval time.Duration
		if v, ok := m["packInterval"]; ok {
			interval, ok := v.(string)
			if !ok {
				return nil, Error.New("packInterval not a string")
			}
			var err error
			packInterval, err = time.ParseDuration(interval)
			if err != nil {
				return nil, Error.New("packInterval not a duration: %v", err)
			}
		}

		var debugAddr string
		if v, ok := m["debugAddr"]; ok {
			debugAddr, ok = v.(string)
			if !ok {
				return nil, Error.New("debugAddr not a string")
			}
		}

		var updateBloomFilter bool
		if v, ok := m["updateBloomFilter"]; ok {
			updateFlag, ok := v.(string)
			if !ok {
				return nil, Error.New("updateBloomFilter not a string")
			}
			var err error
			updateBloomFilter, err = strconv.ParseBool(updateFlag)
			if err != nil {
				return nil, Error.New("updateBloomFilter not a boolean: %v", err)
			}
		}

		return &StorjConfig{
			cfg: storjds.Config{
				DBURI:             dbURI,
				Bucket:            bucket,
				AccessGrant:       accessGrant,
				PackInterval:      packInterval,
				DebugAddr:         debugAddr,
				UpdateBloomFilter: updateBloomFilter,
			},
		}, nil
	}
}

func (plugin *StorjPlugin) Start(node *core.IpfsNode) error {
	log.Desugar().Debug("Start")

	plugin.root, plugin.cancel = context.WithCancel(node.Context())
	plugin.group, plugin.root = errgroup.WithContext(plugin.root)

	repoCfg, err := node.Repo.Config()
	if err != nil {
		return Error.Wrap(err)
	}

	storjCfg := lookupStorjDatastoreSpec(repoCfg.Datastore.Spec)
	if storjCfg == nil {
		return Error.New("storj datastore spec not found")
	}

	cfg, err := fsrepo.AnyDatastoreConfig(storjCfg)
	if err != nil {
		return Error.Wrap(err)
	}

	storj, ok := cfg.(*StorjConfig)
	if !ok {
		return Error.New("storj datastore spec is not of type *StorjConfig")
	}

	plugin.group.Go(func() error {
		return storj.RunDebug(plugin.root)
	})

	if repoCfg.Datastore.BloomFilterSize <= 0 {
		log.Desugar().Debug("Bloom filter disabled")
		if storj.cfg.UpdateBloomFilter {
			return Error.New("bloom filter updater is enabled, but the bloom filter itself is disabled")
		}
		return nil
	}

	if !storj.cfg.UpdateBloomFilter {
		log.Desugar().Debug("Bloom filter updater disabled")
		return nil
	}

	bloomFilter := getBloomFilter(node.BaseBlocks)
	if bloomFilter == nil {
		return Error.New("bloom filter not found")
	}

	_, _, impl, err := dbutil.SplitConnStr(storj.cfg.DBURI)
	if err != nil {
		return Error.Wrap(err)
	}

	if impl != dbutil.Cockroach {
		return Error.New("bloom filter updater is not supported for %s", impl)
	}

	bloomUpdater := bloom.NewUpdater(storj.cfg.DBURI, bloomFilter)
	plugin.group.Go(func() error {
		bloomUpdater.Run(plugin.root)
		return nil
	})

	return nil
}

func lookupStorjDatastoreSpec(spec map[string]interface{}) map[string]interface{} {
	mounts, ok := spec["mounts"].([]interface{})
	if !ok {
		return nil
	}

	for _, iface := range mounts {
		mount, ok := iface.(map[string]interface{})
		if !ok {
			return nil
		}

		storjds := lookupStorjDatastoreSpecFromMount(mount)
		if storjds != nil {
			return storjds
		}
	}

	return nil
}

func lookupStorjDatastoreSpecFromMount(mount map[string]interface{}) map[string]interface{} {
	which, ok := mount["type"].(string)
	if !ok {
		return nil
	}

	if which == "storjds" {
		return mount
	}

	child, ok := mount["child"].(map[string]interface{})
	if !ok {
		return nil
	}

	return lookupStorjDatastoreSpecFromMount(child)
}

func (plugin *StorjPlugin) Close() error {
	log.Desugar().Debug("Close")

	plugin.cancel()
	err := plugin.group.Wait()
	return err
}

type StorjConfig struct {
	cfg storjds.Config
}

func (storj *StorjConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{
		"bucket": storj.cfg.Bucket,
	}
}

type DatastoreProcess struct {
	*storjds.Datastore
	DB *db.DB

	root   context.Context
	cancel context.CancelFunc
	group  *errgroup.Group
}

func OpenProcess(ctx context.Context, cfg storjds.Config) (*DatastoreProcess, error) {
	proc := &DatastoreProcess{}
	proc.root, proc.cancel = context.WithCancel(ctx)
	proc.group, proc.root = errgroup.WithContext(proc.root)

	db, err := db.Open(ctx, cfg.DBURI)
	if err != nil {
		return nil, Error.New("failed to connect to cache database: %s", err)
	}
	proc.DB = db

	err = db.MigrateToLatest(ctx)
	if err != nil {
		_ = db.Close()
		return nil, Error.New("failed to migrate database schema: %w", err)
	}

	datastore, err := storjds.OpenDatastore(ctx, db, cfg)
	if err != nil {
		_ = db.Close()
		return nil, Error.New("failed to open datastore: %w", err)
	}
	proc.Datastore = datastore

	return proc, nil
}

func (p *DatastoreProcess) Close() error {
	p.cancel()
	return Error.Wrap(
		errs.Combine(
			p.group.Wait(),
			p.Datastore.Close(),
			p.DB.Close(),
		))
}

func (storj *StorjConfig) Create(path string) (repo.Datastore, error) {
	log.Desugar().Debug("Create", zap.String("Path", path))

	ctx := context.Background()
	data, err := OpenProcess(ctx, storj.cfg)
	return data, Error.Wrap(err)
}

func (storj *StorjConfig) RunDebug(ctx context.Context) (err error) {
	if len(storj.cfg.DebugAddr) == 0 {
		return nil
	}

	monkit.AddErrorNameHandler(func(err error) (string, bool) {
		if errors.Is(err, ds.ErrNotFound) {
			return "not found", true
		}
		return "", false
	})

	ln, err := net.Listen("tcp", storj.cfg.DebugAddr)
	if err != nil {
		return Error.New("failed to initialize debugger: %v", err)
	}

	server := debug.NewServerWithAtomicLevel(log.Desugar(), ln, monkit.Default, debug.Config{
		Address: storj.cfg.DebugAddr,
	}, getAtomicLevel())

	log.Desugar().Debug("Debug server listening", zap.Stringer("Address", ln.Addr()))

	err = server.Run(ctx)
	if err != nil {
		log.Desugar().Error("Debug server died", zap.Error(err))
	}

	return nil
}

func getAtomicLevel() *zap.AtomicLevel {
	level := getUnexportedField(log.Desugar().Core(), "level")
	if level == nil {
		return nil
	}

	atomic, ok := level.(zap.AtomicLevel)
	if !ok {
		log.Desugar().Warn("Could not obtain atomic log level")
		return nil
	}

	return &atomic
}

func getBloomFilter(blockstore blockstore.Blockstore) *bbloom.Bloom {
	bs := getUnexportedField(blockstore, "bs")
	if bs == nil {
		return nil
	}

	rbloom := getUnexportedField(bs, "bloom")
	if rbloom == nil {
		return nil
	}

	bloom, ok := rbloom.(*bbloom.Bloom)
	if !ok {
		return nil
	}

	return bloom
}

func getUnexportedField(iface interface{}, name string) interface{} {
	value := reflect.ValueOf(iface)
	if value.Kind() != reflect.Interface && value.Kind() != reflect.Ptr {
		log.Desugar().Debug("Interface kind is not interface or ptr", zap.Any("Kind", value.Kind()), zap.Any("Value", value))
		return nil
	}

	elem := value.Elem()
	if elem.Kind() != reflect.Struct {
		log.Desugar().Debug("Elem kind is not struct", zap.Any("Kind", elem.Kind()), zap.Any("Elem", elem))
		return nil
	}

	field := elem.FieldByName(name)
	if field == (reflect.Value{}) {
		log.Desugar().Debug("Zero reflect value for field", zap.String("Name", name))
		return nil
	}

	if !field.CanAddr() {
		log.Desugar().Debug("Field is not addressable", zap.Any(name, field))
		return nil
	}

	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}
