// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package plugin

import (
	"context"
	"errors"
	"net"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	logging "github.com/ipfs/go-log/v2"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	storjds "storj.io/ipfs-go-ds-storj"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/private/debug"
)

var log = logging.Logger("storjds").Named("plugin")

// Error is the error class for Storj datastore plugin.
var Error = errs.Class("storjds")

var Plugins = []plugin.Plugin{
	&StorjPlugin{},
}

type StorjPlugin struct{}

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
		if !ok {
			return nil, Error.New("no dbURI specified")
		}

		bucket, ok := m["bucket"].(string)
		if !ok {
			return nil, Error.New("no bucket specified")
		}

		accessGrant, ok := m["accessGrant"].(string)
		if !ok {
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

		return &StorjConfig{
			cfg: storjds.Config{
				DBURI:        dbURI,
				Bucket:       bucket,
				AccessGrant:  accessGrant,
				PackInterval: packInterval,
				DebugAddr:    debugAddr,
			},
		}, nil
	}
}

type StorjConfig struct {
	cfg storjds.Config
}

func (storj *StorjConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{
		"bucket": storj.cfg.Bucket,
	}
}

func (storj *StorjConfig) Create(path string) (repo.Datastore, error) {
	ctx := context.Background()

	err := storj.initDebug()
	if err != nil {
		return nil, err
	}

	db, err := db.Open(ctx, storj.cfg.DBURI)
	if err != nil {
		return nil, Error.New("failed to connect to cache database: %s", err)
	}

	err = db.MigrateToLatest(ctx)
	if err != nil {
		return nil, Error.New("failed to migrate database schema: %s", err)
	}

	return storjds.NewDatastore(context.Background(), db, storj.cfg)
}

func (storj *StorjConfig) initDebug() (err error) {
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

	go func() {
		server := debug.NewServer(log.Desugar(), ln, monkit.Default, debug.Config{
			Address: storj.cfg.DebugAddr,
		})

		log.Desugar().Debug("Debug server listening", zap.Stringer("Address", ln.Addr()))

		err := server.Run(context.Background())
		if err != nil {
			log.Desugar().Error("Debug server died", zap.Error(err))
		}
	}()

	return nil
}
