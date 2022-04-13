// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	storjds "storj.io/ipfs-go-ds-storj"
	"storj.io/ipfs-go-ds-storj/db"
	"storj.io/private/debug"
	"storj.io/private/process"
)

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
	return "0.1.0"
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

		var logFile string
		if v, ok := m["logFile"]; ok {
			logFile, ok = v.(string)
			if !ok {
				return nil, Error.New("logFile not a string")
			}
		}

		var logLevel string
		if v, ok := m["logLevel"]; ok {
			logLevel, ok = v.(string)
			if !ok {
				return nil, Error.New("logLevel not a string")
			}
		}

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
				LogFile:      logFile,
				LogLevel:     logLevel,
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

	log, level, err := storj.initLogger()
	if err != nil {
		return nil, err
	}

	err = storj.initDebug(log, monkit.Default, level)
	if err != nil {
		return nil, err
	}

	db, err := db.Open(ctx, storj.cfg.DBURI)
	if err != nil {
		return nil, Error.New("failed to connect to cache database: %s", err)
	}

	db = db.WithLog(log)

	err = db.MigrateToLatest(ctx)
	if err != nil {
		return nil, Error.New("failed to migrate database schema: %s", err)
	}

	return storjds.NewDatastore(context.Background(), log, db, storj.cfg)
}

func (storj *StorjConfig) initLogger() (log *zap.Logger, level *zap.AtomicLevel, err error) {
	if len(storj.cfg.LogFile) == 0 {
		log, level, err = process.NewLogger("ipfs-go-ds-storj")
	} else {
		log, level, err = process.NewLoggerWithOutputPathsAndAtomicLevel("ipfs-go-ds-storj", storj.cfg.LogFile)
	}
	if err != nil {
		return nil, nil, Error.New("failed to initialize logger: %v", err)
	}

	if len(storj.cfg.LogLevel) == 0 {
		level.SetLevel(zapcore.InfoLevel)
		return log, level, nil
	}

	lvl := zapcore.Level(0)
	err = lvl.Set(storj.cfg.LogLevel)
	if err != nil {
		return nil, nil, Error.New("failed to parse log level: %v", err)
	}

	level.SetLevel(lvl)

	return log, level, nil
}

func (storj *StorjConfig) initDebug(log *zap.Logger, r *monkit.Registry, atomicLevel *zap.AtomicLevel) (err error) {
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
		server := debug.NewServerWithAtomicLevel(log, ln, r, debug.Config{
			Address: storj.cfg.DebugAddr,
		}, atomicLevel)
		log.Debug(fmt.Sprintf("debug server listening on %s", ln.Addr().String()))
		err := server.Run(context.Background())
		if err != nil {
			log.Error("debug server died", zap.Error(err))
		}
	}()

	return nil
}
