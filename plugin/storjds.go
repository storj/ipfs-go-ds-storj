// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package plugin

import (
	"context"
	"time"

	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	storjds "storj.io/ipfs-go-ds-storj"
	"storj.io/ipfs-go-ds-storj/db"
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

		return &StorjConfig{
			cfg: storjds.Config{
				DBURI:        dbURI,
				Bucket:       bucket,
				AccessGrant:  accessGrant,
				LogFile:      logFile,
				LogLevel:     logLevel,
				PackInterval: packInterval,
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

	log, err := storj.initLogger()
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

func (storj *StorjConfig) initLogger() (log *zap.Logger, err error) {
	var level *zap.AtomicLevel

	if len(storj.cfg.LogFile) == 0 {
		log, level, err = process.NewLogger("ipfs-go-ds-storj")
	} else {
		log, level, err = process.NewLoggerWithOutputPathsAndAtomicLevel("ipfs-go-ds-storj", storj.cfg.LogFile)
	}
	if err != nil {
		return nil, Error.New("failed to initialize logger: %v", err)
	}

	if len(storj.cfg.LogLevel) == 0 {
		level.SetLevel(zapcore.InfoLevel)
		return log, nil
	}

	lvl := zapcore.Level(0)
	err = lvl.Set(storj.cfg.LogLevel)
	if err != nil {
		return nil, Error.New("failed to parse log level: %v", err)
	}

	level.SetLevel(lvl)

	return log, nil
}
