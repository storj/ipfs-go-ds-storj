// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package plugin

import (
	"fmt"
	"time"

	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	storjds "github.com/kaloyan-raev/ipfs-go-ds-storj"
)

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
			return nil, fmt.Errorf("storjds: no dbURI specified")
		}

		bucket, ok := m["bucket"].(string)
		if !ok {
			return nil, fmt.Errorf("storjds: no bucket specified")
		}

		accessGrant, ok := m["accessGrant"].(string)
		if !ok {
			return nil, fmt.Errorf("storjds: no accessGrant specified")
		}

		// Optional.

		var logFile string
		if v, ok := m["logFile"]; ok {
			logFile, ok = v.(string)
			if !ok {
				return nil, fmt.Errorf("storjds: logFile not a string")
			}
		}

		var packInterval time.Duration
		if v, ok := m["packInterval"]; ok {
			interval, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("storjds: packInterval not a string")
			}
			var err error
			packInterval, err = time.ParseDuration(interval)
			if err != nil {
				return nil, fmt.Errorf("storjds: packInterval not a duration: %v", err)
			}
		}

		return &StorjConfig{
			cfg: storjds.Config{
				DBURI:        dbURI,
				Bucket:       bucket,
				AccessGrant:  accessGrant,
				LogFile:      logFile,
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
	return storjds.NewStorjDatastore(storj.cfg)
}
