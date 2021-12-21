// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package plugin

import (
	"reflect"
	"testing"
	"time"

	storjds "github.com/kaloyan-raev/ipfs-go-ds-storj"
)

func TestStorjPluginDatastoreConfigParser(t *testing.T) {
	testcases := []struct {
		Input  map[string]interface{}
		Want   *StorjConfig
		HasErr bool
	}{
		{
			// Default case
			Input: map[string]interface{}{
				"dbURI":       "somedburi",
				"bucket":      "somebucket",
				"accessGrant": "someaccessgrant",
			},
			Want: &StorjConfig{cfg: storjds.Config{
				DBURI:       "somedburi",
				Bucket:      "somebucket",
				AccessGrant: "someaccessgrant",
			}},
		},
		{
			// Required dbURI fields missing
			Input: map[string]interface{}{
				"bucket":      "somebucket",
				"accessGrant": "someaccessgrant",
			},
			HasErr: true,
		},
		{
			// Required bucket fields missing
			Input: map[string]interface{}{
				"dbURI":       "somedburi",
				"accessGrant": "someaccessgrant",
			},
			HasErr: true,
		},
		{
			// Required accessGrant fields missing
			Input: map[string]interface{}{
				"dbURI":  "somedburi",
				"bucket": "somebucket",
			},
			HasErr: true,
		},
		{
			// Optional fields included
			Input: map[string]interface{}{
				"dbURI":        "somedburi",
				"bucket":       "somebucket",
				"accessGrant":  "someaccessgrant",
				"logFile":      "somelogfile",
				"packInterval": "3m",
			},
			Want: &StorjConfig{cfg: storjds.Config{
				DBURI:        "somedburi",
				Bucket:       "somebucket",
				AccessGrant:  "someaccessgrant",
				LogFile:      "somelogfile",
				PackInterval: 3 * time.Minute,
			}},
		},
		{
			// Invalid packInterval format
			Input: map[string]interface{}{
				"dbURI":        "somedburi",
				"bucket":       "somebucket",
				"accessGrant":  "someaccessgrant",
				"packInterval": "3",
			},
			HasErr: true,
		},
	}

	for i, tc := range testcases {
		cfg, err := StorjPlugin{}.DatastoreConfigParser()(tc.Input)
		if err != nil {
			if tc.HasErr {
				continue
			}
			t.Errorf("case %d: Failed to parse: %s", i, err)
			continue
		}
		if got, ok := cfg.(*StorjConfig); !ok {
			t.Errorf("wrong config type returned: %T", cfg)
		} else if !reflect.DeepEqual(got, tc.Want) {
			t.Errorf("case %d: got: %v; want %v", i, got, tc.Want)
		}
	}

}
