// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package plugin

import (
	"reflect"
	"testing"
	"time"

	storjds "storj.io/ipfs-go-ds-storj"
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

				NodeConnectionPoolCapacity:       DefaultNodeConnectionPoolCapacity,
				NodeConnectionPoolKeyCapacity:    DefaultNodeConnectionPoolKeyCapacity,
				NodeConnectionPoolIdleExpiration: DefaultNodeConnectionPoolIdleExpiration,

				SatelliteConnectionPoolCapacity:       DefaultSatelliteConnectionPoolCapacity,
				SatelliteConnectionPoolKeyCapacity:    DefaultSatelliteConnectionPoolKeyCapacity,
				SatelliteConnectionPoolIdleExpiration: DefaultSatelliteConnectionPoolIdleExpiration,
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
			// Required dbURI is empty
			Input: map[string]interface{}{
				"dbURI":       "",
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
			// Required bucket fields is empty
			Input: map[string]interface{}{
				"dbURI":       "somedburi",
				"bucket":      "",
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
			// Required accessGrant fields is empty
			Input: map[string]interface{}{
				"dbURI":       "somedburi",
				"bucket":      "somebucket",
				"accessGrant": "",
			},
			HasErr: true,
		},
		{
			// Optional fields included
			Input: map[string]interface{}{
				"dbURI":       "somedburi",
				"bucket":      "somebucket",
				"accessGrant": "someaccessgrant",

				"packInterval":      "3m",
				"debugAddr":         "somedebugaddr",
				"updateBloomFilter": "true",

				"nodeConnectionPoolCapacity":       "1234",
				"nodeConnectionPoolKeyCapacity":    "12",
				"nodeConnectionPoolIdleExpiration": "23m",

				"satelliteConnectionPoolCapacity":       "789",
				"satelliteConnectionPoolKeyCapacity":    "89",
				"satelliteConnectionPoolIdleExpiration": "9m",
			},
			Want: &StorjConfig{cfg: storjds.Config{
				DBURI:       "somedburi",
				Bucket:      "somebucket",
				AccessGrant: "someaccessgrant",

				PackInterval:      3 * time.Minute,
				DebugAddr:         "somedebugaddr",
				UpdateBloomFilter: true,

				NodeConnectionPoolCapacity:       1234,
				NodeConnectionPoolKeyCapacity:    12,
				NodeConnectionPoolIdleExpiration: 23 * time.Minute,

				SatelliteConnectionPoolCapacity:       789,
				SatelliteConnectionPoolKeyCapacity:    89,
				SatelliteConnectionPoolIdleExpiration: 9 * time.Minute,
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
		{
			// Invalid updateBloomFilter format
			Input: map[string]interface{}{
				"dbURI":             "somedburi",
				"bucket":            "somebucket",
				"accessGrant":       "someaccessgrant",
				"updateBloomFilter": "yes",
			},
			HasErr: true,
		},
		{
			// Invalid nodeConnectionPoolCapacity format
			Input: map[string]interface{}{
				"dbURI":                      "somedburi",
				"bucket":                     "somebucket",
				"accessGrant":                "someaccessgrant",
				"nodeConnectionPoolCapacity": "not-a-number",
			},
			HasErr: true,
		},
		{
			// Invalid nodeConnectionPoolKeyCapacity format
			Input: map[string]interface{}{
				"dbURI":                         "somedburi",
				"bucket":                        "somebucket",
				"accessGrant":                   "someaccessgrant",
				"nodeConnectionPoolKeyCapacity": "not-a-number",
			},
			HasErr: true,
		},
		{
			// Invalid nodeConnectionPoolIdleExpiration format
			Input: map[string]interface{}{
				"dbURI":                            "somedburi",
				"bucket":                           "somebucket",
				"accessGrant":                      "someaccessgrant",
				"nodeConnectionPoolIdleExpiration": "3",
			},
			HasErr: true,
		},
		{
			// Invalid satelliteConnectionPoolCapacity format
			Input: map[string]interface{}{
				"dbURI":                           "somedburi",
				"bucket":                          "somebucket",
				"accessGrant":                     "someaccessgrant",
				"satelliteConnectionPoolCapacity": "not-a-number",
			},
			HasErr: true,
		},
		{
			// Invalid satelliteConnectionPoolKeyCapacity format
			Input: map[string]interface{}{
				"dbURI":                              "somedburi",
				"bucket":                             "somebucket",
				"accessGrant":                        "someaccessgrant",
				"satelliteConnectionPoolKeyCapacity": "not-a-number",
			},
			HasErr: true,
		},
		{
			// Invalid satelliteConnectionPoolIdleExpiration format
			Input: map[string]interface{}{
				"dbURI":                                 "somedburi",
				"bucket":                                "somebucket",
				"accessGrant":                           "someaccessgrant",
				"satelliteConnectionPoolIdleExpiration": "3",
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
