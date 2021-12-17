// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
	"storj.io/common/memory"
	"storj.io/uplink"
)

func TestSuiteStorj(t *testing.T) {
	// Only run tests when STORJ_ACCESS is set. STORJ_ACCESS must be set to a
	// valid access grant. The easiest way to set up a local Storj test network
	// is to use https://github.com/storj/up. After setting it up, executing
	// `storj-up credentials -e` will print a valid access grant to the local
	// test network. Executing `eval $(storj-up credentials -e) will set it
	// also to the STORJ_ACCESS environment variable. Finally, execute
	// `go test -v ./...` to run the tests.
	access, set := os.LookupEnv("STORJ_ACCESS")
	if !set {
		t.Skipf("skipping test suit; STORJ_ACCESS is not set.")
	}

	config := Config{
		Bucket:       "testbucket",
		AccessGrant:  access,
		PackInterval: 100 * time.Millisecond,
		MinPackSize:  1 * memory.MiB.Int(),
		MaxPackSize:  2 * memory.MiB.Int(),
	}

	storj, err := NewStorjDatastore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := storj.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	storj.runCleanTest(t, "basic operations", func(t *testing.T) {
		dstest.SubtestBasicPutGet(t, storj)
	})
	storj.runCleanTest(t, "not found operations", func(t *testing.T) {
		dstest.SubtestNotFounds(t, storj)
	})
	storj.runCleanTest(t, "many puts and gets, query", func(t *testing.T) {
		dstest.SubtestManyKeysAndQuery(t, storj)
	})
	storj.runCleanTest(t, "return sizes", func(t *testing.T) {
		dstest.SubtestReturnSizes(t, storj)
	})
	storj.runCleanTest(t, "batch", func(t *testing.T) {
		dstest.RunBatchTest(t, storj)
	})
	storj.runCleanTest(t, "batch delete", func(t *testing.T) {
		dstest.RunBatchDeleteTest(t, storj)
	})
	storj.runCleanTest(t, "batch put and delete", func(t *testing.T) {
		dstest.RunBatchPutAndDeleteTest(t, storj)
	})
}

func (storj *StorjDS) runCleanTest(t *testing.T, name string, f func(t *testing.T)) {
	_, err := storj.project.DeleteBucketWithObjects(context.Background(), storj.Bucket)
	if err != nil && !errors.Is(err, uplink.ErrBucketNotFound) {
		t.Fatal(err)
	}

	_, err = storj.project.CreateBucket(context.Background(), storj.Bucket)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_, err := storj.project.DeleteBucketWithObjects(context.Background(), storj.Bucket)
		if err != nil {
			t.Fatal(err)
		}
	}()

	t.Run(name, f)

	err = storj.Sync(datastore.Key{})
	if err != nil {
		t.Fatal(err)
	}
}
