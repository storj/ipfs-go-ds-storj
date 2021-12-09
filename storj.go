// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"

	"storj.io/uplink"
)

type StorjDS struct {
	Config
	Project *uplink.Project
	log     *os.File
}

type Config struct {
	AccessGrant string
	Bucket      string
}

func NewStorjDatastore(conf Config) (*StorjDS, error) {
	f, err := os.OpenFile("/tmp/ipfs.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %s", err)
	}

	fmt.Fprintln(f, "NewStorjDatastore")

	access, err := uplink.ParseAccess(conf.AccessGrant)
	if err != nil {
		return nil, fmt.Errorf("failed to parse access grant: %s", err)
	}

	project, err := uplink.OpenProject(context.Background(), access)
	if err != nil {
		return nil, fmt.Errorf("failed to open project: %s", err)
	}

	return &StorjDS{
		Config:  conf,
		Project: project,
		log:     f,
	}, nil
}

func (storj *StorjDS) Put(k ds.Key, value []byte) error {
	fmt.Fprintf(storj.log, "Put --- key: %s --- bytes: %d --- type: %s --- parent: %s\n", k.String(), len(value), k.Type(), k.Parent().String())
	upload, err := storj.Project.UploadObject(context.Background(), storj.Bucket, k.String(), nil)
	if err != nil {
		return err
	}

	_, err = upload.Write(value)
	if err != nil {
		return err
	}

	return upload.Commit()
}

func (storj *StorjDS) Sync(prefix ds.Key) error {
	fmt.Fprintf(storj.log, "Sync --- prefix: %s\n", prefix.String())
	return nil
}

func (storj *StorjDS) Get(k ds.Key) ([]byte, error) {
	fmt.Fprintf(storj.log, "Get --- key: %s --- type: %s --- parent: %s\n", k.String(), k.Type(), k.Parent().String())
	download, err := storj.Project.DownloadObject(context.Background(), storj.Bucket, k.String(), nil)
	if err != nil {
		if isNotFound(err) {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return ioutil.ReadAll(download)
}

func (storj *StorjDS) Has(k ds.Key) (exists bool, err error) {
	fmt.Fprintf(storj.log, "Has --- key: %s --- type: %s --- parent: %s\n", k.String(), k.Type(), k.Parent().String())
	_, err = storj.Project.StatObject(context.Background(), storj.Bucket, k.String())
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (storj *StorjDS) GetSize(k ds.Key) (size int, err error) {
	//fmt.Fprintf(s.log, "GetSize --- key: %s --- type: %s --- parent: %s\n", k.String(), k.Type(), k.Parent().String())
	obj, err := storj.Project.StatObject(context.Background(), storj.Bucket, k.String())
	if err != nil {
		if isNotFound(err) {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}
	return int(obj.System.ContentLength), nil
}

func (storj *StorjDS) Delete(k ds.Key) error {
	fmt.Fprintf(storj.log, "Delete --- key: %s --- type: %s --- parent: %s\n", k.String(), k.Type(), k.Parent().String())
	_, err := storj.Project.DeleteObject(context.Background(), storj.Bucket, k.String())
	if isNotFound(err) {
		// delete is idempotent
		err = nil
	}
	return err
}

func (storj *StorjDS) Query(q dsq.Query) (dsq.Results, error) {
	fmt.Fprintf(storj.log, "Query --- %s\n", q.String())
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("storjds: filters or orders are not supported")
	}

	// Storj stores a "/foo" key as "foo" so we need to trim the leading "/"
	// q.Prefix = strings.TrimPrefix(q.Prefix, "/")

	list := storj.Project.ListObjects(context.Background(), storj.Bucket, &uplink.ListObjectsOptions{
		Prefix:    q.Prefix,
		Recursive: true,
		System:    true, // TODO: enable only if q.ReturnsSizes = true
		// Cursor: TODO,
	})
	if list.Err() != nil {
		return nil, list.Err()
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			return nil
		},
		Next: func() (dsq.Result, bool) {
			// TODO: skip offset, apply limit
			more := list.Next()
			if !more {
				if list.Err() != nil {
					return dsq.Result{Error: list.Err()}, false
				}
				return dsq.Result{}, false
			}
			entry := dsq.Entry{
				Key:  list.Item().Key,
				Size: int(list.Item().System.ContentLength),
			}
			if !q.KeysOnly {
				value, err := storj.Get(ds.NewKey(entry.Key))
				if err != nil {
					return dsq.Result{Error: err}, false
				}
				entry.Value = value
			}
			return dsq.Result{Entry: entry}, true
		},
	}), nil
}

func (storj *StorjDS) Batch() (ds.Batch, error) {
	fmt.Fprintln(storj.log, "Batch")
	return &storjBatch{
		storj: storj,
		ops:   make(map[ds.Key]batchOp),
	}, nil
}

func (storj *StorjDS) Close() error {
	fmt.Fprintln(storj.log, "Close")

	// TODO: combine errors
	err := storj.Project.Close()
	if err != nil {
		return err
	}

	return storj.log.Close()
}

func isNotFound(err error) bool {
	return errors.Is(err, uplink.ErrObjectNotFound)
}

type storjBatch struct {
	storj *StorjDS
	ops   map[ds.Key]batchOp
}

type batchOp struct {
	val    []byte
	delete bool
}

func (b *storjBatch) Put(k ds.Key, val []byte) error {
	fmt.Fprintf(b.storj.log, "BatchPut --- key: %s --- bytes: %d --- type: %s --- parent: %s\n", k.String(), len(val), k.Type(), k.Parent().String())
	b.ops[k] = batchOp{
		val:    val,
		delete: false,
	}
	return nil
}

func (b *storjBatch) Delete(k ds.Key) error {
	fmt.Fprintf(b.storj.log, "BatchDelete --- key: %s --- type: %s --- parent: %s\n", k.String(), k.Type(), k.Parent().String())
	b.ops[k] = batchOp{
		val:    nil,
		delete: true,
	}
	return nil
}

func (b *storjBatch) Commit() error {
	fmt.Fprintln(b.storj.log, "BatchCommit")

	for k, op := range b.ops {
		var err error
		if op.delete {
			err = b.storj.Delete(k)
		} else {
			err = b.storj.Put(k, op.val)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

var _ ds.Batching = (*StorjDS)(nil)
