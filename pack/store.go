// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import (
	"context"
	"io/ioutil"

	"github.com/google/uuid"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/uplink"
	"storj.io/zipper"
)

// Error is the error class for pack chore.
var Error = errs.Class("pack")

type Store struct {
	log     *zap.Logger
	project *uplink.Project
	bucket  string
}

func NewStore(log *zap.Logger, project *uplink.Project, bucket string) *Store {
	return &Store{
		log:     log,
		project: project,
		bucket:  bucket,
	}
}

func (store *Store) ReadBlock(ctx context.Context, packObject string, packOffset, size int) ([]byte, error) {
	download, err := store.project.DownloadObject(ctx, store.bucket, packObject, &uplink.DownloadOptions{
		Offset: int64(packOffset),
		Length: int64(size),
	})
	if err != nil {
		return nil, Error.Wrap(err)
	}
	defer func() {
		err = errs.Combine(err, download.Close())
	}()

	data, err := ioutil.ReadAll(download)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return data, nil
}

func (store *Store) WritePack(ctx context.Context, blocks map[string][]byte) (string, map[string]int, error) {
	store.log.Debug("Pack: blocks ready to pack", zap.Int("Count", len(blocks)))

	packObjectKey := uuid.NewString()
	pack, err := zipper.CreatePack(ctx, store.project, store.bucket, packObjectKey, nil)
	if err != nil {
		return "", nil, Error.Wrap(err)
	}

	store.log.Debug("Pack: created pending pack", zap.String("Object Key", packObjectKey))

	cidOffs := make(map[string]int, len(blocks))
	for cid, data := range blocks {
		writer, err := pack.Add(ctx, cid, &zipper.FileHeader{Uncompressed: true})
		if err != nil {
			return "", nil, Error.Wrap(err)
		}

		cidOffs[cid] = int(writer.ContentOffset())

		_, err = writer.Write(data)
		if err != nil {
			return "", nil, Error.Wrap(err)
		}

		store.log.Debug("Pack: added block to pack",
			zap.String("CID", cid),
			zap.Int("Size", len(data)),
			zap.String("Object Key", packObjectKey),
			zap.Int("Offset", cidOffs[cid]))
	}

	err = pack.Commit(ctx)
	if err != nil {
		return "", nil, Error.Wrap(err)
	}

	store.log.Debug("Pack: committed pack", zap.String("Object Key", packObjectKey))

	return packObjectKey, cidOffs, nil
}
