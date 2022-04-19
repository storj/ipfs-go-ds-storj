// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import (
	"context"
	"io/ioutil"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/uplink"
	"storj.io/zipper"
)

var mon = monkit.Package()

var log = logging.Logger("storjds").Named("pack")

// Error is the error class for pack chore.
var Error = errs.Class("pack")

type Store struct {
	project *uplink.Project
	bucket  string
}

func NewStore(project *uplink.Project, bucket string) *Store {
	return &Store{
		project: project,
		bucket:  bucket,
	}
}

func (store *Store) ReadBlock(ctx context.Context, packObject string, packOffset, size int) (data []byte, err error) {
	defer mon.Task()(&ctx)(&err)

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

	data, err = ioutil.ReadAll(download)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return data, nil
}

func (store *Store) WritePack(ctx context.Context, blocks map[string][]byte) (packObjectKey string, cidOffs map[string]int, err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Blocks ready to pack", zap.Int("Count", len(blocks)))

	packObjectKey = uuid.NewString()
	pack, err := zipper.CreatePack(ctx, store.project, store.bucket, packObjectKey, nil)
	if err != nil {
		return "", nil, Error.Wrap(err)
	}

	log.Desugar().Debug("Created pending pack", zap.String("Object Key", packObjectKey))

	cidOffs = make(map[string]int, len(blocks))
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

		log.Desugar().Debug("Added block to pack",
			zap.String("CID", cid),
			zap.Int("Size", len(data)),
			zap.String("Object Key", packObjectKey),
			zap.Int("Offset", cidOffs[cid]))
	}

	err = pack.Commit(ctx)
	if err != nil {
		return "", nil, Error.Wrap(err)
	}

	log.Desugar().Debug("Committed pack", zap.String("Object Key", packObjectKey))

	return packObjectKey, cidOffs, nil
}
