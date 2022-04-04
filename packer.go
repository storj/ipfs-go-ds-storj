// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/kaloyan-raev/ipfs-go-ds-storj/db"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/pack"

	"storj.io/common/memory"
	"storj.io/common/sync2"
)

const (
	DefaultInterval = 1 * time.Minute
	DefaultMinSize  = 60 * memory.MiB
	DefaultMaxSize  = 62 * memory.MiB
	MaxBlockSize    = 1 * memory.MiB
)

type Packer struct {
	logger     *log.Logger
	blockstore *db.Blockstore
	packstore  *pack.Store
	interval   time.Duration
	minSize    int
	maxSize    int
	loop       *sync2.Cycle
	runOnce    sync.Once
}

func NewPacker(logger *log.Logger, blockstore *db.Blockstore, packstore *pack.Store) *Packer {
	return &Packer{
		logger:     logger,
		blockstore: blockstore,
		packstore:  packstore,
		interval:   DefaultInterval,
		minSize:    DefaultMinSize.Int(),
		maxSize:    DefaultMaxSize.Int(),
	}
}

func (packer *Packer) WithInterval(interval time.Duration) *Packer {
	packer.interval = interval
	if interval <= 0 {
		packer.interval = DefaultInterval
	}
	return packer
}

func (packer *Packer) WithPackSize(min, max int) *Packer {
	if min >= MaxBlockSize.Int() {
		packer.minSize = min
	}
	if max-packer.minSize >= MaxBlockSize.Int() {
		packer.maxSize = max
	}
	return packer
}

func (packer *Packer) Run(ctx context.Context) {
	packer.runOnce.Do(func() {
		packer.loop = sync2.NewCycle(packer.interval)
		go func() {
			err := packer.loop.Run(ctx, packer.pack)
			packer.logger.Printf("Pack error: %v\n", err)
		}()
	})
}

func (packer *Packer) Close() error {
	if packer.loop == nil {
		return nil
	}

	packer.loop.Close()

	return nil
}

func (packer *Packer) TriggerWait() {
	packer.loop.TriggerWait()
}

func (packer *Packer) pack(ctx context.Context) (err error) {
	packer.logger.Println("Pack")
	defer func() {
		if err != nil {
			packer.logger.Printf("Pack error: %v\n", err)
		}
	}()

	for {
		blocks, err := packer.blockstore.QueryNextPack(ctx, packer.minSize, packer.maxSize)
		if err != nil {
			return Error.Wrap(err)
		}

		if len(blocks) == 0 {
			// unpacked blocks are not enough for a new pack
			return nil
		}

		packObjectKey, cidOffs, err := packer.packstore.WritePack(ctx, blocks)
		if err != nil {
			return Error.Wrap(err)
		}

		err = packer.blockstore.UpdatePackedBlocks(ctx, packObjectKey, cidOffs)
		if err != nil {
			return Error.Wrap(err)
		}
	}
}
