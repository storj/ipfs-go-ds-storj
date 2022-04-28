// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"storj.io/common/memory"
	"storj.io/common/sync2"
	"storj.io/ipfs-go-ds-storj/db"
)

const (
	DefaultInterval = 1 * time.Minute
	DefaultMinSize  = 60 * memory.MiB
	DefaultMaxSize  = 62 * memory.MiB
	MaxBlockSize    = 1 * memory.MiB
)

type Chore struct {
	db       *db.DB
	packs    *Store
	interval time.Duration
	minSize  int
	maxSize  int
	loop     *sync2.Cycle
	runOnce  sync.Once
}

func NewChore(db *db.DB, packs *Store) *Chore {
	return &Chore{
		db:       db,
		packs:    packs,
		interval: DefaultInterval,
		minSize:  DefaultMinSize.Int(),
		maxSize:  DefaultMaxSize.Int(),
	}
}

func (chore *Chore) WithInterval(interval time.Duration) *Chore {
	chore.interval = interval
	if interval == 0 {
		chore.interval = DefaultInterval
	}
	return chore
}

func (chore *Chore) WithPackSize(min, max int) *Chore {
	if min >= MaxBlockSize.Int() {
		chore.minSize = min
	}
	if max-chore.minSize >= MaxBlockSize.Int() {
		chore.maxSize = max
	}
	return chore
}

func (chore *Chore) Run(ctx context.Context) {
	defer mon.Task()(&ctx)(nil)

	chore.runOnce.Do(func() {
		// Don't run if the pack interval is negative.
		if chore.interval < 0 {
			log.Desugar().Info("Packing disabled")
			return
		}

		chore.loop = sync2.NewCycle(chore.interval)

		go func() {
			err := chore.loop.Run(ctx, chore.pack)
			if err != nil {
				log.Desugar().Error("Pack error", zap.Error(err))
			}
		}()
	})
}

func (chore *Chore) Close() error {
	if chore.loop == nil {
		return nil
	}

	chore.loop.Close()

	return nil
}

func (chore *Chore) TriggerWait() {
	chore.loop.TriggerWait()
}

func (chore *Chore) pack(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Info("Pack")
	defer func() {
		if err != nil {
			log.Desugar().Error("Pack error", zap.Error(err))
		}
	}()

	for {
		blocks, err := chore.db.QueryNextPack(ctx, chore.minSize, chore.maxSize)
		if err != nil {
			return Error.Wrap(err)
		}

		if len(blocks) == 0 {
			// unpacked blocks are not enough for a new pack
			return nil
		}

		packObjectKey, cidOffs, err := chore.packs.WritePack(ctx, blocks)
		if err != nil {
			return Error.Wrap(err)
		}

		err = chore.db.UpdatePackedBlocks(ctx, packObjectKey, cidOffs)
		if err != nil {
			return Error.Wrap(err)
		}
	}
}
