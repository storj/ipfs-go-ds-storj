// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"storj.io/common/errs2"
	"storj.io/common/memory"
	"storj.io/common/sync2"
	"storj.io/ipfs-go-ds-storj/db"
)

const (
	DefaultInterval  = 1 * time.Minute
	DefaultMinSize   = 60 * memory.MiB
	DefaultMaxSize   = 62 * memory.MiB
	DefaultMaxBlocks = 100 * 1000
	MaxBlockSize     = 1 * memory.MiB
)

type Chore struct {
	db        *db.DB
	packs     *Store
	interval  time.Duration
	minSize   int
	maxSize   int
	maxBlocks int
	loop      *sync2.Cycle
	runOnce   sync.Once
}

func NewChore(db *db.DB, packs *Store) *Chore {
	return &Chore{
		db:        db,
		packs:     packs,
		interval:  DefaultInterval,
		minSize:   DefaultMinSize.Int(),
		maxSize:   DefaultMaxSize.Int(),
		maxBlocks: DefaultMaxBlocks,
	}
}

func (chore *Chore) WithInterval(interval time.Duration) *Chore {
	chore.interval = interval
	if interval == 0 {
		chore.interval = DefaultInterval
	}
	return chore
}

func (chore *Chore) WithPackSize(minSize, maxSize, maxBlocks int) *Chore {
	if minSize > 0 {
		chore.minSize = minSize
	}
	if maxSize > 0 {
		chore.maxSize = maxSize
	}
	if maxBlocks > 0 {
		chore.maxBlocks = maxBlocks
	}
	if chore.minSize < MaxBlockSize.Int() {
		chore.minSize = MaxBlockSize.Int()
	}
	if chore.maxSize < MaxBlockSize.Int() {
		chore.maxSize = MaxBlockSize.Int()
	}
	if chore.minSize > chore.maxSize {
		chore.minSize = chore.maxSize
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
			err := chore.loop.Run(ctx, func(ctx context.Context) error {
				err := chore.pack(ctx)
				if err != nil {
					if errs2.IsCanceled(err) {
						// Return the cancel error to stop the loop.
						return err
					}
				}
				// Returning no error will execute the loop again.
				return nil
			})
			if err != nil {
				mon.Event("pack_cycle_error")
				log.Desugar().Error("Pack cycle error", zap.Error(err))
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

	log.Desugar().Debug("Pack")
	defer func() {
		if err != nil {
			mon.Event("pack_error")
			log.Desugar().Error("Pack error", zap.Error(err))
		}
	}()

	for {
		unpackedSize, packingSize, err := chore.db.GetNotPackedBlocksTotalSize(ctx)
		if err != nil {
			return Error.Wrap(err)
		}

		log.Desugar().Debug("Blocks pending packing",
			zap.Int64("Unpacked size", unpackedSize),
			zap.Int64("Packing size", packingSize),
			zap.Int64("Total size", unpackedSize+packingSize),
			zap.Int("Min required", chore.minSize),
		)

		if unpackedSize+packingSize < int64(chore.minSize) {
			return nil
		}

		blocks := make(map[string][]byte)

		// First query any stalled blocks in packing status.
		if packingSize > 0 {
			err := chore.db.QueryPackingBlocksData(ctx, chore.maxSize, chore.maxBlocks, blocks)
			if err != nil {
				return Error.Wrap(err)
			}
			log.Desugar().Debug("Queried stalled blocks with packing status", zap.Int("Count", len(blocks)))
		}

		// Then fill up any remaining space in the pack with unpacked blocks.
		if unpackedSize > 0 && packingSize < int64(chore.maxSize) {
			cids, err := chore.db.GetUnpackedBlocksUpToMaxSize(ctx, chore.maxSize-int(packingSize))
			if err != nil {
				return Error.Wrap(err)
			}

			if len(cids) > 0 {
				err = chore.db.QueryUnpackedBlocksData(ctx, cids, blocks)
				if err != nil {
					return Error.Wrap(err)
				}
			}
		}

		// Final sanity check before the pack (in case the blocks have been deleted while querying).
		if len(blocks) == 0 {
			log.Desugar().Debug("No blocks to pack")
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
