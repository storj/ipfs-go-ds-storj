// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/kaloyan-raev/ipfs-go-ds-storj/db"

	"storj.io/common/memory"
	"storj.io/common/sync2"
)

const (
	DefaultInterval = 1 * time.Minute
	DefaultMinSize  = 60 * memory.MiB
	DefaultMaxSize  = 62 * memory.MiB
	MaxBlockSize    = 1 * memory.MiB
)

type Chore struct {
	logger   *log.Logger
	db       *db.DB
	packs    *Store
	interval time.Duration
	minSize  int
	maxSize  int
	loop     *sync2.Cycle
	runOnce  sync.Once
}

func NewChore(logger *log.Logger, db *db.DB, packs *Store) *Chore {
	return &Chore{
		logger:   logger,
		db:       db,
		packs:    packs,
		interval: DefaultInterval,
		minSize:  DefaultMinSize.Int(),
		maxSize:  DefaultMaxSize.Int(),
	}
}

func (chore *Chore) WithInterval(interval time.Duration) *Chore {
	chore.interval = interval
	if interval <= 0 {
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
	chore.runOnce.Do(func() {
		chore.loop = sync2.NewCycle(chore.interval)
		go func() {
			err := chore.loop.Run(ctx, chore.pack)
			chore.logger.Printf("Pack error: %v\n", err)
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
	chore.logger.Println("Pack")
	defer func() {
		if err != nil {
			chore.logger.Printf("Pack error: %v\n", err)
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
