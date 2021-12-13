// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package storjds

import (
	"context"
	"log"
	"time"

	"github.com/kaloyan-raev/ipfs-go-ds-storj/dbx"

	"storj.io/common/sync2"
	"storj.io/uplink"
)

const DefaultPackInterval = 1 * time.Minute

type packer struct {
	logger   *log.Logger
	db       *dbx.DB
	project  *uplink.Project
	interval time.Duration
	loop     *sync2.Cycle
}

func (packer *packer) ensureRunning(ctx context.Context) {
	if packer.loop != nil {
		return
	}

	if packer.interval == 0 {
		packer.interval = DefaultPackInterval
	}

	packer.loop = sync2.NewCycle(packer.interval)

	go packer.loop.Run(ctx, packer.pack)
}

func (packer *packer) Close() error {
	if packer.loop == nil {
		return nil
	}

	// final packing attempt before closing
	packer.loop.TriggerWait()
	packer.loop.Close()

	return nil
}

func (packer *packer) pack(ctx context.Context) error {
	packer.logger.Println("Pack")

	return nil
}
