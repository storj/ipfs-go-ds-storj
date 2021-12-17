// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zeebo/errs"

	"storj.io/common/memory"
	"storj.io/common/sync2"
	"storj.io/uplink"
	"storj.io/zipper"
)

const (
	DefaultInterval = 1 * time.Minute
	DefaultMinSize  = 32 * memory.MiB
	DefaultMaxSize  = 60 * memory.MiB
	MaxBlockSize    = 1 * memory.MiB
)

type Status int

const (
	Unpacked = Status(0)
	Packing  = Status(1)
	Packed   = Status(2)

	unpackedStatus = "0"
	packingStatus  = "1"
	packedStatus   = "2"
)

type Chore struct {
	logger   *log.Logger
	db       *sql.DB
	project  *uplink.Project
	bucket   string
	interval time.Duration
	minSize  int
	maxSize  int
	loop     *sync2.Cycle
	runOnce  sync.Once
}

func NewChore(logger *log.Logger, db *sql.DB, project *uplink.Project, bucket string) *Chore {
	return &Chore{
		logger:   logger,
		db:       db,
		project:  project,
		bucket:   bucket,
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
		blocks, err := chore.queryNextPack(ctx)
		if err != nil {
			return err
		}

		if len(blocks) == 0 {
			// unpacked blocks are not enough for a new pack
			return nil
		}

		packObjectKey := uuid.NewString()
		pack, err := zipper.CreatePack(ctx, chore.project, chore.bucket, packObjectKey, nil)
		if err != nil {
			return err
		}

		cidOffs := make(map[string]int, len(blocks))
		for cid, data := range blocks {
			writer, err := pack.Add(ctx, cid, &zipper.FileHeader{Uncompressed: true})
			if err != nil {
				return err
			}

			cidOffs[cid] = int(writer.ContentOffset)

			_, err = writer.Write(data)
			if err != nil {
				return err
			}
		}

		err = pack.Commit(ctx)
		if err != nil {
			return err
		}

		err = chore.updatePackedBlocks(ctx, packObjectKey, cidOffs)
		if err != nil {
			return err
		}
	}
}

func (chore *Chore) queryNextPack(ctx context.Context) (map[string][]byte, error) {
	result, err := chore.db.ExecContext(ctx, `
		WITH next_pack AS (
			SELECT b.cid, sum(b2.size) AS sums
			FROM blocks b
			INNER JOIN blocks b2 ON b.pack_status=b2.pack_status AND b2.created <= b.created
			WHERE b.pack_status = `+unpackedStatus+`
			GROUP BY b.cid
			HAVING sum(b2.size) <= $1
			ORDER BY b.created ASC
		)
		UPDATE blocks
		SET pack_status = `+packingStatus+`
		WHERE 
			$2 <= (SELECT max(sums) FROM next_pack) AND
			cid IN (SELECT cid FROM next_pack)
	`, chore.maxSize, chore.minSize)
	if err != nil {
		return nil, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	chore.logger.Printf("queryNextPack: affected %d rows", affected)

	if affected == 0 {
		return nil, nil
	}

	rows, err := chore.db.QueryContext(ctx, `
		SELECT cid, data
		FROM blocks
		WHERE
			pack_status = `+packingStatus+`
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocks := make(map[string][]byte)
	for rows.Next() {
		var cid string
		var data []byte
		if err := rows.Scan(&cid, &data); err != nil {
			return nil, err
		}
		blocks[cid] = data
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return blocks, nil
}

func (chore *Chore) updatePackedBlocks(ctx context.Context, packObjectKey string, cidOffs map[string]int) error {
	tx, err := chore.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, tx.Rollback())
			return
		}
		err = tx.Commit()
	}()

	for cid, off := range cidOffs {
		result, err := tx.ExecContext(ctx, `
			UPDATE blocks
			SET
				pack_status = `+packedStatus+`, 
				pack_object = $1,
				pack_offset = $2,
				data = NULL
			WHERE
				cid = $3 AND
				pack_status = `+packingStatus+`
		`, packObjectKey, off, cid)
		if err != nil {
			return err
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}

		if affected != 1 {
			return fmt.Errorf("unexpected number of blocks updated db: want 1, got %d", affected)
		}

		chore.logger.Printf("Updated block %s with as packed at offset %d", cid, off)
	}

	return nil
}
