// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jtolio/zipper"
	"github.com/kaloyan-raev/ipfs-go-ds-storj/dbx"

	"storj.io/common/memory"
	"storj.io/common/sync2"
	"storj.io/uplink"
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
	db       *dbx.DB
	project  *uplink.Project
	bucket   string
	interval time.Duration
	minSize  int
	maxSize  int
	loop     *sync2.Cycle
	runOnce  sync.Once
}

func NewChore(logger *log.Logger, db *dbx.DB, project *uplink.Project, bucket string) *Chore {
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

	// final packing attempt before closing
	chore.loop.TriggerWait()
	chore.loop.Close()

	return nil
}

func (chore *Chore) pack(ctx context.Context) (err error) {
	chore.logger.Println("Pack")
	defer func() {
		if err != nil {
			chore.logger.Printf("Pack error: %v\n", err)
		}
	}()

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

	var cids []string
	for cid, data := range blocks {
		cids = append(cids, cid)

		writer, err := pack.Add(ctx, cid, nil)
		if err != nil {
			return err
		}

		_, err = writer.Write(data)
		if err != nil {
			return err
		}
	}

	err = pack.Commit(ctx)
	if err != nil {
		return err
	}

	return chore.updatePackedBlocks(ctx, cids, packObjectKey)
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

func (chore *Chore) updatePackedBlocks(ctx context.Context, cids []string, packObjectKey string) error {
	stmt := fmt.Sprintf(`
		UPDATE blocks
		SET
			pack_status = `+packedStatus+`, 
			pack_object = $1,
			data = NULL
		WHERE
			pack_status = `+packingStatus+` AND
			cid IN (%s)
	`, sqlTextArray(cids))

	result, err := chore.db.ExecContext(ctx, stmt, packObjectKey)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected != int64(len(cids)) {
		// TODO: revert the update, perhaps use a transaction and roll it back
		return fmt.Errorf("unexpected number of blocks updated db: want %d, got %d", len(cids), affected)
	}

	return nil
}

func sqlTextArray(elems []string) string {
	if len(elems) == 0 {
		return ""
	}
	return fmt.Sprintf(`"%s"`, strings.Join(elems, `","`))
}
