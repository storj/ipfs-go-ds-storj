// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package bloom

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/bbloom"
	ds "github.com/ipfs/go-datastore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	logging "github.com/ipfs/go-log"
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/ipfs-go-ds-storj/db"
)

var log = logging.Logger("storjds").Named("bloom")

var mon = monkit.Package()

// Error is the error class for Storj datastore.
var Error = errs.Class("bloom")

type Updater struct {
	dbURI      string
	bloom      *bbloom.Bloom
	runOnce    sync.Once
	closedOnce sync.Once
	closed     chan struct{}
}

func NewUpdater(dbURI string, bloom *bbloom.Bloom) *Updater {
	return &Updater{
		dbURI:  dbURI,
		bloom:  bloom,
		closed: make(chan struct{}),
	}
}

func (updater *Updater) Run(ctx context.Context) {
	defer mon.Task()(&ctx)(nil)

	updater.runOnce.Do(func() {
		go func() {
			var err error
			for {
				select {
				case <-updater.closed:
					log.Desugar().Debug("Updater closed")
					return
				case <-ctx.Done():
					log.Desugar().Debug("Context done")
					return
				default:
					if err != nil {
						log.Desugar().Error("Bloom filter updater error", zap.Error(err))
						time.Sleep(1 * time.Second)
					}
					err = updater.listen(ctx, time.Now().Add(-1*time.Minute))
				}
			}
		}()
	})
}

func (updater *Updater) Close() error {
	updater.closedOnce.Do(func() { close(updater.closed) })
	return nil
}

func (updater *Updater) listen(ctx context.Context, cursor time.Time) (err error) {
	defer mon.Task()(&ctx)(&err)

	log.Desugar().Debug("Bloom filter updater")

	db, err := db.Open(ctx, updater.dbURI)
	if err != nil {
		return Error.New("failed to connect to cache database: %s", err)
	}
	defer db.Close()

	dbCreatedTime, err := db.GetCreatedTime(ctx)
	if err != nil {
		return Error.Wrap(err)
	}

	// The cursor cannot be before the DB created time
	if cursor.Before(dbCreatedTime) {
		log.Desugar().Debug("Setting cursor to DB created time",
			zap.Time("Cursor", cursor),
			zap.Time("DB Created Time", dbCreatedTime))
		cursor = dbCreatedTime
	}

	// TODO: for some reason variable bind does not work
	rows, err := db.Query(ctx, `
		EXPERIMENTAL CHANGEFEED
		FOR blocks
		WITH
			envelope = key_only,
			cursor = `+fmt.Sprintf("'%d'", cursor.UnixNano()))
	if err != nil {
		return Error.Wrap(err)
	}
	defer rows.Close()

	var (
		table string
		key   string
		value string
	)

	for rows.Next() {
		if err := rows.Scan(&table, &key, &value); err != nil {
			return Error.Wrap(err)
		}

		cid := strings.Trim(key, "[\"]")

		binary, err := dshelp.BinaryFromDsKey(ds.NewKey(cid))
		if err != nil {
			return Error.Wrap(err)
		}

		mon.Counter("bloom_filter_add").Inc(1)
		log.Desugar().Debug("Updating bloom filter with CID", zap.String("CID", cid))

		updater.bloom.AddIfNotHasTS(binary)
	}

	return Error.Wrap(rows.Err())
}
