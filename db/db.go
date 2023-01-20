// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.
package db

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	_ "github.com/jackc/pgx/v4/stdlib" // registers pgx as a tagsql driver.
	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"

	"storj.io/private/dbutil"
	_ "storj.io/private/dbutil/cockroachutil" // registers cockroach as a tagsql driver.
	"storj.io/private/migrate"
	"storj.io/private/tagsql"
)

var mon = monkit.Package()

var log = logging.Logger("storjds").Named("db")

// Error is the error class for datastore database.
var Error = errs.Class("db")

// DB is the datastore database for mapping IPFS blocks to Storj object packs.
type DB struct {
	tagsql.DB
}

// Open creates instance of the database.
func Open(ctx context.Context, databaseURL string) (db *DB, err error) {
	defer mon.Task()(&ctx)(&err)

	_, _, impl, err := dbutil.SplitConnStr(databaseURL)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	var driverName string
	switch impl {
	case dbutil.Postgres:
		driverName = "pgx"
	case dbutil.Cockroach:
		driverName = "cockroach"
	default:
		return nil, Error.New("unsupported implementation: %s", driverName)
	}

	tagdb, err := tagsql.Open(ctx, driverName, databaseURL)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return Wrap(tagdb), nil
}

// MigrateToLatest migrates pindb to the latest version.
func (db *DB) MigrateToLatest(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)

	err = db.Migration().Run(ctx, log.Desugar())

	return Error.Wrap(err)
}

// Migration returns steps needed for migrating the database.
func (db *DB) Migration() *migrate.Migration {
	return &migrate.Migration{
		Table: "versions",
		Steps: []*migrate.Step{
			{
				DB:          &db.DB,
				Description: "Initial setup",
				Version:     0,
				Action: migrate.SQL{`
					CREATE TABLE IF NOT EXISTS blocks (
						cid TEXT NOT NULL,
						size INTEGER NOT NULL,
						created TIMESTAMP NOT NULL DEFAULT NOW(),
						data BYTEA,
						deleted BOOLEAN NOT NULL DEFAULT false,
						pack_object TEXT NOT NULL DEFAULT '',
						pack_offset INTEGER NOT NULL DEFAULT 0,
						pack_status INTEGER NOT NULL DEFAULT 0,
						PRIMARY KEY ( cid )
					)`,
					`
					CREATE TABLE IF NOT EXISTS datastore (
						key TEXT NOT NULL,
						data BYTEA,
						PRIMARY KEY ( key )
					)`,
				},
			},
			{
				DB:          &db.DB,
				Description: "Index to optimize packing",
				Version:     1,
				Action: migrate.SQL{
					`CREATE INDEX IF NOT EXISTS blocks_size_created_status_index ON blocks ( size, created, pack_status )`,
				},
			},
			{
				DB:          &db.DB,
				Description: "Improve indexes",
				Version:     2,
				Action: migrate.SQL{
					`DROP INDEX IF EXISTS blocks_size_created_status_index`,
					`CREATE INDEX IF NOT EXISTS blocks_pack_status_index ON blocks ( pack_status ) INCLUDE ( size, created )`,
					`CREATE INDEX IF NOT EXISTS blocks_deleted_false_index ON blocks ( deleted ) WHERE deleted = false`,
				},
			},
		},
	}
}

// GetCreatedTime returns the time when the database was created.
func (db *DB) GetCreatedTime(ctx context.Context) (created time.Time, err error) {
	var committedAt string

	err = db.QueryRow(ctx, `
		SELECT commited_at
		FROM versions
		WHERE version = 0
	`).Scan(&committedAt)
	if err != nil {
		return time.Time{}, Error.Wrap(err)
	}

	// Trim the monolitic clock part
	committedAt = strings.Split(committedAt, " m=")[0]

	created, err = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", committedAt)
	if err != nil {
		return time.Time{}, Error.Wrap(err)
	}

	return created, nil
}

// Wrap turns a tagsql.DB into a DB struct.
func Wrap(db tagsql.DB) *DB {
	return &DB{
		DB: postgresRebind{DB: db},
	}
}

// This is needed for migrate to work.
// TODO: clean this up.
type postgresRebind struct{ tagsql.DB }

func (pq postgresRebind) Rebind(sql string) string {
	type sqlParseState int
	const (
		sqlParseStart sqlParseState = iota
		sqlParseInStringLiteral
		sqlParseInQuotedIdentifier
		sqlParseInComment
	)

	out := make([]byte, 0, len(sql)+10)

	j := 1
	state := sqlParseStart
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch state {
		case sqlParseStart:
			switch ch {
			case '?':
				out = append(out, '$')
				out = append(out, strconv.Itoa(j)...)
				state = sqlParseStart
				j++
				continue
			case '-':
				if i+1 < len(sql) && sql[i+1] == '-' {
					state = sqlParseInComment
				}
			case '"':
				state = sqlParseInQuotedIdentifier
			case '\'':
				state = sqlParseInStringLiteral
			}
		case sqlParseInStringLiteral:
			if ch == '\'' {
				state = sqlParseStart
			}
		case sqlParseInQuotedIdentifier:
			if ch == '"' {
				state = sqlParseStart
			}
		case sqlParseInComment:
			if ch == '\n' {
				state = sqlParseStart
			}
		}
		out = append(out, ch)
	}

	return string(out)
}

func isNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
