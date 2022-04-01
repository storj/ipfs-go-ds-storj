// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.
package db

import (
	"context"
	"strconv"

	_ "github.com/jackc/pgx/v4/stdlib" // registers pgx as a tagsql driver.
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/private/dbutil"
	_ "storj.io/private/dbutil/cockroachutil" // registers cockroach as a tagsql driver.
	"storj.io/private/migrate"
	"storj.io/private/tagsql"
)

// Error is the error class for datastore database.
var Error = errs.Class("db")

// DB is the datastore database for mapping IPFS blocks to Storj object packs.
type DB struct {
	tagsql.DB
}

// Open creates instance of the database.
func Open(ctx context.Context, databaseURL string) (*DB, error) {
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

	db, err := tagsql.Open(ctx, driverName, databaseURL)
	if err != nil {
		return nil, Error.Wrap(err)
	}

	return Wrap(db), nil
}

// MigrateToLatest migrates pindb to the latest version.
func (db *DB) MigrateToLatest(ctx context.Context) error {
	err := db.Migration().Run(ctx, zap.NewExample())
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
					)
				`},
			},
		},
	}
}

// Wrap turns a tagsql.DB into a DB struct.
func Wrap(db tagsql.DB) *DB {
	return &DB{DB: postgresRebind{DB: db}}
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
