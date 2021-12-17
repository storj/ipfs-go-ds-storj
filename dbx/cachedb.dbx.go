// AUTOGENERATED BY storj.io/dbx
// DO NOT EDIT.

package dbx

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/mattn/go-sqlite3"
	"math/rand"
)

// Prevent conditional imports from causing build failures.
var _ = strconv.Itoa
var _ = strings.LastIndex
var _ = fmt.Sprint
var _ sync.Mutex

var (
	WrapErr = func(err *Error) error { return err }
	Logger  func(format string, args ...interface{})

	errTooManyRows       = errors.New("too many rows")
	errUnsupportedDriver = errors.New("unsupported driver")
	errEmptyUpdate       = errors.New("empty update")
)

func logError(format string, args ...interface{}) {
	if Logger != nil {
		Logger(format, args...)
	}
}

type ErrorCode int

const (
	ErrorCode_Unknown ErrorCode = iota
	ErrorCode_UnsupportedDriver
	ErrorCode_NoRows
	ErrorCode_TxDone
	ErrorCode_TooManyRows
	ErrorCode_ConstraintViolation
	ErrorCode_EmptyUpdate
)

type Error struct {
	Err         error
	Code        ErrorCode
	Driver      string
	Constraint  string
	QuerySuffix string
}

func (e *Error) Error() string {
	return e.Err.Error()
}

func (e *Error) Unwrap() error {
	return e.Err
}

func wrapErr(e *Error) error {
	if WrapErr == nil {
		return e
	}
	return WrapErr(e)
}

func makeErr(err error) error {
	if err == nil {
		return nil
	}
	e := &Error{Err: err}
	switch err {
	case sql.ErrNoRows:
		e.Code = ErrorCode_NoRows
	case sql.ErrTxDone:
		e.Code = ErrorCode_TxDone
	}
	return wrapErr(e)
}

func unsupportedDriver(driver string) error {
	return wrapErr(&Error{
		Err:    errUnsupportedDriver,
		Code:   ErrorCode_UnsupportedDriver,
		Driver: driver,
	})
}

func emptyUpdate() error {
	return wrapErr(&Error{
		Err:  errEmptyUpdate,
		Code: ErrorCode_EmptyUpdate,
	})
}

func tooManyRows(query_suffix string) error {
	return wrapErr(&Error{
		Err:         errTooManyRows,
		Code:        ErrorCode_TooManyRows,
		QuerySuffix: query_suffix,
	})
}

func constraintViolation(err error, constraint string) error {
	return wrapErr(&Error{
		Err:        err,
		Code:       ErrorCode_ConstraintViolation,
		Constraint: constraint,
	})
}

type driver interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

var (
	notAPointer     = errors.New("destination not a pointer")
	lossyConversion = errors.New("lossy conversion")
)

type DB struct {
	*sql.DB
	dbMethods

	Hooks struct {
		Now func() time.Time
	}
}

func Open(driver, source string) (db *DB, err error) {
	var sql_db *sql.DB
	switch driver {
	case "pgx":
		sql_db, err = openpgx(source)
	case "sqlite3":
		sql_db, err = opensqlite3(source)
	default:
		return nil, unsupportedDriver(driver)
	}
	if err != nil {
		return nil, makeErr(err)
	}
	defer func(sql_db *sql.DB) {
		if err != nil {
			_ = sql_db.Close()
		}
	}(sql_db)

	if err := sql_db.Ping(); err != nil {
		return nil, makeErr(err)
	}

	db = &DB{
		DB: sql_db,
	}
	db.Hooks.Now = time.Now

	switch driver {
	case "pgx":
		db.dbMethods = newpgx(db)
	case "sqlite3":
		db.dbMethods = newsqlite3(db)
	default:
		return nil, unsupportedDriver(driver)
	}

	return db, nil
}

func (obj *DB) Close() (err error) {
	return obj.makeErr(obj.DB.Close())
}

func (obj *DB) Open(ctx context.Context) (*Tx, error) {
	tx, err := obj.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, obj.makeErr(err)
	}

	return &Tx{
		Tx:        tx,
		txMethods: obj.wrapTx(tx),
	}, nil
}

func (obj *DB) NewRx() *Rx {
	return &Rx{db: obj}
}

func DeleteAll(ctx context.Context, db *DB) (int64, error) {
	tx, err := db.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err == nil {
			err = db.makeErr(tx.Commit())
			return
		}

		if err_rollback := tx.Rollback(); err_rollback != nil {
			logError("delete-all: rollback failed: %v", db.makeErr(err_rollback))
		}
	}()
	return tx.deleteAll(ctx)
}

type Tx struct {
	Tx *sql.Tx
	txMethods
}

type dialectTx struct {
	tx *sql.Tx
}

func (tx *dialectTx) Commit() (err error) {
	return makeErr(tx.tx.Commit())
}

func (tx *dialectTx) Rollback() (err error) {
	return makeErr(tx.tx.Rollback())
}

type pgxImpl struct {
	db      *DB
	dialect __sqlbundle_pgx
	driver  driver
}

func (obj *pgxImpl) Rebind(s string) string {
	return obj.dialect.Rebind(s)
}

func (obj *pgxImpl) logStmt(stmt string, args ...interface{}) {
	pgxLogStmt(stmt, args...)
}

func (obj *pgxImpl) makeErr(err error) error {
	constraint, ok := obj.isConstraintError(err)
	if ok {
		return constraintViolation(err, constraint)
	}
	return makeErr(err)
}

type pgxDB struct {
	db *DB
	*pgxImpl
}

func newpgx(db *DB) *pgxDB {
	return &pgxDB{
		db: db,
		pgxImpl: &pgxImpl{
			db:     db,
			driver: db.DB,
		},
	}
}

func (obj *pgxDB) Schema() string {
	return `CREATE TABLE blocks (
	cid text NOT NULL,
	size integer NOT NULL,
	created timestamp with time zone NOT NULL,
	data bytea,
	deleted boolean NOT NULL,
	pack_object text NOT NULL,
	pack_offset integer NOT NULL,
	pack_status integer NOT NULL,
	PRIMARY KEY ( cid )
);`
}

func (obj *pgxDB) wrapTx(tx *sql.Tx) txMethods {
	return &pgxTx{
		dialectTx: dialectTx{tx: tx},
		pgxImpl: &pgxImpl{
			db:     obj.db,
			driver: tx,
		},
	}
}

type pgxTx struct {
	dialectTx
	*pgxImpl
}

func pgxLogStmt(stmt string, args ...interface{}) {
	// TODO: render placeholders
	if Logger != nil {
		out := fmt.Sprintf("stmt: %s\nargs: %v\n", stmt, pretty(args))
		Logger(out)
	}
}

type sqlite3Impl struct {
	db      *DB
	dialect __sqlbundle_sqlite3
	driver  driver
}

func (obj *sqlite3Impl) Rebind(s string) string {
	return obj.dialect.Rebind(s)
}

func (obj *sqlite3Impl) logStmt(stmt string, args ...interface{}) {
	sqlite3LogStmt(stmt, args...)
}

func (obj *sqlite3Impl) makeErr(err error) error {
	constraint, ok := obj.isConstraintError(err)
	if ok {
		return constraintViolation(err, constraint)
	}
	return makeErr(err)
}

type sqlite3DB struct {
	db *DB
	*sqlite3Impl
}

func newsqlite3(db *DB) *sqlite3DB {
	return &sqlite3DB{
		db: db,
		sqlite3Impl: &sqlite3Impl{
			db:     db,
			driver: db.DB,
		},
	}
}

func (obj *sqlite3DB) Schema() string {
	return `CREATE TABLE blocks (
	cid TEXT NOT NULL,
	size INTEGER NOT NULL,
	created TIMESTAMP NOT NULL,
	data BLOB,
	deleted INTEGER NOT NULL,
	pack_object TEXT NOT NULL,
	pack_offset INTEGER NOT NULL,
	pack_status INTEGER NOT NULL,
	PRIMARY KEY ( cid )
);`
}

func (obj *sqlite3DB) wrapTx(tx *sql.Tx) txMethods {
	return &sqlite3Tx{
		dialectTx: dialectTx{tx: tx},
		sqlite3Impl: &sqlite3Impl{
			db:     obj.db,
			driver: tx,
		},
	}
}

type sqlite3Tx struct {
	dialectTx
	*sqlite3Impl
}

func sqlite3LogStmt(stmt string, args ...interface{}) {
	// TODO: render placeholders
	if Logger != nil {
		out := fmt.Sprintf("stmt: %s\nargs: %v\n", stmt, pretty(args))
		Logger(out)
	}
}

type pretty []interface{}

func (p pretty) Format(f fmt.State, c rune) {
	fmt.Fprint(f, "[")
nextval:
	for i, val := range p {
		if i > 0 {
			fmt.Fprint(f, ", ")
		}
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				fmt.Fprint(f, "NULL")
				continue
			}
			val = rv.Elem().Interface()
		}
		switch v := val.(type) {
		case string:
			fmt.Fprintf(f, "%q", v)
		case time.Time:
			fmt.Fprintf(f, "%s", v.Format(time.RFC3339Nano))
		case []byte:
			for _, b := range v {
				if !unicode.IsPrint(rune(b)) {
					fmt.Fprintf(f, "%#x", v)
					continue nextval
				}
			}
			fmt.Fprintf(f, "%q", v)
		default:
			fmt.Fprintf(f, "%v", v)
		}
	}
	fmt.Fprint(f, "]")
}

type Block struct {
	Cid        string
	Size       int
	Created    time.Time
	Data       []byte
	Deleted    bool
	PackObject string
	PackOffset int
	PackStatus int
}

func (Block) _Table() string { return "blocks" }

type Block_Create_Fields struct {
	Data Block_Data_Field
}

type Block_Update_Fields struct {
	Data       Block_Data_Field
	Deleted    Block_Deleted_Field
	PackObject Block_PackObject_Field
	PackOffset Block_PackOffset_Field
	PackStatus Block_PackStatus_Field
}

type Block_Cid_Field struct {
	_set   bool
	_null  bool
	_value string
}

func Block_Cid(v string) Block_Cid_Field {
	return Block_Cid_Field{_set: true, _value: v}
}

func (f Block_Cid_Field) value() interface{} {
	if !f._set || f._null {
		return nil
	}
	return f._value
}

func (Block_Cid_Field) _Column() string { return "cid" }

type Block_Size_Field struct {
	_set   bool
	_null  bool
	_value int
}

func Block_Size(v int) Block_Size_Field {
	return Block_Size_Field{_set: true, _value: v}
}

func (f Block_Size_Field) value() interface{} {
	if !f._set || f._null {
		return nil
	}
	return f._value
}

func (Block_Size_Field) _Column() string { return "size" }

type Block_Created_Field struct {
	_set   bool
	_null  bool
	_value time.Time
}

func Block_Created(v time.Time) Block_Created_Field {
	return Block_Created_Field{_set: true, _value: v}
}

func (f Block_Created_Field) value() interface{} {
	if !f._set || f._null {
		return nil
	}
	return f._value
}

func (Block_Created_Field) _Column() string { return "created" }

type Block_Data_Field struct {
	_set   bool
	_null  bool
	_value []byte
}

func Block_Data(v []byte) Block_Data_Field {
	return Block_Data_Field{_set: true, _value: v}
}

func Block_Data_Raw(v []byte) Block_Data_Field {
	if v == nil {
		return Block_Data_Null()
	}
	return Block_Data(v)
}

func Block_Data_Null() Block_Data_Field {
	return Block_Data_Field{_set: true, _null: true}
}

func (f Block_Data_Field) isnull() bool { return !f._set || f._null || f._value == nil }

func (f Block_Data_Field) value() interface{} {
	if !f._set || f._null {
		return nil
	}
	return f._value
}

func (Block_Data_Field) _Column() string { return "data" }

type Block_Deleted_Field struct {
	_set   bool
	_null  bool
	_value bool
}

func Block_Deleted(v bool) Block_Deleted_Field {
	return Block_Deleted_Field{_set: true, _value: v}
}

func (f Block_Deleted_Field) value() interface{} {
	if !f._set || f._null {
		return nil
	}
	return f._value
}

func (Block_Deleted_Field) _Column() string { return "deleted" }

type Block_PackObject_Field struct {
	_set   bool
	_null  bool
	_value string
}

func Block_PackObject(v string) Block_PackObject_Field {
	return Block_PackObject_Field{_set: true, _value: v}
}

func (f Block_PackObject_Field) value() interface{} {
	if !f._set || f._null {
		return nil
	}
	return f._value
}

func (Block_PackObject_Field) _Column() string { return "pack_object" }

type Block_PackOffset_Field struct {
	_set   bool
	_null  bool
	_value int
}

func Block_PackOffset(v int) Block_PackOffset_Field {
	return Block_PackOffset_Field{_set: true, _value: v}
}

func (f Block_PackOffset_Field) value() interface{} {
	if !f._set || f._null {
		return nil
	}
	return f._value
}

func (Block_PackOffset_Field) _Column() string { return "pack_offset" }

type Block_PackStatus_Field struct {
	_set   bool
	_null  bool
	_value int
}

func Block_PackStatus(v int) Block_PackStatus_Field {
	return Block_PackStatus_Field{_set: true, _value: v}
}

func (f Block_PackStatus_Field) value() interface{} {
	if !f._set || f._null {
		return nil
	}
	return f._value
}

func (Block_PackStatus_Field) _Column() string { return "pack_status" }

func toUTC(t time.Time) time.Time {
	return t.UTC()
}

func toDate(t time.Time) time.Time {
	// keep up the minute portion so that translations between timezones will
	// continue to reflect properly.
	return t.Truncate(time.Minute)
}

//
// runtime support for building sql statements
//

type __sqlbundle_SQL interface {
	Render() string

	private()
}

type __sqlbundle_Dialect interface {
	Rebind(sql string) string
}

type __sqlbundle_RenderOp int

const (
	__sqlbundle_NoFlatten __sqlbundle_RenderOp = iota
	__sqlbundle_NoTerminate
)

func __sqlbundle_Render(dialect __sqlbundle_Dialect, sql __sqlbundle_SQL, ops ...__sqlbundle_RenderOp) string {
	out := sql.Render()

	flatten := true
	terminate := true
	for _, op := range ops {
		switch op {
		case __sqlbundle_NoFlatten:
			flatten = false
		case __sqlbundle_NoTerminate:
			terminate = false
		}
	}

	if flatten {
		out = __sqlbundle_flattenSQL(out)
	}
	if terminate {
		out += ";"
	}

	return dialect.Rebind(out)
}

func __sqlbundle_flattenSQL(x string) string {
	// trim whitespace from beginning and end
	s, e := 0, len(x)-1
	for s < len(x) && (x[s] == ' ' || x[s] == '\t' || x[s] == '\n') {
		s++
	}
	for s <= e && (x[e] == ' ' || x[e] == '\t' || x[e] == '\n') {
		e--
	}
	if s > e {
		return ""
	}
	x = x[s : e+1]

	// check for whitespace that needs fixing
	wasSpace := false
	for i := 0; i < len(x); i++ {
		r := x[i]
		justSpace := r == ' '
		if (wasSpace && justSpace) || r == '\t' || r == '\n' {
			// whitespace detected, start writing a new string
			var result strings.Builder
			result.Grow(len(x))
			if wasSpace {
				result.WriteString(x[:i-1])
			} else {
				result.WriteString(x[:i])
			}
			for p := i; p < len(x); p++ {
				for p < len(x) && (x[p] == ' ' || x[p] == '\t' || x[p] == '\n') {
					p++
				}
				result.WriteByte(' ')

				start := p
				for p < len(x) && !(x[p] == ' ' || x[p] == '\t' || x[p] == '\n') {
					p++
				}
				result.WriteString(x[start:p])
			}

			return result.String()
		}
		wasSpace = justSpace
	}

	// no problematic whitespace found
	return x
}

// this type is specially named to match up with the name returned by the
// dialect impl in the sql package.
type __sqlbundle_postgres struct{}

func (p __sqlbundle_postgres) Rebind(sql string) string {
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

// this type is specially named to match up with the name returned by the
// dialect impl in the sql package.
type __sqlbundle_sqlite3 struct{}

func (s __sqlbundle_sqlite3) Rebind(sql string) string {
	return sql
}

// this type is specially named to match up with the name returned by the
// dialect impl in the sql package.
type __sqlbundle_cockroach struct{}

func (p __sqlbundle_cockroach) Rebind(sql string) string {
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

// this type is specially named to match up with the name returned by the
// dialect impl in the sql package.
type __sqlbundle_pgx struct{}

func (p __sqlbundle_pgx) Rebind(sql string) string {
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

// this type is specially named to match up with the name returned by the
// dialect impl in the sql package.
type __sqlbundle_pgxcockroach struct{}

func (p __sqlbundle_pgxcockroach) Rebind(sql string) string {
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

type __sqlbundle_Literal string

func (__sqlbundle_Literal) private() {}

func (l __sqlbundle_Literal) Render() string { return string(l) }

type __sqlbundle_Literals struct {
	Join string
	SQLs []__sqlbundle_SQL
}

func (__sqlbundle_Literals) private() {}

func (l __sqlbundle_Literals) Render() string {
	var out bytes.Buffer

	first := true
	for _, sql := range l.SQLs {
		if sql == nil {
			continue
		}
		if !first {
			out.WriteString(l.Join)
		}
		first = false
		out.WriteString(sql.Render())
	}

	return out.String()
}

type __sqlbundle_Condition struct {
	// set at compile/embed time
	Name  string
	Left  string
	Equal bool
	Right string

	// set at runtime
	Null bool
}

func (*__sqlbundle_Condition) private() {}

func (c *__sqlbundle_Condition) Render() string {
	// TODO(jeff): maybe check if we can use placeholders instead of the
	// literal null: this would make the templates easier.

	switch {
	case c.Equal && c.Null:
		return c.Left + " is null"
	case c.Equal && !c.Null:
		return c.Left + " = " + c.Right
	case !c.Equal && c.Null:
		return c.Left + " is not null"
	case !c.Equal && !c.Null:
		return c.Left + " != " + c.Right
	default:
		panic("unhandled case")
	}
}

type __sqlbundle_Hole struct {
	// set at compiile/embed time
	Name string

	// set at runtime or possibly embed time
	SQL __sqlbundle_SQL
}

func (*__sqlbundle_Hole) private() {}

func (h *__sqlbundle_Hole) Render() string {
	if h.SQL == nil {
		return ""
	}
	return h.SQL.Render()
}

//
// end runtime support for building sql statements
//

type Deleted_Row struct {
	Deleted bool
}

type Size_Deleted_Row struct {
	Size    int
	Deleted bool
}

func (obj *pgxImpl) Create_Block(ctx context.Context,
	block_cid Block_Cid_Field,
	block_size Block_Size_Field,
	optional Block_Create_Fields) (
	block *Block, err error) {

	__now := obj.db.Hooks.Now().UTC()
	__cid_val := block_cid.value()
	__size_val := block_size.value()
	__created_val := __now
	__data_val := optional.Data.value()
	__deleted_val := false
	__pack_object_val := ""
	__pack_offset_val := int(0)
	__pack_status_val := int(0)

	var __embed_stmt = __sqlbundle_Literal("INSERT INTO blocks ( cid, size, created, data, deleted, pack_object, pack_offset, pack_status ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? ) RETURNING blocks.cid, blocks.size, blocks.created, blocks.data, blocks.deleted, blocks.pack_object, blocks.pack_offset, blocks.pack_status")

	var __values []interface{}
	__values = append(__values, __cid_val, __size_val, __created_val, __data_val, __deleted_val, __pack_object_val, __pack_offset_val, __pack_status_val)

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	block = &Block{}
	err = obj.driver.QueryRowContext(ctx, __stmt, __values...).Scan(&block.Cid, &block.Size, &block.Created, &block.Data, &block.Deleted, &block.PackObject, &block.PackOffset, &block.PackStatus)
	if err != nil {
		return nil, obj.makeErr(err)
	}
	return block, nil

}

func (obj *pgxImpl) Get_Block_Deleted_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	row *Deleted_Row, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.deleted FROM blocks WHERE blocks.cid = ?")

	var __values []interface{}
	__values = append(__values, block_cid.value())

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	row = &Deleted_Row{}
	err = obj.driver.QueryRowContext(ctx, __stmt, __values...).Scan(&row.Deleted)
	if err != nil {
		return (*Deleted_Row)(nil), obj.makeErr(err)
	}
	return row, nil

}

func (obj *pgxImpl) Get_Block_Size_Block_Deleted_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	row *Size_Deleted_Row, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.size, blocks.deleted FROM blocks WHERE blocks.cid = ?")

	var __values []interface{}
	__values = append(__values, block_cid.value())

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	row = &Size_Deleted_Row{}
	err = obj.driver.QueryRowContext(ctx, __stmt, __values...).Scan(&row.Size, &row.Deleted)
	if err != nil {
		return (*Size_Deleted_Row)(nil), obj.makeErr(err)
	}
	return row, nil

}

func (obj *pgxImpl) Get_Block_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	block *Block, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.cid, blocks.size, blocks.created, blocks.data, blocks.deleted, blocks.pack_object, blocks.pack_offset, blocks.pack_status FROM blocks WHERE blocks.cid = ?")

	var __values []interface{}
	__values = append(__values, block_cid.value())

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	block = &Block{}
	err = obj.driver.QueryRowContext(ctx, __stmt, __values...).Scan(&block.Cid, &block.Size, &block.Created, &block.Data, &block.Deleted, &block.PackObject, &block.PackOffset, &block.PackStatus)
	if err != nil {
		return (*Block)(nil), obj.makeErr(err)
	}
	return block, nil

}

func (obj *pgxImpl) Limited_Block_By_PackStatus_Equal_Number_OrderBy_Asc_Created(ctx context.Context,
	limit int, offset int64) (
	rows []*Block, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.cid, blocks.size, blocks.created, blocks.data, blocks.deleted, blocks.pack_object, blocks.pack_offset, blocks.pack_status FROM blocks WHERE blocks.pack_status = 0 ORDER BY blocks.created LIMIT ? OFFSET ?")

	var __values []interface{}

	__values = append(__values, limit, offset)

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	__rows, err := obj.driver.QueryContext(ctx, __stmt, __values...)
	if err != nil {
		return nil, obj.makeErr(err)
	}
	defer __rows.Close()

	for __rows.Next() {
		block := &Block{}
		err = __rows.Scan(&block.Cid, &block.Size, &block.Created, &block.Data, &block.Deleted, &block.PackObject, &block.PackOffset, &block.PackStatus)
		if err != nil {
			return nil, obj.makeErr(err)
		}
		rows = append(rows, block)
	}
	if err := __rows.Err(); err != nil {
		return nil, obj.makeErr(err)
	}
	return rows, nil

}

func (obj *pgxImpl) Delete_Block_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	deleted bool, err error) {

	var __embed_stmt = __sqlbundle_Literal("DELETE FROM blocks WHERE blocks.cid = ?")

	var __values []interface{}
	__values = append(__values, block_cid.value())

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	__res, err := obj.driver.ExecContext(ctx, __stmt, __values...)
	if err != nil {
		return false, obj.makeErr(err)
	}

	__count, err := __res.RowsAffected()
	if err != nil {
		return false, obj.makeErr(err)
	}

	return __count > 0, nil

}

func (impl pgxImpl) isConstraintError(err error) (
	constraint string, ok bool) {
	if e, ok := err.(*pgconn.PgError); ok {
		if e.Code[:2] == "23" {
			return e.ConstraintName, true
		}
	}
	return "", false
}

func (obj *pgxImpl) deleteAll(ctx context.Context) (count int64, err error) {
	var __res sql.Result
	var __count int64
	__res, err = obj.driver.ExecContext(ctx, "DELETE FROM blocks;")
	if err != nil {
		return 0, obj.makeErr(err)
	}

	__count, err = __res.RowsAffected()
	if err != nil {
		return 0, obj.makeErr(err)
	}
	count += __count

	return count, nil

}

func (obj *sqlite3Impl) Create_Block(ctx context.Context,
	block_cid Block_Cid_Field,
	block_size Block_Size_Field,
	optional Block_Create_Fields) (
	block *Block, err error) {

	__now := obj.db.Hooks.Now().UTC()
	__cid_val := block_cid.value()
	__size_val := block_size.value()
	__created_val := __now
	__data_val := optional.Data.value()
	__deleted_val := false
	__pack_object_val := ""
	__pack_offset_val := int(0)
	__pack_status_val := int(0)

	var __embed_stmt = __sqlbundle_Literal("INSERT INTO blocks ( cid, size, created, data, deleted, pack_object, pack_offset, pack_status ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )")

	var __values []interface{}
	__values = append(__values, __cid_val, __size_val, __created_val, __data_val, __deleted_val, __pack_object_val, __pack_offset_val, __pack_status_val)

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	__res, err := obj.driver.ExecContext(ctx, __stmt, __values...)
	if err != nil {
		return nil, obj.makeErr(err)
	}
	__pk, err := __res.LastInsertId()
	if err != nil {
		return nil, obj.makeErr(err)
	}
	return obj.getLastBlock(ctx, __pk)

}

func (obj *sqlite3Impl) Get_Block_Deleted_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	row *Deleted_Row, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.deleted FROM blocks WHERE blocks.cid = ?")

	var __values []interface{}
	__values = append(__values, block_cid.value())

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	row = &Deleted_Row{}
	err = obj.driver.QueryRowContext(ctx, __stmt, __values...).Scan(&row.Deleted)
	if err != nil {
		return (*Deleted_Row)(nil), obj.makeErr(err)
	}
	return row, nil

}

func (obj *sqlite3Impl) Get_Block_Size_Block_Deleted_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	row *Size_Deleted_Row, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.size, blocks.deleted FROM blocks WHERE blocks.cid = ?")

	var __values []interface{}
	__values = append(__values, block_cid.value())

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	row = &Size_Deleted_Row{}
	err = obj.driver.QueryRowContext(ctx, __stmt, __values...).Scan(&row.Size, &row.Deleted)
	if err != nil {
		return (*Size_Deleted_Row)(nil), obj.makeErr(err)
	}
	return row, nil

}

func (obj *sqlite3Impl) Get_Block_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	block *Block, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.cid, blocks.size, blocks.created, blocks.data, blocks.deleted, blocks.pack_object, blocks.pack_offset, blocks.pack_status FROM blocks WHERE blocks.cid = ?")

	var __values []interface{}
	__values = append(__values, block_cid.value())

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	block = &Block{}
	err = obj.driver.QueryRowContext(ctx, __stmt, __values...).Scan(&block.Cid, &block.Size, &block.Created, &block.Data, &block.Deleted, &block.PackObject, &block.PackOffset, &block.PackStatus)
	if err != nil {
		return (*Block)(nil), obj.makeErr(err)
	}
	return block, nil

}

func (obj *sqlite3Impl) Limited_Block_By_PackStatus_Equal_Number_OrderBy_Asc_Created(ctx context.Context,
	limit int, offset int64) (
	rows []*Block, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.cid, blocks.size, blocks.created, blocks.data, blocks.deleted, blocks.pack_object, blocks.pack_offset, blocks.pack_status FROM blocks WHERE blocks.pack_status = 0 ORDER BY blocks.created LIMIT ? OFFSET ?")

	var __values []interface{}

	__values = append(__values, limit, offset)

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	__rows, err := obj.driver.QueryContext(ctx, __stmt, __values...)
	if err != nil {
		return nil, obj.makeErr(err)
	}
	defer __rows.Close()

	for __rows.Next() {
		block := &Block{}
		err = __rows.Scan(&block.Cid, &block.Size, &block.Created, &block.Data, &block.Deleted, &block.PackObject, &block.PackOffset, &block.PackStatus)
		if err != nil {
			return nil, obj.makeErr(err)
		}
		rows = append(rows, block)
	}
	if err := __rows.Err(); err != nil {
		return nil, obj.makeErr(err)
	}
	return rows, nil

}

func (obj *sqlite3Impl) Delete_Block_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	deleted bool, err error) {

	var __embed_stmt = __sqlbundle_Literal("DELETE FROM blocks WHERE blocks.cid = ?")

	var __values []interface{}
	__values = append(__values, block_cid.value())

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, __values...)

	__res, err := obj.driver.ExecContext(ctx, __stmt, __values...)
	if err != nil {
		return false, obj.makeErr(err)
	}

	__count, err := __res.RowsAffected()
	if err != nil {
		return false, obj.makeErr(err)
	}

	return __count > 0, nil

}

func (obj *sqlite3Impl) getLastBlock(ctx context.Context,
	pk int64) (
	block *Block, err error) {

	var __embed_stmt = __sqlbundle_Literal("SELECT blocks.cid, blocks.size, blocks.created, blocks.data, blocks.deleted, blocks.pack_object, blocks.pack_offset, blocks.pack_status FROM blocks WHERE _rowid_ = ?")

	var __stmt = __sqlbundle_Render(obj.dialect, __embed_stmt)
	obj.logStmt(__stmt, pk)

	block = &Block{}
	err = obj.driver.QueryRowContext(ctx, __stmt, pk).Scan(&block.Cid, &block.Size, &block.Created, &block.Data, &block.Deleted, &block.PackObject, &block.PackOffset, &block.PackStatus)
	if err != nil {
		return (*Block)(nil), obj.makeErr(err)
	}
	return block, nil

}

func (impl sqlite3Impl) isConstraintError(err error) (
	constraint string, ok bool) {
	if e, ok := err.(sqlite3.Error); ok {
		if e.Code == sqlite3.ErrConstraint {
			msg := err.Error()
			colon := strings.LastIndex(msg, ":")
			if colon != -1 {
				return strings.TrimSpace(msg[colon:]), true
			}
			return "", true
		}
	}
	return "", false
}

func (obj *sqlite3Impl) deleteAll(ctx context.Context) (count int64, err error) {
	var __res sql.Result
	var __count int64
	__res, err = obj.driver.ExecContext(ctx, "DELETE FROM blocks;")
	if err != nil {
		return 0, obj.makeErr(err)
	}

	__count, err = __res.RowsAffected()
	if err != nil {
		return 0, obj.makeErr(err)
	}
	count += __count

	return count, nil

}

type Rx struct {
	db *DB
	tx *Tx
}

func (rx *Rx) UnsafeTx(ctx context.Context) (unsafe_tx *sql.Tx, err error) {
	tx, err := rx.getTx(ctx)
	if err != nil {
		return nil, err
	}
	return tx.Tx, nil
}

func (rx *Rx) getTx(ctx context.Context) (tx *Tx, err error) {
	if rx.tx == nil {
		if rx.tx, err = rx.db.Open(ctx); err != nil {
			return nil, err
		}
	}
	return rx.tx, nil
}

func (rx *Rx) Rebind(s string) string {
	return rx.db.Rebind(s)
}

func (rx *Rx) Commit() (err error) {
	if rx.tx != nil {
		err = rx.tx.Commit()
		rx.tx = nil
	}
	return err
}

func (rx *Rx) Rollback() (err error) {
	if rx.tx != nil {
		err = rx.tx.Rollback()
		rx.tx = nil
	}
	return err
}

func (rx *Rx) Create_Block(ctx context.Context,
	block_cid Block_Cid_Field,
	block_size Block_Size_Field,
	optional Block_Create_Fields) (
	block *Block, err error) {
	var tx *Tx
	if tx, err = rx.getTx(ctx); err != nil {
		return
	}
	return tx.Create_Block(ctx, block_cid, block_size, optional)

}

func (rx *Rx) Delete_Block_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	deleted bool, err error) {
	var tx *Tx
	if tx, err = rx.getTx(ctx); err != nil {
		return
	}
	return tx.Delete_Block_By_Cid(ctx, block_cid)
}

func (rx *Rx) Get_Block_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	block *Block, err error) {
	var tx *Tx
	if tx, err = rx.getTx(ctx); err != nil {
		return
	}
	return tx.Get_Block_By_Cid(ctx, block_cid)
}

func (rx *Rx) Get_Block_Deleted_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	row *Deleted_Row, err error) {
	var tx *Tx
	if tx, err = rx.getTx(ctx); err != nil {
		return
	}
	return tx.Get_Block_Deleted_By_Cid(ctx, block_cid)
}

func (rx *Rx) Get_Block_Size_Block_Deleted_By_Cid(ctx context.Context,
	block_cid Block_Cid_Field) (
	row *Size_Deleted_Row, err error) {
	var tx *Tx
	if tx, err = rx.getTx(ctx); err != nil {
		return
	}
	return tx.Get_Block_Size_Block_Deleted_By_Cid(ctx, block_cid)
}

func (rx *Rx) Limited_Block_By_PackStatus_Equal_Number_OrderBy_Asc_Created(ctx context.Context,
	limit int, offset int64) (
	rows []*Block, err error) {
	var tx *Tx
	if tx, err = rx.getTx(ctx); err != nil {
		return
	}
	return tx.Limited_Block_By_PackStatus_Equal_Number_OrderBy_Asc_Created(ctx, limit, offset)
}

type Methods interface {
	Create_Block(ctx context.Context,
		block_cid Block_Cid_Field,
		block_size Block_Size_Field,
		optional Block_Create_Fields) (
		block *Block, err error)

	Delete_Block_By_Cid(ctx context.Context,
		block_cid Block_Cid_Field) (
		deleted bool, err error)

	Get_Block_By_Cid(ctx context.Context,
		block_cid Block_Cid_Field) (
		block *Block, err error)

	Get_Block_Deleted_By_Cid(ctx context.Context,
		block_cid Block_Cid_Field) (
		row *Deleted_Row, err error)

	Get_Block_Size_Block_Deleted_By_Cid(ctx context.Context,
		block_cid Block_Cid_Field) (
		row *Size_Deleted_Row, err error)

	Limited_Block_By_PackStatus_Equal_Number_OrderBy_Asc_Created(ctx context.Context,
		limit int, offset int64) (
		rows []*Block, err error)
}

type TxMethods interface {
	Methods

	Rebind(s string) string
	Commit() error
	Rollback() error
}

type txMethods interface {
	TxMethods

	deleteAll(ctx context.Context) (int64, error)
	makeErr(err error) error
}

type DBMethods interface {
	Methods

	Schema() string
	Rebind(sql string) string
}

type dbMethods interface {
	DBMethods

	wrapTx(tx *sql.Tx) txMethods
	makeErr(err error) error
}

func openpgx(source string) (*sql.DB, error) {
	return sql.Open("pgx", source)
}

var sqlite3DriverName = func() string {
	var id [16]byte
	rand.Read(id[:])
	return fmt.Sprintf("sqlite3_%x", string(id[:]))
}()

func init() {
	sql.Register(sqlite3DriverName, &sqlite3.SQLiteDriver{
		ConnectHook: sqlite3SetupConn,
	})
}

// SQLite3JournalMode controls the journal_mode pragma for all new connections.
// Since it is read without a mutex, it must be changed to the value you want
// before any Open calls.
var SQLite3JournalMode = "WAL"

func sqlite3SetupConn(conn *sqlite3.SQLiteConn) (err error) {
	_, err = conn.Exec("PRAGMA foreign_keys = ON", nil)
	if err != nil {
		return makeErr(err)
	}
	_, err = conn.Exec("PRAGMA journal_mode = "+SQLite3JournalMode, nil)
	if err != nil {
		return makeErr(err)
	}
	return nil
}

func opensqlite3(source string) (*sql.DB, error) {
	return sql.Open(sqlite3DriverName, source)
}
