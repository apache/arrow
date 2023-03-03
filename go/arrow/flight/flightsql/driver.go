// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package flightsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"

	"google.golang.org/grpc"
)

var (
	ErrNotSupported          = errors.New("not supported")
	ErrOutOfRange            = errors.New("index out of range")
	ErrTransactionInProgress = errors.New("transaction still in progress")
)

type Rows struct {
	schema        *arrow.Schema
	records       []arrow.Record
	currentRecord int
	currentRow    int
}

// Columns returns the names of the columns.
func (r *Rows) Columns() []string {
	if len(r.records) == 0 {
		return nil
	}

	// All records have the same columns
	var cols []string
	for _, c := range r.schema.Fields() {
		cols = append(cols, c.Name)
	}

	return cols
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	for _, rec := range r.records {
		rec.Release()
	}
	r.currentRecord = 0
	r.currentRow = 0

	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *Rows) Next(dest []driver.Value) error {
	if r.currentRecord >= len(r.records) {
		return io.EOF
	}
	record := r.records[r.currentRecord]

	if int64(r.currentRow) >= record.NumRows() {
		return ErrOutOfRange
	}

	for i, arr := range record.Columns() {
		v, err := fromArrowType(arr, r.currentRow)
		if err != nil {
			return err
		}
		dest[i] = v
	}

	r.currentRow++
	if int64(r.currentRow) >= record.NumRows() {
		r.currentRecord++
		r.currentRow = 0
	}

	return nil
}

type Result struct {
	affected   int64
	lastinsert int64
}

// LastInsertId returns the database's auto-generated ID after, for example,
// an INSERT into a table with primary key.
func (r *Result) LastInsertId() (int64, error) {
	if r.lastinsert < 0 {
		return -1, ErrNotSupported
	}
	return r.lastinsert, nil
}

// RowsAffected returns the number of rows affected by the query.
func (r *Result) RowsAffected() (int64, error) {
	if r.affected < 0 {
		return -1, ErrNotSupported
	}
	return r.affected, nil
}

type Stmt struct {
	stmt   *PreparedStatement
	client *Client

	timeout time.Duration
}

// Close closes the statement.
func (s *Stmt) Close() error {
	ctx := context.Background()
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	return s.stmt.Close(ctx)
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	schema := s.stmt.ParameterSchema()
	if schema == nil {
		// NumInput may also return -1, if the driver doesn't know its number
		// of placeholders. In that case, the sql package will not sanity check
		// Exec or Query argument counts.
		return -1
	}

	// If NumInput returns >= 0, the sql package will sanity check argument
	// counts from callers and return errors to the caller before the
	// statement's Exec or Query methods are called.
	return len(schema.Fields())
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	var params []driver.NamedValue
	for i, arg := range args {
		params = append(params, driver.NamedValue{
			Name:    fmt.Sprintf("arg_%d", i),
			Ordinal: i,
			Value:   arg,
		})
	}

	return s.ExecContext(context.Background(), params)
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.setParameters(args); err != nil {
		return nil, err
	}

	if _, set := ctx.Deadline(); !set && s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	n, err := s.stmt.ExecuteUpdate(ctx)
	if err != nil {
		return nil, err
	}

	// FIXME: For now we ignore the number of affected records as it seems like
	// the returned value is always one.
	_ = n

	return &Result{affected: -1, lastinsert: -1}, nil
}

// Query executes a query that may return rows, such as a SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	var params []driver.NamedValue
	for i, arg := range args {
		params = append(params, driver.NamedValue{
			Name:    fmt.Sprintf("arg_%d", i),
			Ordinal: i,
			Value:   arg,
		})
	}

	return s.QueryContext(context.Background(), params)
}

// QueryContext executes a query that may return rows, such as a SELECT.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.setParameters(args); err != nil {
		return nil, err
	}

	if _, set := ctx.Deadline(); !set && s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	info, err := s.stmt.Execute(ctx)
	if err != nil {
		return nil, err
	}

	rows := Rows{}
	for _, endpoint := range info.Endpoint {
		reader, err := s.client.DoGet(ctx, endpoint.GetTicket())
		if err != nil {
			return nil, fmt.Errorf("getting ticket failed: %w", err)
		}
		record, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("reading record failed: %w", err)
		}

		if rows.schema == nil {
			rows.schema = record.Schema()
		}
		if !rows.schema.Equal(record.Schema()) {
			return nil, fmt.Errorf("mixed schemas %w", ErrNotSupported)
		}
		rows.records = append(rows.records, record)
	}

	return &rows, nil
}

func (s *Stmt) setParameters(args []driver.NamedValue) error {
	if len(args) == 0 {
		s.stmt.SetParameters(nil)
		return nil
	}

	var fields []arrow.Field
	sort.SliceStable(args, func(i, j int) bool {
		return args[i].Ordinal < args[j].Ordinal
	})

	for _, arg := range args {
		dt, err := toArrowDataType(arg.Value)
		if err != nil {
			return fmt.Errorf("schema: %w", err)
		}
		fields = append(fields, arrow.Field{
			Name:     arg.Name,
			Type:     dt,
			Nullable: true,
		})
	}

	schema := s.stmt.ParameterSchema()
	if schema == nil {
		schema = arrow.NewSchema(fields, nil)
	}

	recBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer recBuilder.Release()

	for i, arg := range args {
		fieldBuilder := recBuilder.Field(i)
		if err := setFieldValue(fieldBuilder, arg.Value); err != nil {
			return err
		}
	}

	rec := recBuilder.NewRecord()
	defer rec.Release()

	s.stmt.SetParameters(rec)

	return nil
}

type Tx struct {
	tx      *Txn
	timeout time.Duration
}

func (t *Tx) Commit() error {
	ctx := context.Background()
	if t.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}

	return t.tx.Commit(ctx)
}

func (t *Tx) Rollback() error {
	ctx := context.Background()
	if t.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}

	return t.tx.Rollback(ctx)
}

type Driver struct {
	addr    string
	timeout time.Duration
	options []grpc.DialOption

	client *Client
	txn    *Txn
}

// Open returns a new connection to the database.
func (d *Driver) Open(name string) (driver.Conn, error) {
	if _, err := d.OpenConnector(name); err != nil {
		return nil, err
	}

	return d.Connect(context.Background())
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	config, err := NewDriverConfigFromDSN(name)
	if err != nil {
		return nil, err
	}

	if err := d.Configure(config); err != nil {
		return nil, err
	}

	return d, nil
}

// Connect returns a connection to the database.
func (d *Driver) Connect(ctx context.Context) (driver.Conn, error) {
	if _, set := ctx.Deadline(); !set && d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}

	client, err := NewClientCtx(ctx, d.addr, nil, nil, d.options...)
	if err != nil {
		return nil, err
	}
	d.client = client

	return d, nil
}

// Configure the driver with the corresponding config
func (d *Driver) Configure(config *DriverConfig) error {
	// Set the driver properties
	d.addr = config.Address
	d.timeout = config.Timeout

	// Create GRPC options necessary for the backend
	var err error
	switch config.Backend {
	case "sqlite":
		d.options, err = newSqliteBackend(config)
		if err != nil {
			return err
		}
	case "iox", "ioxs":
		d.options, err = newIOxBackend(config)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid backend %q", config.Backend)
	}
	d.options = append(d.options, grpc.WithBlock())

	return nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (d *Driver) Driver() driver.Driver {
	return d
}

// Prepare returns a prepared statement, bound to this connection.
func (d *Driver) Prepare(query string) (driver.Stmt, error) {
	return d.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (d *Driver) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if _, set := ctx.Deadline(); !set && d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}

	var err error
	var stmt *PreparedStatement
	if d.txn != nil && d.txn.txn.IsValid() {
		stmt, err = d.txn.Prepare(ctx, query)
	} else {
		stmt, err = d.client.Prepare(ctx, query)
		d.txn = nil
	}
	if err != nil {
		return nil, err
	}

	return &Stmt{
		stmt:    stmt,
		client:  d.client,
		timeout: d.timeout,
	}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (d *Driver) Close() error {
	d.addr = ""
	d.options = nil
	if d.client == nil {
		return nil
	}

	if d.txn != nil && d.txn.txn.IsValid() {
		return ErrTransactionInProgress
	}

	// Drivers must ensure all network calls made by Close
	// do not block indefinitely (e.g. apply a timeout).
	err := d.client.Close()
	d.client = nil

	return err
}

// Begin starts and returns a new transaction.
func (d *Driver) Begin() (driver.Tx, error) {
	return d.BeginTx(context.Background(), sql.TxOptions{})
}

func (d *Driver) BeginTx(ctx context.Context, opts sql.TxOptions) (driver.Tx, error) {
	tx, err := d.client.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}
	d.txn = tx

	return &Tx{tx: tx, timeout: d.timeout}, nil
}

func fromArrowType(arr arrow.Array, idx int) (interface{}, error) {
	switch c := arr.(type) {
	case *array.Boolean:
		return c.Value(idx), nil
	case *array.Float16:
		return float64(c.Value(idx).Float32()), nil
	case *array.Float32:
		return float64(c.Value(idx)), nil
	case *array.Float64:
		return c.Value(idx), nil
	case *array.Int8:
		return int64(c.Value(idx)), nil
	case *array.Int16:
		return int64(c.Value(idx)), nil
	case *array.Int32:
		return int64(c.Value(idx)), nil
	case *array.Int64:
		return c.Value(idx), nil
	case *array.String:
		return c.Value(idx), nil
	case *array.Time32:
		dt, ok := arr.DataType().(*arrow.Time32Type)
		if !ok {
			return nil, fmt.Errorf("datatype %T not matching time32", arr.DataType())
		}
		v := c.Value(idx)
		return v.ToTime(dt.TimeUnit()), nil
	case *array.Time64:
		dt, ok := arr.DataType().(*arrow.Time64Type)
		if !ok {
			return nil, fmt.Errorf("datatype %T not matching time64", arr.DataType())
		}
		v := c.Value(idx)
		return v.ToTime(dt.TimeUnit()), nil
	case *array.Timestamp:
		dt, ok := arr.DataType().(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("datatype %T not matching timestamp", arr.DataType())
		}
		v := c.Value(idx)
		return v.ToTime(dt.TimeUnit()), nil
	}

	return nil, fmt.Errorf("type %T: %w", arr, ErrNotSupported)
}

func toArrowDataType(value interface{}) (arrow.DataType, error) {
	switch value.(type) {
	case bool:
		return &arrow.BooleanType{}, nil
	case float32:
		return &arrow.Float32Type{}, nil
	case float64:
		return &arrow.Float64Type{}, nil
	case int8:
		return &arrow.Int8Type{}, nil
	case int16:
		return &arrow.Int16Type{}, nil
	case int32:
		return &arrow.Int32Type{}, nil
	case int64:
		return &arrow.Int64Type{}, nil
	case uint8:
		return &arrow.Uint8Type{}, nil
	case uint16:
		return &arrow.Uint16Type{}, nil
	case uint32:
		return &arrow.Uint32Type{}, nil
	case uint64:
		return &arrow.Uint64Type{}, nil
	case string:
		return &arrow.StringType{}, nil
	case time.Time:
		return &arrow.Time64Type{Unit: arrow.Nanosecond}, nil
	}
	return nil, fmt.Errorf("type %T: %w", value, ErrNotSupported)
}

func setFieldValue(builder array.Builder, arg interface{}) error {
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		switch v := arg.(type) {
		case bool:
			b.Append(v)
		case []bool:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Float32Builder:
		switch v := arg.(type) {
		case float32:
			b.Append(v)
		case []float32:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Float64Builder:
		switch v := arg.(type) {
		case float64:
			b.Append(v)
		case []float64:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Int8Builder:
		switch v := arg.(type) {
		case int8:
			b.Append(v)
		case []int8:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Int16Builder:
		switch v := arg.(type) {
		case int16:
			b.Append(v)
		case []int16:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Int32Builder:
		switch v := arg.(type) {
		case int32:
			b.Append(v)
		case []int32:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Int64Builder:
		switch v := arg.(type) {
		case int64:
			b.Append(v)
		case []int64:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Uint8Builder:
		switch v := arg.(type) {
		case uint8:
			b.Append(v)
		case []uint8:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Uint16Builder:
		switch v := arg.(type) {
		case uint16:
			b.Append(v)
		case []uint16:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Uint32Builder:
		switch v := arg.(type) {
		case uint32:
			b.Append(v)
		case []uint32:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Uint64Builder:
		switch v := arg.(type) {
		case uint64:
			b.Append(v)
		case []uint64:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.StringBuilder:
		switch v := arg.(type) {
		case string:
			b.Append(v)
		case []string:
			b.AppendValues(v, nil)
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	case *array.Time64Builder:
		switch v := arg.(type) {
		case int64:
			b.Append(arrow.Time64(v))
		case []int64:
			for _, x := range v {
				b.Append(arrow.Time64(x))
			}
		case uint64:
			b.Append(arrow.Time64(v))
		case []uint64:
			for _, x := range v {
				b.Append(arrow.Time64(x))
			}
		case time.Time:
			b.Append(arrow.Time64(v.Nanosecond()))
		default:
			return fmt.Errorf("invalid value type %T for builder %T", arg, builder)
		}
	}
	return nil
}

type DriverConfig struct {
	Backend  string
	Address  string
	Username string
	Password string
	Database string
	Token    string
	Timeout  time.Duration
}

func NewDriverConfigFromDSN(dsn string) (*DriverConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	// Sanity checks on the given connection string
	switch u.Scheme {
	case "sqlite", "iox", "ioxs":
	default:
		return nil, fmt.Errorf("invalid scheme %q", u.Scheme)
	}

	// Determine the URL parameters
	var username, password string
	if u.User != nil {
		username = u.User.Username()
		if v, set := u.User.Password(); set {
			password = v
		}
	}

	var database string
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) >= 1 && parts[0] != "" {
		database = parts[0]
	}

	var timeout time.Duration
	if u.Query().Has("timeout") {
		timeout, err = time.ParseDuration(u.Query().Get("timeout"))
		if err != nil {
			return nil, err
		}
	}

	config := &DriverConfig{
		Backend:  u.Scheme,
		Address:  u.Host,
		Username: username,
		Password: password,
		Database: database,
		Timeout:  timeout,
	}

	return config, nil
}

func (config *DriverConfig) ToDSN() (string, error) {
	switch config.Backend {
	case "sqlite":
		return generateSqliteDSN(config)
	case "iox", "ioxs":
		return generateIOxDSN(config)
	}
	return "", fmt.Errorf("invalid backend %q", config.Backend)
}

// Register the driver on load.
func init() {
	sql.Register("flightsql", &Driver{})
}
