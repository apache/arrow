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
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const dsnPattern = "flightsq://[username[:password]@]address[:port][?param1=value1&...&paramN=valueN]"

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

	return &Result{affected: n, lastinsert: -1}, nil
}

// Query executes a query that may return rows, such as a SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	var params []driver.NamedValue
	for i, arg := range args {
		params = append(params, driver.NamedValue{
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

		rows.schema = reader.Schema()
		for reader.Next() {
			record := reader.Record()
			record.Retain()
			rows.records = append(rows.records, record)

		}
		if err := reader.Err(); err != nil {
			return &rows, err
		}
	}

	return &rows, nil
}

func (s *Stmt) setParameters(args []driver.NamedValue) error {
	if len(args) == 0 {
		s.stmt.SetParameters(nil)
		return nil
	}

	sort.SliceStable(args, func(i, j int) bool {
		return args[i].Ordinal < args[j].Ordinal
	})

	schema := s.stmt.ParameterSchema()
	if schema == nil {
		var fields []arrow.Field
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

type Driver struct{}

// Open returns a new connection to the database.
func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	return c.Connect(context.Background())
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	config, err := NewDriverConfigFromDSN(name)
	if err != nil {
		return nil, err
	}

	c := &Connector{}
	if err := c.Configure(config); err != nil {
		return nil, err
	}

	return c, nil
}

type Connector struct {
	addr    string
	timeout time.Duration
	options []grpc.DialOption
}

// Configure the driver with the corresponding config
func (c *Connector) Configure(config *DriverConfig) error {
	// Set the driver properties
	c.addr = config.Address
	c.timeout = config.Timeout
	c.options = []grpc.DialOption{grpc.WithBlock()}

	// Create GRPC options necessary for the backend
	var transportCreds credentials.TransportCredentials
	if !config.TLSEnabled {
		transportCreds = insecure.NewCredentials()
	} else {
		transportCreds = credentials.NewTLS(config.TLSConfig)
	}
	c.options = append(c.options, grpc.WithTransportCredentials(transportCreds))

	// Set authentication credentials
	rpcCreds := grpcCredentials{
		username: config.Username,
		password: config.Password,
		token:    config.Token,
		params:   config.Params,
	}
	c.options = append(c.options, grpc.WithPerRPCCredentials(rpcCreds))

	return nil
}

// Connect returns a connection to the database.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if _, set := ctx.Deadline(); !set && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	client, err := NewClientCtx(ctx, c.addr, nil, nil, c.options...)
	if err != nil {
		return nil, err
	}

	return &Connection{
		client:  client,
		timeout: c.timeout,
	}, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *Connector) Driver() driver.Driver {
	return &Driver{}
}

type Connection struct {
	client *Client
	txn    *Txn

	timeout time.Duration
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement,
// it must not store the context within the statement itself.
func (c *Connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if _, set := ctx.Deadline(); !set && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	var err error
	var stmt *PreparedStatement
	if c.txn != nil && c.txn.txn.IsValid() {
		stmt, err = c.txn.Prepare(ctx, query)
	} else {
		stmt, err = c.client.Prepare(ctx, query)
		c.txn = nil
	}
	if err != nil {
		return nil, err
	}

	return &Stmt{
		stmt:    stmt,
		client:  c.client,
		timeout: c.timeout,
	}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *Connection) Close() error {
	if c.txn != nil && c.txn.txn.IsValid() {
		return ErrTransactionInProgress
	}

	if c.client == nil {
		return nil
	}

	err := c.client.Close()
	c.client = nil

	return err
}

// Begin starts and returns a new transaction.
func (c *Connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), sql.TxOptions{})
}

func (c *Connection) BeginTx(ctx context.Context, opts sql.TxOptions) (driver.Tx, error) {
	tx, err := c.client.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}
	c.txn = tx

	return &Tx{tx: tx, timeout: c.timeout}, nil
}

// *** Internal functions and structures ***
type grpcCredentials struct {
	username string
	password string
	token    string
	params   map[string]string
}

func (g grpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	md := make(map[string]string, len(g.params)+1)

	// Authentication parameters
	switch {
	case g.token != "":
		md["authorization"] = "Bearer " + g.token
	case g.username != "":

		md["authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(g.username+":"+g.password))
	}

	for k, v := range g.params {
		md[k] = v
	}

	return md, nil
}

func (g grpcCredentials) RequireTransportSecurity() bool {
	return g.token != "" || g.username != ""
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
	default:
		return fmt.Errorf("unknown builder type %T", builder)
	}
	return nil
}

// *** Config related code ***
type DriverConfig struct {
	Address  string
	Username string
	Password string
	Token    string
	Timeout  time.Duration
	Params   map[string]string

	TLSEnabled bool
	TLSConfig  *tls.Config
}

func NewDriverConfigFromDSN(dsn string) (*DriverConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	// Sanity checks on the given connection string
	if u.Scheme != "flightsql" {
		return nil, fmt.Errorf("invalid scheme %q", u.Scheme)
	}
	if u.Path != "" {
		return nil, fmt.Errorf("unexpected path %q", u.Path)
	}

	// Extract the settings
	var username, password string
	if u.User != nil {
		username = u.User.Username()
		if v, set := u.User.Password(); set {
			password = v
		}
	}

	config := &DriverConfig{
		Address:  u.Host,
		Username: username,
		Password: password,
		Params:   make(map[string]string),
	}

	// Determine the parameters
	for key, values := range u.Query() {
		// We only support single instances
		if len(values) > 1 {
			return nil, fmt.Errorf("too many values for %q", key)
		}
		var v string
		if len(values) > 0 {
			v = values[0]
		}

		switch key {
		case "token":
			config.Token = v
		case "timeout":
			config.Timeout, err = time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
		default:
			config.Params[key] = v
		}
	}

	return config, nil
}

func (config *DriverConfig) DSN() string {
	u := url.URL{
		Scheme: "flightsql",
		Host:   config.Address,
	}
	if config.Username != "" {
		if config.Password == "" {
			u.User = url.User(config.Username)
		} else {
			u.User = url.UserPassword(config.Username, config.Password)
		}
	}

	// Set the parameters
	values := url.Values{}
	if config.Token != "" {
		values.Add("token", config.Token)
	}
	if config.Timeout > 0 {
		values.Add("timeout", config.Timeout.String())
	}
	for k, v := range config.Params {
		values.Add(k, v)
	}

	// Check if we do have parameters at all and set them
	if len(values) > 0 {
		u.RawQuery = values.Encode()
	}

	return u.String()
}

// Register the driver on load.
func init() {
	sql.Register("flightsql", &Driver{})
}
