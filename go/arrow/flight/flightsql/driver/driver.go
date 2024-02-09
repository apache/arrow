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
package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v15/arrow/memory"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Rows struct {
	schema           *arrow.Schema
	recordChan       chan arrow.Record
	currentRecord    arrow.Record
	currentRecordMux sync.Mutex
	currentRow       uint64
	initializedChan  chan bool
	streamError      error
}

func newRows() *Rows {
	return &Rows{
		recordChan:      make(chan arrow.Record, 1),
		initializedChan: make(chan bool),
	}
}

// Columns returns the names of the columns.
func (r *Rows) Columns() []string {
	if r.schema == nil {
		return nil
	}

	// All records have the same columns
	cols := make([]string, len(r.schema.Fields()))
	for i, c := range r.schema.Fields() {
		cols[i] = c.Name
	}

	return cols
}

func (r *Rows) releaseRecord() {
	r.currentRecordMux.Lock()
	defer r.currentRecordMux.Unlock()

	if r.currentRecord != nil {
		r.currentRecord.Release()
		r.currentRecord = nil
	}
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	go func() {
		if r.recordChan != nil {
			if _, ok := <-r.recordChan; ok {
				close(r.recordChan)
			}
		}
	}()

	go func() {
		if r.initializedChan != nil {
			if _, ok := <-r.initializedChan; ok {
				close(r.initializedChan)
			}
		}
	}()

	r.releaseRecord()

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
	if r.currentRecord == nil || int64(r.currentRow) >= r.currentRecord.NumRows() {
		if r.streamError != nil {
			return r.streamError
		}

		r.releaseRecord()

		// Get the next record from the channel
		var ok bool

		if r.currentRecord, ok = <-r.recordChan; !ok || r.currentRecord == nil || r.currentRecord.NumRows() < 1 {
			return io.EOF // Channel closed, no more records
		}

		r.currentRow = 0
	}

	for i, col := range r.currentRecord.Columns() {
		v, err := fromArrowType(col, int(r.currentRow))
		if err != nil {
			return err
		}

		dest[i] = v
	}

	r.currentRow++

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
	stmt   *flightsql.PreparedStatement
	client *flightsql.Client

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
	return schema.NumFields()
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

	rows := newRows()
	go rows.streamRecordset(ctx, s.client, info.Endpoint)

	<-rows.initializedChan

	return rows, nil
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
				Name: arg.Name,
				Type: dt,
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
	tx      *flightsql.Txn
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

	client, err := flightsql.NewClientCtx(ctx, c.addr, nil, nil, c.options...)
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
	client *flightsql.Client
	txn    *flightsql.Txn

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
	var stmt *flightsql.PreparedStatement
	if c.txn != nil && c.txn.ID().IsValid() {
		stmt, err = c.txn.Prepare(ctx, query)
	} else {
		stmt, err = c.client.Prepare(ctx, query)
		c.txn = nil
	}
	if err != nil {
		return nil, err
	}

	s := &Stmt{
		stmt:    stmt,
		client:  c.client,
		timeout: c.timeout,
	}

	return s, nil
}

func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		// We cannot pass arguments to the client so we skip a direct query.
		// This will force the sql-framework to prepare and execute queries.
		return nil, driver.ErrSkip
	}

	if _, set := ctx.Deadline(); !set && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	info, err := c.client.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	rows := newRows()
	go rows.streamRecordset(ctx, c.client, info.Endpoint)

	<-rows.initializedChan

	return rows, nil
}

func (r *Rows) streamRecordset(ctx context.Context, c *flightsql.Client, endpoints []*flight.FlightEndpoint) {
	defer close(r.recordChan)

	defer func() { // in case of error, init anyway
		select {
		case <-r.initializedChan:
			r.initializedChan <- true
		default:
			r.initializedChan <- true
		}
	}()

	// reads each endpoint
	for _, endpoint := range endpoints {
		func() { // with a func() is possible to {defer reader.Release()}
			reader, err := c.DoGet(ctx, endpoint.GetTicket())
			if err != nil {
				r.streamError = fmt.Errorf("getting ticket failed: %w", err)
				return
			}

			defer reader.Release()

			r.schema = reader.Schema()

			r.initializedChan <- true

			// reads each record into a blocking channel
			for reader.Next() {
				record := reader.Record()
				record.Retain()

				select {
				case r.recordChan <- record:

				case <-ctx.Done():
					r.releaseRecord()
					r.streamError = fmt.Errorf("stream recordset context timed out")
					return
				}
			}

			if err := reader.Err(); err != nil && !errors.Is(err, io.EOF) {
				r.streamError = err
				return
			}
		}()
	}
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *Connection) Close() error {
	if c.txn != nil && c.txn.ID().IsValid() {
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

// Register the driver on load.
func init() {
	sql.Register("flightsql", &Driver{})
}
