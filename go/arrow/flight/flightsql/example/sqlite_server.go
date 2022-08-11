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

// Package example contains a FlightSQL Server implementation using
// sqlite as the backing engine.
//
// In order to ensure portability we'll use modernc.org/sqlite instead
// of github.com/mattn/go-sqlite3 because modernc is a translation of the
// SQLite source into Go, such that it doesn't require CGO to run and
// doesn't need to link against the actual libsqlite3 libraries. This way
// we don't require CGO or libsqlite3 to run this example or the tests.
//
// That said, since both implement in terms of Go's standard database/sql
// package, it's easy to swap them out if desired as the modernc.org/sqlite
// package is slower than go-sqlite3.
package example

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	_ "modernc.org/sqlite"
)

func genRandomString() []byte {
	const length = 16
	max := int('z')
	min := int('0')

	out := make([]byte, length)
	for i := range out {
		out[i] = byte(rand.Intn(max-min+1) + min)
	}
	return out
}

func prepareQueryForGetTables(cmd flightsql.GetTables) string {
	var b strings.Builder
	b.WriteString(`SELECT null AS catalog_name, null AS schema_name, 
		name AS table_name, type AS table_type FROM sqlite_master WHERE 1=1`)

	if cmd.GetCatalog() != nil {
		b.WriteString(" and catalog_name = '")
		b.WriteString(*cmd.GetCatalog())
		b.WriteByte('\'')
	}

	if cmd.GetDBSchemaFilterPattern() != nil {
		b.WriteString(" and schema_name LIKE '")
		b.WriteString(*cmd.GetDBSchemaFilterPattern())
		b.WriteByte('\'')
	}

	if cmd.GetTableNameFilterPattern() != nil {
		b.WriteString(" and table_name LIKE '")
		b.WriteString(*cmd.GetTableNameFilterPattern())
		b.WriteByte('\'')
	}

	if len(cmd.GetTableTypes()) > 0 {
		b.WriteString(" and table_type IN (")
		for i, t := range cmd.GetTableTypes() {
			if i != 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "'%s'", t)
		}
		b.WriteByte(')')
	}

	b.WriteString(" order by table_name")
	return b.String()
}

type SQLiteFlightSQLServer struct {
	flightsql.BaseServer
	db  *sql.DB
	mem memory.Allocator

	prepared sync.Map
}

func NewSQLiteFlightSQLServer() (*SQLiteFlightSQLServer, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
	CREATE TABLE foreignTable (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		foreignName varchar(100),
		value int);	

	CREATE TABLE intTable (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		keyName varchar(100),
		value int,
		foreignId int references foreignTable(id));

	INSERT INTO foreignTable (foreignName, value) VALUES ('keyOne', 1);
	INSERT INTO foreignTable (foreignName, value) VALUES ('keyTwo', 0);
	INSERT INTO foreignTable (foreignName, value) VALUES ('keyThree', -1);
	INSERT INTO intTable (keyName, value, foreignId) VALUES ('one', 1, 1);
	INSERT INTO intTable (keyName, value, foreignId) VALUES ('zero', 0, 1);
	INSERT INTO intTable (keyName, value, foreignId) VALUES ('negative one', -1, 1);
	INSERT INTO intTable (keyName, value, foreignId) VALUES (NULL, NULL, NULL);
	`)

	if err != nil {
		return nil, err
	}
	return &SQLiteFlightSQLServer{db: db}, nil
}

func (s *SQLiteFlightSQLServer) flightInfoForCommand(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.mem),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

func (s *SQLiteFlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()
	tkt, err := flightsql.CreateStatementQueryTicket([]byte(query))
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *SQLiteFlightSQLServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	rows, err := s.db.QueryContext(ctx, string(cmd.GetStatementHandle()))
	if err != nil {
		return nil, nil, err
	}

	reader, err := NewSqlBatchReader(s.mem, rows)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(reader, ch)
	return reader.schema, ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.Catalogs), nil
}

func (s *SQLiteFlightSQLServer) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// sqlite doesn't support catalogs, this returns an empty record batch
	schema := schema_ref.Catalogs
	batchBldr := array.NewRecordBuilder(s.mem, schema)
	defer batchBldr.Release()

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batchBldr.NewRecord()}
	close(ch)

	return schema, ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoSchemas(_ context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (s *SQLiteFlightSQLServer) DoGetDBSchemas(context.Context, flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// sqlite doesn't support schemas, this returns an empty record batch
	schema := schema_ref.DBSchemas
	batchBldr := array.NewRecordBuilder(s.mem, schema)
	defer batchBldr.Release()

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batchBldr.NewRecord()}
	close(ch)

	return schema, ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return s.flightInfoForCommand(desc, schema), nil
}

func (s *SQLiteFlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := prepareQueryForGetTables(cmd)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	var rdr array.RecordReader

	rdr, err = NewSqlBatchReaderWithSchema(s.mem, schema_ref.Tables, rows)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk, 2)
	if cmd.GetIncludeSchema() {
		rdr, err = NewSqliteTablesSchemaBatchReader(ctx, s.mem, rdr, s.db, query)
		if err != nil {
			return nil, nil, err
		}
	}

	go flight.StreamChunksFromReader(rdr, ch)
	return rdr.Schema(), ch, nil
}

func (s *SQLiteFlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	res, err := s.db.ExecContext(ctx, cmd.GetQuery())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLiteFlightSQLServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	stmt, err := s.db.PrepareContext(ctx, req.GetQuery())
	if err != nil {
		return result, err
	}

	handle := genRandomString()
	s.prepared.Store(handle, stmt)

	result.Handle = handle
	// no way to get the dataset or parameter schemas from sql.DB
	return
}

func (s *SQLiteFlightSQLServer) ClosePreparedStatement(ctx context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	handle := request.GetPreparedStatementHandle()
	if val, loaded := s.prepared.LoadAndDelete(handle); loaded {
		stmt := val.(sql.Stmt)
		return stmt.Close()
	}

	return status.Error(codes.InvalidArgument, "prepared statement not found")
}

func (s *SQLiteFlightSQLServer) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	_, ok := s.prepared.Load(cmd.GetPreparedStatementHandle())
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *SQLiteFlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	val, ok := s.prepared.Load(cmd.GetPreparedStatementHandle())
	if !ok {
		return nil, nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(sql.Stmt)
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	rdr, err := NewSqlBatchReader(s.mem, rows)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(rdr, ch)
	return rdr.Schema(), ch, nil
}

// func (s *SQLiteFlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
// 	val, ok := s.prepared.Load(cmd.GetPreparedStatementHandle())
// 	if !ok {
// 		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
// 	}

// 	stmt := val.(sql.Stmt)

// }
