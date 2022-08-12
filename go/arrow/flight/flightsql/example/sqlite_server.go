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
	"github.com/apache/arrow/go/v10/arrow/scalar"
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

func prepareQueryForGetKeys(filter string) string {
	return `SELECT * FROM (
		SELECT 
			NULL AS pk_catalog_name,
			NULL AS pk_schema_name,
			p."table" AS pk_table_name,
			p."to" AS pk_column_name,
			NULL AS fk_catalog_name,
			NULL AS fk_schema_name,
			m.name AS fk_table_name,
			p."from" AS fk_column_name,
			p.seq AS key_sequence,
			NULL AS pk_key_name,
			NULL AS fk_key_name,
			CASE
				WHEN p.on_update = 'CASCADE' THEN 0
				WHEN p.on_update = 'RESTRICT' THEN 1
				WHEN p.on_update = 'SET NULL' THEN 2
				WHEN p.on_update = 'NO ACTION' THEN 3
				WHEN p.on_update = 'SET DEFAULT' THEN 4
			END AS update_rule,
			CASE
				WHEN p.on_delete = 'CASCADE' THEN 0
				WHEN p.on_delete = 'RESTRICT' THEN 1
				WHEN p.on_delete = 'SET NULL' THEN 2
				WHEN p.on_delete = 'NO ACTION' THEN 3
				WHEN p.on_delete = 'SET DEFAULT' THEN 4
			END AS delete_rule
		FROM sqlite_master m
		JOIN pragma_foreign_key_list(m.name) p ON m.name != p."table"
		WHERE m.type = 'table') WHERE ` + filter +
		` ORDER BY pk_catalog_name, pk_schema_name, pk_table_name, pk_key_name, key_sequence`
}

type Statement struct {
	stmt   *sql.Stmt
	params []interface{}
}

type SQLiteFlightSQLServer struct {
	flightsql.BaseServer
	db *sql.DB

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
	ret := &SQLiteFlightSQLServer{db: db}
	for k, v := range SqlInfoResultMap() {
		ret.RegisterSqlInfo(flightsql.SqlInfo(k), v)
	}
	return ret, nil
}

func (s *SQLiteFlightSQLServer) flightInfoForCommand(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.Alloc),
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
	return doGetQuery(ctx, s.Alloc, s.db, string(cmd.GetStatementHandle()), nil)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.Catalogs), nil
}

func (s *SQLiteFlightSQLServer) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// sqlite doesn't support catalogs, this returns an empty record batch
	schema := schema_ref.Catalogs

	ch := make(chan flight.StreamChunk)
	close(ch)

	return schema, ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoSchemas(_ context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (s *SQLiteFlightSQLServer) DoGetDBSchemas(context.Context, flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// sqlite doesn't support schemas, this returns an empty record batch
	schema := schema_ref.DBSchemas

	ch := make(chan flight.StreamChunk)
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

	rdr, err = NewSqlBatchReaderWithSchema(s.Alloc, schema_ref.Tables, rows)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk, 2)
	if cmd.GetIncludeSchema() {
		rdr, err = NewSqliteTablesSchemaBatchReader(ctx, s.Alloc, rdr, s.db, query)
		if err != nil {
			return nil, nil, err
		}
	}

	go flight.StreamChunksFromReader(rdr, ch)
	return rdr.Schema(), ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoXdbcTypeInfo(_ context.Context, _ flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.XdbcTypeInfo), nil
}

func (s *SQLiteFlightSQLServer) DoGetXdbcTypeInfo(_ context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var batch arrow.Record
	if cmd.GetDataType() == nil {
		batch = GetTypeInfoResult(s.Alloc)
	} else {
		batch = GetFilteredTypeInfoResult(s.Alloc, *cmd.GetDataType())
	}

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)
	return batch.Schema(), ch, nil
}

func (s *SQLiteFlightSQLServer) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.TableTypes), nil
}

func (s *SQLiteFlightSQLServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := "SELECT DISTINCT type AS table_type FROM sqlite_master"
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.TableTypes)
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
	s.prepared.Store(string(handle), Statement{stmt: stmt})

	result.Handle = handle
	// no way to get the dataset or parameter schemas from sql.DB
	return
}

func (s *SQLiteFlightSQLServer) ClosePreparedStatement(ctx context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	handle := request.GetPreparedStatementHandle()
	if val, loaded := s.prepared.LoadAndDelete(string(handle)); loaded {
		stmt := val.(Statement)
		return stmt.stmt.Close()
	}

	return status.Error(codes.InvalidArgument, "prepared statement not found")
}

func (s *SQLiteFlightSQLServer) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	_, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
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

func doGetQuery(ctx context.Context, mem memory.Allocator, db *sql.DB, query string, schema *arrow.Schema, args ...interface{}) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}

	var rdr *SqlBatchReader
	if schema != nil {
		rdr, err = NewSqlBatchReaderWithSchema(mem, schema, rows)
	} else {
		rdr, err = NewSqlBatchReader(mem, rows)
	}

	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(rdr, ch)
	return rdr.schema, ch, nil
}

func (s *SQLiteFlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return nil, nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	rows, err := stmt.stmt.QueryContext(ctx, stmt.params...)
	if err != nil {
		return nil, nil, err
	}

	rdr, err := NewSqlBatchReader(s.Alloc, rows)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(rdr, ch)
	return rdr.Schema(), ch, nil
}

func getParamsForStatement(rdr flight.MessageReader) (params []interface{}, err error) {
	for rdr.Next() {
		rec := rdr.Record()

		nrows := int(rec.NumRows())
		ncols := int(rec.NumCols())

		if len(params) < int(ncols) {
			params = make([]interface{}, ncols)
		}

		for i := 0; i < nrows; i++ {
			for c := 0; c < ncols; c++ {
				col := rec.Column(c)
				sc, err := scalar.GetScalar(col, i)
				if err != nil {
					return nil, err
				}
				if r, ok := sc.(scalar.Releasable); ok {
					r.Release()
				}

				switch v := sc.(*scalar.DenseUnion).Value.(type) {
				case *scalar.Int64:
					params[c] = v.Value
				case *scalar.Float32:
					params[c] = v.Value
				case *scalar.Float64:
					params[c] = v.Value
				case *scalar.String:
					params[c] = string(v.Value.Bytes())
				case *scalar.Binary:
					params[c] = v.Value.Bytes()
				default:
					return nil, fmt.Errorf("unsupported type: %s", v)
				}
			}
		}
	}

	return params, rdr.Err()
}

func (s *SQLiteFlightSQLServer) DoPutPreparedStatementQuery(_ context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, _ flight.MetadataWriter) error {
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	args, err := getParamsForStatement(rdr)
	if err != nil {
		return status.Errorf(codes.Internal, "error gathering parameters for prepared statement query: %s", err.Error())
	}

	stmt.params = args
	s.prepared.Store(string(cmd.GetPreparedStatementHandle()), stmt)
	return nil
}

func (s *SQLiteFlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	args, err := getParamsForStatement(rdr)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "error gathering parameters for prepared statement: %s", err.Error())
	}

	stmt.params = args
	result, err := stmt.stmt.ExecContext(ctx, args...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func (s *SQLiteFlightSQLServer) GetFlightInfoPrimaryKeys(_ context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.PrimaryKeys), nil
}

func (s *SQLiteFlightSQLServer) DoGetPrimaryKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// the field key_name can not be recovered by sqlite so it is
	// being set to null following the same pattern for catalog name and schema_name
	var b strings.Builder

	b.WriteString(`
	SELECT null AS catalog_name, null AS schema_name, table_name, name AS column_name, pk AS key_sequence, null as key_name
	FROM pragma_table_info(table_name)
		JOIN (SELECT null AS catalog_name, null AS schema_name, name AS table_name, type AS table_type
			FROM sqlite_master) where 1=1 AND pk !=0`)

	if cmd.Catalog != nil {
		fmt.Fprintf(&b, " and catalog_name LIKE '%s'", *cmd.Catalog)
	}
	if cmd.DBSchema != nil {
		fmt.Fprintf(&b, " and schema_name LIKE '%s'", *cmd.DBSchema)
	}

	fmt.Fprintf(&b, " and table_name LIKE '%s'", cmd.Table)

	return doGetQuery(ctx, s.Alloc, s.db, b.String(), schema_ref.PrimaryKeys)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoImportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ImportedKeys), nil
}

func (s *SQLiteFlightSQLServer) DoGetImportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	filter := "fk_table_name = '" + ref.Table + "'"
	if ref.Catalog != nil {
		filter += " AND fk_catalog_name = '" + *ref.Catalog + "'"
	}
	if ref.DBSchema != nil {
		filter += " AND fk_schema_name = '" + *ref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ImportedKeys)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoExportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.ExportedKeys), nil
}

func (s *SQLiteFlightSQLServer) DoGetExportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	filter := "pk_table_name = '" + ref.Table + "'"
	if ref.Catalog != nil {
		filter += " AND pk_catalog_name = '" + *ref.Catalog + "'"
	}
	if ref.DBSchema != nil {
		filter += " AND pk_schema_name = '" + *ref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ExportedKeys)
}

func (s *SQLiteFlightSQLServer) GetFlightInfoCrossReference(_ context.Context, _ flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfoForCommand(desc, schema_ref.CrossReference), nil
}

func (s *SQLiteFlightSQLServer) DoGetCrossReference(ctx context.Context, cmd flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	pkref := cmd.PKRef
	filter := "pk_table_name = '" + pkref.Table + "'"
	if pkref.Catalog != nil {
		filter += " AND pk_catalog_name = '" + *pkref.Catalog + "'"
	}
	if pkref.DBSchema != nil {
		filter += " AND pk_schema_name = '" + *pkref.DBSchema + "'"
	}

	fkref := cmd.FKRef
	filter += " AND fk_table_name = '" + fkref.Table + "'"
	if fkref.Catalog != nil {
		filter += " AND fk_catalog_name = '" + *fkref.Catalog + "'"
	}
	if fkref.DBSchema != nil {
		filter += " AND fk_schema_name = '" + *fkref.DBSchema + "'"
	}
	query := prepareQueryForGetKeys(filter)
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ExportedKeys)
}
