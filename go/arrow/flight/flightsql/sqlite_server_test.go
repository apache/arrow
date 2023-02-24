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

//go:build go1.18
// +build go1.18

package flightsql_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql/example"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	sqlite3 "modernc.org/sqlite/lib"
)

type FlightSqliteServerSuite struct {
	suite.Suite

	db  *sql.DB
	srv *example.SQLiteFlightSQLServer
	s   flight.Server
	cl  *flightsql.Client

	mem *memory.CheckedAllocator
}

func (s *FlightSqliteServerSuite) getColMetadata(colType int, table string) arrow.Metadata {
	bldr := flightsql.NewColumnMetadataBuilder()
	bldr.Scale(15).IsReadOnly(false).IsAutoIncrement(false)
	if table != "" {
		bldr.TableName(table)
	}
	switch colType {
	case sqlite3.SQLITE_TEXT, sqlite3.SQLITE_BLOB:
	case sqlite3.SQLITE_INTEGER:
		bldr.Precision(10)
	case sqlite3.SQLITE_FLOAT:
		bldr.Precision(15)
	default:
		bldr.Precision(0)
	}
	return bldr.Metadata()
}

func (s *FlightSqliteServerSuite) SetupTest() {
	var err error
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.s = flight.NewServerWithMiddleware(nil)
	s.db, err = example.CreateDB()
	s.Require().NoError(err)
	s.srv, err = example.NewSQLiteFlightSQLServer(s.db)
	s.Require().NoError(err)
	s.srv.Alloc = s.mem

	s.s.RegisterFlightService(flightsql.NewFlightServer(s.srv))
	s.s.Init("localhost:0")
	s.s.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go s.s.Serve()
	s.cl, err = flightsql.NewClient(s.s.Addr().String(), nil, nil, dialOpts...)
	s.Require().NoError(err)
	s.Require().NotNil(s.cl)
	s.cl.Alloc = s.mem
}

func (s *FlightSqliteServerSuite) TearDownTest() {
	s.Require().NoError(s.cl.Close())
	s.s.Shutdown()
	s.srv = nil
	err := s.db.Close()
	s.Require().NoError(err)
	s.mem.AssertSize(s.T(), 0)
}

func (s *FlightSqliteServerSuite) fromJSON(dt arrow.DataType, json string) arrow.Array {
	arr, _, _ := array.FromJSON(s.mem, dt, strings.NewReader(json))
	return arr
}

func (s *FlightSqliteServerSuite) execCountQuery(query string) int64 {
	info, err := s.cl.Execute(context.Background(), query)
	s.NoError(err)

	rdr, err := s.cl.DoGet(context.Background(), info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	rec, err := rdr.Read()
	s.NoError(err)
	return rec.Column(0).(*array.Int64).Value(0)
}

func (s *FlightSqliteServerSuite) TestCommandStatementQuery() {
	ctx := context.Background()
	info, err := s.cl.Execute(ctx, "SELECT * FROM intTable")
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.NotNil(rec)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true},
		{Name: "keyName", Type: arrow.BinaryTypes.String, Metadata: s.getColMetadata(sqlite3.SQLITE_TEXT, ""), Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true},
		{Name: "foreignId", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true},
	}, nil)

	s.Truef(expectedSchema.Equal(rec.Schema()), "expected: %s\ngot: %s", expectedSchema, rec.Schema())

	idarr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 2, 3, 4]`)
	defer idarr.Release()
	keyarr := s.fromJSON(arrow.BinaryTypes.String, `["one", "zero", "negative one", null]`)
	defer keyarr.Release()
	valarr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 0, -1, null]`)
	defer valarr.Release()
	foreignarr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 1, 1, null]`)
	defer foreignarr.Release()

	expectedRec := array.NewRecord(expectedSchema, []arrow.Array{idarr, keyarr, valarr, foreignarr}, 4)
	defer expectedRec.Release()

	s.Truef(array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)
}

func (s *FlightSqliteServerSuite) TestCommandGetTables() {
	ctx := context.Background()
	info, err := s.cl.GetTables(ctx, &flightsql.GetTablesOpts{})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	catalogName := s.fromJSON(arrow.BinaryTypes.String, `["main", "main", "main"]`)
	defer catalogName.Release()
	schemaName := s.fromJSON(arrow.BinaryTypes.String, `["", "", ""]`)
	defer schemaName.Release()

	tableName := s.fromJSON(arrow.BinaryTypes.String, `["foreignTable", "intTable", "sqlite_sequence"]`)
	defer tableName.Release()

	tableType := s.fromJSON(arrow.BinaryTypes.String, `["table", "table", "table"]`)
	defer tableType.Release()

	expectedRec := array.NewRecord(schema_ref.Tables, []arrow.Array{catalogName, schemaName, tableName, tableType}, 3)
	defer expectedRec.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())

	s.Truef(array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)
}

func (s *FlightSqliteServerSuite) TestCommandGetTablesWithIncludedSchemasNoFilter() {
	ctx := context.Background()
	info, err := s.cl.GetTables(ctx, &flightsql.GetTablesOpts{
		IncludeSchema: true,
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	// Don't check the actual data since it'll include SQLite internal tables
	s.True(rdr.Next())
	s.False(rdr.Next())
	s.NoError(rdr.Err())
}

func (s *FlightSqliteServerSuite) TestCommandGetTablesWithTableFilter() {
	ctx := context.Background()
	info, err := s.cl.GetTables(ctx, &flightsql.GetTablesOpts{
		TableNameFilterPattern: proto.String("int%"),
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	catalog := s.fromJSON(arrow.BinaryTypes.String, `["main"]`)
	schema := s.fromJSON(arrow.BinaryTypes.String, `[""]`)
	table := s.fromJSON(arrow.BinaryTypes.String, `["intTable"]`)
	tabletype := s.fromJSON(arrow.BinaryTypes.String, `["table"]`)
	expected := array.NewRecord(schema_ref.Tables, []arrow.Array{catalog, schema, table, tabletype}, 1)
	defer func() {
		catalog.Release()
		schema.Release()
		table.Release()
		tabletype.Release()
		expected.Release()
	}()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())
	s.NoError(rdr.Err())

	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
}

func (s *FlightSqliteServerSuite) TestCommandGetTablesWithTableTypesFilter() {
	ctx := context.Background()
	info, err := s.cl.GetTables(ctx, &flightsql.GetTablesOpts{
		TableTypes: []string{"index"},
	})
	s.NoError(err)

	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(schema_ref.Tables.Equal(rdr.Schema()), rdr.Schema().String())
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandGetTablesWithExistingTableTypeFilter() {
	ctx := context.Background()
	info, err := s.cl.GetTables(ctx, &flightsql.GetTablesOpts{
		TableTypes: []string{"table"},
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	catalogName := s.fromJSON(arrow.BinaryTypes.String, `["main", "main", "main"]`)
	defer catalogName.Release()
	schemaName := s.fromJSON(arrow.BinaryTypes.String, `["", "", ""]`)
	defer schemaName.Release()

	tableName := s.fromJSON(arrow.BinaryTypes.String, `["foreignTable", "intTable", "sqlite_sequence"]`)
	defer tableName.Release()

	tableType := s.fromJSON(arrow.BinaryTypes.String, `["table", "table", "table"]`)
	defer tableType.Release()

	expectedRec := array.NewRecord(schema_ref.Tables, []arrow.Array{catalogName, schemaName, tableName, tableType}, 3)
	defer expectedRec.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())

	s.Truef(array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)
}

func (s *FlightSqliteServerSuite) TestCommandGetTablesWithIncludedSchemas() {
	ctx := context.Background()
	info, err := s.cl.GetTables(ctx, &flightsql.GetTablesOpts{
		TableNameFilterPattern: proto.String("int%"),
		IncludeSchema:          true,
	})
	s.NoError(err)
	s.NotNil(info)

	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	catalog := s.fromJSON(arrow.BinaryTypes.String, `["main"]`)
	schema := s.fromJSON(arrow.BinaryTypes.String, `[""]`)
	table := s.fromJSON(arrow.BinaryTypes.String, `["intTable"]`)
	tabletype := s.fromJSON(arrow.BinaryTypes.String, `["table"]`)

	dbTableName := "intTable"

	tableSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64,
			Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, dbTableName)},
		{Name: "keyName", Type: arrow.BinaryTypes.String,
			Metadata: s.getColMetadata(sqlite3.SQLITE_TEXT, dbTableName)},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64,
			Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, dbTableName)},
		{Name: "foreignId", Type: arrow.PrimitiveTypes.Int64,
			Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, dbTableName)},
	}, nil)
	schemaBuf := flight.SerializeSchema(tableSchema, s.mem)
	binaryBldr := array.NewBinaryBuilder(s.mem, arrow.BinaryTypes.Binary)
	binaryBldr.Append(schemaBuf)
	schemaCol := binaryBldr.NewArray()

	expected := array.NewRecord(schema_ref.TablesWithIncludedSchema, []arrow.Array{catalog, schema, table, tabletype, schemaCol}, 1)
	defer func() {
		catalog.Release()
		schema.Release()
		table.Release()
		tabletype.Release()
		binaryBldr.Release()
		schemaCol.Release()
		expected.Release()
	}()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())
	s.NoError(rdr.Err())

	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
}

func (s *FlightSqliteServerSuite) TestCommandGetTypeInfo() {
	ctx := context.Background()
	info, err := s.cl.GetXdbcTypeInfo(ctx, nil)
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	expected := example.GetTypeInfoResult(s.mem)
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandGetTypeInfoFiltered() {
	ctx := context.Background()
	info, err := s.cl.GetXdbcTypeInfo(ctx, proto.Int32(-4))
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	expected := example.GetFilteredTypeInfoResult(s.mem, -4)
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandGetCatalogs() {
	ctx := context.Background()
	info, err := s.cl.GetCatalogs(ctx)
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Schema().Equal(schema_ref.Catalogs), rdr.Schema().String())

	catalog := s.fromJSON(arrow.BinaryTypes.String, `["main"]`)
	expected := array.NewRecord(schema_ref.Catalogs, []arrow.Array{catalog}, 1)
	defer catalog.Release()
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)

	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandGetDbSchemas() {
	ctx := context.Background()
	info, err := s.cl.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Schema().Equal(schema_ref.DBSchemas), rdr.Schema().String())

	catalog := s.fromJSON(arrow.BinaryTypes.String, `["main"]`)
	schema := s.fromJSON(arrow.BinaryTypes.String, `[""]`)
	expected := array.NewRecord(schema_ref.DBSchemas, []arrow.Array{catalog, schema}, 1)
	defer catalog.Release()
	defer schema.Release()
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.NotNil(rec)
	rec.Retain()
	defer rec.Release()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)

	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandGetTableTypes() {
	ctx := context.Background()
	info, err := s.cl.GetTableTypes(ctx)
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	expected := s.fromJSON(arrow.BinaryTypes.String, `["table"]`)
	defer expected.Release()
	expectedRec := array.NewRecord(schema_ref.TableTypes, []arrow.Array{expected}, 1)
	defer expectedRec.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandStatementUpdate() {
	ctx := context.Background()
	result, err := s.cl.ExecuteUpdate(ctx, `INSERT INTO intTable (keyName, value) VALUES
							('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)`)
	s.NoError(err)
	s.EqualValues(3, result)

	result, err = s.cl.ExecuteUpdate(ctx, `UPDATE intTable SET keyName = 'KEYNAME1'
										  WHERE keyName = 'KEYNAME2' OR keyName = 'KEYNAME3'`)
	s.NoError(err)
	s.EqualValues(2, result)

	result, err = s.cl.ExecuteUpdate(ctx, `DELETE FROM intTable WHERE keyName = 'KEYNAME1'`)
	s.NoError(err)
	s.EqualValues(3, result)
}

func (s *FlightSqliteServerSuite) TestCommandPreparedStatementQuery() {
	ctx := context.Background()
	prep, err := s.cl.Prepare(ctx, "SELECT * FROM intTable")
	s.NoError(err)
	defer prep.Close(ctx)

	info, err := prep.Execute(ctx)
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true},
		{Name: "keyName", Type: arrow.BinaryTypes.String, Metadata: s.getColMetadata(sqlite3.SQLITE_TEXT, ""), Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true},
		{Name: "foreignId", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true}}, nil)

	idArr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 2, 3, 4]`)
	defer idArr.Release()
	keyNameArr := s.fromJSON(arrow.BinaryTypes.String, `["one", "zero", "negative one", null]`)
	defer keyNameArr.Release()
	valueArr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 0, -1, null]`)
	defer valueArr.Release()
	foreignIdArr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 1, 1, null]`)
	defer foreignIdArr.Release()

	expected := array.NewRecord(expectedSchema, []arrow.Array{idArr, keyNameArr, valueArr, foreignIdArr}, 4)
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandPreparedStatementQueryWithParams() {
	ctx := context.Background()
	stmt, err := s.cl.Prepare(ctx, "SELECT * FROM intTable WHERE keyName LIKE ?")
	s.NoError(err)
	defer stmt.Close(ctx)

	typeIDs := s.fromJSON(arrow.PrimitiveTypes.Int8, "[0]")
	offsets := s.fromJSON(arrow.PrimitiveTypes.Int32, "[0]")
	strArray := s.fromJSON(arrow.BinaryTypes.String, `["%one"]`)
	bytesArr := s.fromJSON(arrow.BinaryTypes.Binary, "[]")
	bigintArr := s.fromJSON(arrow.PrimitiveTypes.Int64, "[]")
	dblArr := s.fromJSON(arrow.PrimitiveTypes.Float64, "[]")
	paramArr, _ := array.NewDenseUnionFromArraysWithFields(typeIDs,
		offsets, []arrow.Array{strArray, bytesArr, bigintArr, dblArr},
		[]string{"string", "bytes", "bigint", "double"})
	batch := array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: "parameter_1", Type: paramArr.DataType()}}, nil),
		[]arrow.Array{paramArr}, 1)
	defer func() {
		typeIDs.Release()
		offsets.Release()
		strArray.Release()
		bytesArr.Release()
		bigintArr.Release()
		dblArr.Release()
		paramArr.Release()
		batch.Release()
	}()

	stmt.SetParameters(batch)
	info, err := stmt.Execute(ctx)
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true},
		{Name: "keyName", Type: arrow.BinaryTypes.String, Metadata: s.getColMetadata(sqlite3.SQLITE_TEXT, ""), Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true},
		{Name: "foreignId", Type: arrow.PrimitiveTypes.Int64, Metadata: s.getColMetadata(sqlite3.SQLITE_INTEGER, ""), Nullable: true}}, nil)

	idArr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 3]`)
	defer idArr.Release()
	keyNameArr := s.fromJSON(arrow.BinaryTypes.String, `["one", "negative one"]`)
	defer keyNameArr.Release()
	valueArr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, -1]`)
	defer valueArr.Release()
	foreignIdArr := s.fromJSON(arrow.PrimitiveTypes.Int64, `[1, 1]`)
	defer foreignIdArr.Release()

	expected := array.NewRecord(expectedSchema, []arrow.Array{idArr, keyNameArr, valueArr, foreignIdArr}, 2)
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandPreparedStatementUpdateWithParams() {
	ctx := context.Background()
	stmt, err := s.cl.Prepare(ctx, "INSERT INTO intTable (keyName, value) VALUES ('new_value', ?)")
	s.NoError(err)
	defer stmt.Close(ctx)

	typeIDs := s.fromJSON(arrow.PrimitiveTypes.Int8, "[2]")
	offsets := s.fromJSON(arrow.PrimitiveTypes.Int32, "[0]")
	strArray := s.fromJSON(arrow.BinaryTypes.String, "[]")
	bytesArr := s.fromJSON(arrow.BinaryTypes.Binary, "[]")
	bigintArr := s.fromJSON(arrow.PrimitiveTypes.Int64, "[999]")
	dblArr := s.fromJSON(arrow.PrimitiveTypes.Float64, "[]")
	paramArr, err := array.NewDenseUnionFromArraysWithFields(typeIDs,
		offsets, []arrow.Array{strArray, bytesArr, bigintArr, dblArr},
		[]string{"string", "bytes", "bigint", "double"})
	s.NoError(err)
	batch := array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: "parameter_1", Type: paramArr.DataType()}}, nil),
		[]arrow.Array{paramArr}, 1)
	defer func() {
		typeIDs.Release()
		offsets.Release()
		strArray.Release()
		bytesArr.Release()
		bigintArr.Release()
		dblArr.Release()
		paramArr.Release()
		batch.Release()
	}()

	stmt.SetParameters(batch)
	s.EqualValues(4, s.execCountQuery("SELECT COUNT(*) FROM intTable"))
	n, err := stmt.ExecuteUpdate(context.Background())
	s.NoError(err)
	s.EqualValues(1, n)
	s.EqualValues(5, s.execCountQuery("SELECT COUNT(*) FROM intTable"))
	n, err = s.cl.ExecuteUpdate(context.Background(), "DELETE FROM intTable WHERE keyName = 'new_value'")
	s.NoError(err)
	s.EqualValues(1, n)
	s.EqualValues(4, s.execCountQuery("SELECT COUNT(*) FROM intTable"))
}

func (s *FlightSqliteServerSuite) TestCommandPreparedStatementUpdate() {
	ctx := context.Background()
	stmt, err := s.cl.Prepare(ctx, "INSERT INTO intTable (keyName, value) VALUES ('new_value', 999)")
	s.NoError(err)
	defer stmt.Close(ctx)

	s.EqualValues(4, s.execCountQuery("SELECT COUNT(*) FROM intTable"))
	result, err := stmt.ExecuteUpdate(ctx)
	s.NoError(err)
	s.EqualValues(1, result)
	s.EqualValues(5, s.execCountQuery("SELECT COUNT(*) FROM intTable"))
	result, err = s.cl.ExecuteUpdate(ctx, "DELETE FROM intTable WHERE keyName = 'new_value'")
	s.NoError(err)
	s.EqualValues(1, result)
	s.EqualValues(4, s.execCountQuery("SELECT COUNT(*) FROM intTable"))
}

func (s *FlightSqliteServerSuite) TestCommandGetPrimaryKeys() {
	ctx := context.Background()
	info, err := s.cl.GetPrimaryKeys(ctx, flightsql.TableRef{Table: "int%"})
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	bldr := array.NewRecordBuilder(s.mem, schema_ref.PrimaryKeys)
	defer bldr.Release()
	bldr.Field(0).AppendNull()
	bldr.Field(1).AppendNull()
	bldr.Field(2).(*array.StringBuilder).Append("intTable")
	bldr.Field(3).(*array.StringBuilder).Append("id")
	bldr.Field(4).(*array.Int32Builder).Append(1)
	bldr.Field(5).AppendNull()
	expected := bldr.NewRecord()
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandGetImportedKeys() {
	ctx := context.Background()
	info, err := s.cl.GetImportedKeys(ctx, flightsql.TableRef{Table: "intTable"})
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	bldr := array.NewRecordBuilder(s.mem, schema_ref.ImportedKeys)
	defer bldr.Release()
	bldr.Field(0).AppendNull()
	bldr.Field(1).AppendNull()
	bldr.Field(2).(*array.StringBuilder).Append("foreignTable")
	bldr.Field(3).(*array.StringBuilder).Append("id")
	bldr.Field(4).AppendNull()
	bldr.Field(5).AppendNull()
	bldr.Field(6).(*array.StringBuilder).Append("intTable")
	bldr.Field(7).(*array.StringBuilder).Append("foreignId")
	bldr.Field(8).(*array.Int32Builder).Append(0)
	bldr.Field(9).AppendNull()
	bldr.Field(10).AppendNull()
	bldr.Field(11).(*array.Uint8Builder).Append(3)
	bldr.Field(12).(*array.Uint8Builder).Append(3)
	expected := bldr.NewRecord()
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandGetExportedKeys() {
	ctx := context.Background()
	info, err := s.cl.GetExportedKeys(ctx, flightsql.TableRef{Table: "foreignTable"})
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	bldr := array.NewRecordBuilder(s.mem, schema_ref.ImportedKeys)
	defer bldr.Release()
	bldr.Field(0).AppendNull()
	bldr.Field(1).AppendNull()
	bldr.Field(2).(*array.StringBuilder).Append("foreignTable")
	bldr.Field(3).(*array.StringBuilder).Append("id")
	bldr.Field(4).AppendNull()
	bldr.Field(5).AppendNull()
	bldr.Field(6).(*array.StringBuilder).Append("intTable")
	bldr.Field(7).(*array.StringBuilder).Append("foreignId")
	bldr.Field(8).(*array.Int32Builder).Append(0)
	bldr.Field(9).AppendNull()
	bldr.Field(10).AppendNull()
	bldr.Field(11).(*array.Uint8Builder).Append(3)
	bldr.Field(12).(*array.Uint8Builder).Append(3)
	expected := bldr.NewRecord()
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func (s *FlightSqliteServerSuite) TestCommandGetCrossRef() {
	ctx := context.Background()
	info, err := s.cl.GetCrossReference(ctx,
		flightsql.TableRef{Table: "foreignTable"},
		flightsql.TableRef{Table: "intTable"})
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	bldr := array.NewRecordBuilder(s.mem, schema_ref.ImportedKeys)
	defer bldr.Release()
	bldr.Field(0).AppendNull()
	bldr.Field(1).AppendNull()
	bldr.Field(2).(*array.StringBuilder).Append("foreignTable")
	bldr.Field(3).(*array.StringBuilder).Append("id")
	bldr.Field(4).AppendNull()
	bldr.Field(5).AppendNull()
	bldr.Field(6).(*array.StringBuilder).Append("intTable")
	bldr.Field(7).(*array.StringBuilder).Append("foreignId")
	bldr.Field(8).(*array.Int32Builder).Append(0)
	bldr.Field(9).AppendNull()
	bldr.Field(10).AppendNull()
	bldr.Field(11).(*array.Uint8Builder).Append(3)
	bldr.Field(12).(*array.Uint8Builder).Append(3)
	expected := bldr.NewRecord()
	defer expected.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	s.Truef(array.RecordEqual(expected, rec), "expected: %s\ngot: %s", expected, rec)
	s.False(rdr.Next())
}

func validateSqlInfo(t *testing.T, expected interface{}, sc scalar.Scalar) bool {
	switch ex := expected.(type) {
	case string:
		return assert.Equal(t, ex, sc.String())
	case bool:
		return assert.Equal(t, ex, sc.(*scalar.Boolean).Value)
	case int64:
		return assert.Equal(t, ex, sc.(*scalar.Int64).Value)
	case int32:
		return assert.Equal(t, ex, sc.(*scalar.Int32).Value)
	case []string:
		arr := sc.(*scalar.List).Value.(*array.String)
		assert.EqualValues(t, len(ex), arr.Len())
		for i, v := range ex {
			assert.Equal(t, v, arr.Value(i))
		}
	case map[int32][]int32:
		// map is a list of structs with key and values
		structArr := sc.(*scalar.Map).Value.(*array.Struct)
		keys := structArr.Field(0).(*array.Int32)
		values := structArr.Field(1).(*array.List)
		// assert that the map has the right size
		assert.EqualValues(t, len(ex), keys.Len())

		// for each element, match the argument
		for i := 0; i < keys.Len(); i++ {
			keyScalar, _ := scalar.GetScalar(keys, i)
			infoID := keyScalar.(*scalar.Int32).Value

			// assert the key exists
			list, ok := ex[infoID]
			assert.True(t, ok)

			// assert the int32list is the right size
			start, end := values.ValueOffsets(i)
			assert.EqualValues(t, len(list), end-start)

			// for each element make sure it matches
			for j, v := range list {
				listItem, err := scalar.GetScalar(values.ListValues(), int(start)+j)
				assert.NoError(t, err)
				assert.Equal(t, v, listItem.(*scalar.Int32).Value)
			}
		}
	}
	return true
}

func (s *FlightSqliteServerSuite) TestCommandGetSqlInfo() {
	expectedResults := example.SqlInfoResultMap()
	infoIDs := make([]flightsql.SqlInfo, 0, len(expectedResults))
	for k := range expectedResults {
		infoIDs = append(infoIDs, flightsql.SqlInfo(k))
	}

	ctx := context.Background()
	info, err := s.cl.GetSqlInfo(ctx, infoIDs)
	s.NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.NoError(err)
	defer rdr.Release()

	s.True(rdr.Next())
	rec := rdr.Record()
	rec.Retain()
	defer rec.Release()
	s.False(rdr.Next())

	s.EqualValues(2, rec.NumCols())
	s.EqualValues(len(expectedResults), rec.NumRows())

	colName := rec.Column(0).(*array.Uint32)
	colValue := rec.Column(1)
	for i := 0; i < int(rec.NumRows()); i++ {
		expected := expectedResults[colName.Value(i)]
		sc, err := scalar.GetScalar(colValue, i)
		s.NoError(err)

		s.True(validateSqlInfo(s.T(), expected, sc.(*scalar.DenseUnion).ChildValue()))

		sc.(*scalar.DenseUnion).Release()
	}
}

func (s *FlightSqliteServerSuite) TestTransactions() {
	ctx := context.Background()
	tx, err := s.cl.BeginTransaction(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(tx)

	s.True(tx.ID().IsValid())
	s.NotEmpty(tx.ID())

	_, err = tx.BeginSavepoint(ctx, "foobar")
	s.Equal(codes.Unimplemented, status.Code(err))

	info, err := tx.Execute(ctx, "SELECT * FROM intTable")
	s.Require().NoError(err)
	rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)

	toTable := func(r *flight.Reader) arrow.Table {
		defer r.Release()
		recs := make([]arrow.Record, 0)
		for rdr.Next() {
			r := rdr.Record()
			r.Retain()
			defer r.Release()
			recs = append(recs, r)
		}

		return array.NewTableFromRecords(rdr.Schema(), recs)
	}
	tbl := toTable(rdr)
	defer tbl.Release()

	rowCount := tbl.NumRows()

	result, err := tx.ExecuteUpdate(ctx, `INSERT INTO intTable (keyName, value) VALUES
						   ('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)`)
	s.Require().NoError(err)
	s.EqualValues(3, result)

	info, err = tx.Execute(ctx, "SELECT * FROM intTable")
	s.Require().NoError(err)
	rdr, err = s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	tbl = toTable(rdr)
	defer tbl.Release()
	s.EqualValues(rowCount+3, tbl.NumRows())

	s.Require().NoError(tx.Rollback(ctx))
	// commit/rollback invalidates the transaction handle
	s.ErrorIs(tx.Commit(ctx), flightsql.ErrInvalidTxn)
	s.ErrorIs(tx.Rollback(ctx), flightsql.ErrInvalidTxn)

	info, err = s.cl.Execute(ctx, "SELECT * FROM intTable")
	s.Require().NoError(err)
	rdr, err = s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
	s.Require().NoError(err)
	tbl = toTable(rdr)
	defer tbl.Release()
	s.EqualValues(rowCount, tbl.NumRows())
}

func TestSqliteServer(t *testing.T) {
	suite.Run(t, new(FlightSqliteServerSuite))
}
