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

package flightsql_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql/example"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql/schema_ref"
	pb "github.com/apache/arrow/go/v10/arrow/flight/internal/flight"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	sqlite3 "modernc.org/sqlite/lib"
)

var dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

type UnimplementedFlightSqlServerSuite struct {
	suite.Suite

	s  flight.Server
	cl *flightsql.Client
}

func (s *UnimplementedFlightSqlServerSuite) SetupSuite() {
	s.s = flight.NewServerWithMiddleware(nil)
	srv := flightsql.NewFlightServer(&flightsql.BaseServer{})
	s.s.RegisterFlightService(srv)
	s.s.Init("localhost:0")

	go s.s.Serve()
}

func (s *UnimplementedFlightSqlServerSuite) SetupTest() {
	cl, err := flightsql.NewClient(s.s.Addr().String(), nil, nil, dialOpts...)
	s.Require().NoError(err)
	s.cl = cl
}

func (s *UnimplementedFlightSqlServerSuite) TearDownTest() {
	s.Require().NoError(s.cl.Close())
	s.cl = nil
}

func (s *UnimplementedFlightSqlServerSuite) TearDownSuite() {
	s.s.Shutdown()
}

// the following test functions verify that the default base server will
// correctly route requests to the appropriate interface methods based on
// the descriptor types for DoPut/DoGet/DoAction

func (s *UnimplementedFlightSqlServerSuite) TestExecute() {
	info, err := s.cl.Execute(context.TODO(), "SELECT * FROM IRRELEVANT")
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoStatement not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetTables() {
	info, err := s.cl.GetTables(context.TODO(), &flightsql.GetTablesOpts{})
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoTables not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetTableTypes() {
	info, err := s.cl.GetTableTypes(context.TODO())
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoTableTypes not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetPrimaryKeys() {
	info, err := s.cl.GetPrimaryKeys(context.TODO(), flightsql.TableRef{})
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoPrimaryKeys not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetExportedKeys() {
	info, err := s.cl.GetExportedKeys(context.TODO(), flightsql.TableRef{})
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoExportedKeys not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetImportedKeys() {
	info, err := s.cl.GetImportedKeys(context.TODO(), flightsql.TableRef{})
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoImportedKeys not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetCrossReference() {
	info, err := s.cl.GetCrossReference(context.TODO(), flightsql.TableRef{}, flightsql.TableRef{})
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoCrossReference not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetCatalogs() {
	info, err := s.cl.GetCatalogs(context.TODO())
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoCatalogs not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetDBSchemas() {
	info, err := s.cl.GetDBSchemas(context.TODO(), &flightsql.GetDBSchemasOpts{})
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoSchemas not implemented")
	s.Nil(info)
}

func (s *UnimplementedFlightSqlServerSuite) TestGetTypeInfo() {
	info, err := s.cl.GetXdbcTypeInfo(context.TODO(), nil)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "GetFlightInfoXdbcTypeInfo not implemented")
	s.Nil(info)
}

func getTicket(cmd proto.Message) *flight.Ticket {
	var anycmd anypb.Any
	anycmd.MarshalFrom(cmd)

	data, _ := proto.Marshal(&anycmd)
	return &flight.Ticket{
		Ticket: data,
	}
}

func (s *UnimplementedFlightSqlServerSuite) TestDoGet() {
	tests := []struct {
		name   string
		ticket proto.Message
	}{
		{"DoGetStatement", &pb.TicketStatementQuery{}},
		{"DoGetPreparedStatement", &pb.CommandPreparedStatementQuery{}},
		{"DoGetCatalogs", &pb.CommandGetCatalogs{}},
		{"DoGetDBSchemas", &pb.CommandGetDbSchemas{}},
		{"DoGetTables", &pb.CommandGetTables{}},
		{"DoGetTableTypes", &pb.CommandGetTableTypes{}},
		{"DoGetXdbcTypeInfo", &pb.CommandGetXdbcTypeInfo{}},
		{"DoGetPrimaryKeys", &pb.CommandGetPrimaryKeys{}},
		{"DoGetExportedKeys", &pb.CommandGetExportedKeys{}},
		{"DoGetImportedKeys", &pb.CommandGetImportedKeys{}},
		{"DoGetCrossReference", &pb.CommandGetCrossReference{}},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			rdr, err := s.cl.DoGet(context.TODO(), getTicket(tt.ticket))
			s.Nil(rdr)
			s.True(strings.HasSuffix(err.Error(), tt.name+" not implemented"), err.Error())
		})
	}
}

func (s *UnimplementedFlightSqlServerSuite) TestDoAction() {
	prep, err := s.cl.Prepare(context.TODO(), memory.DefaultAllocator, "IRRELEVANT")
	s.Nil(prep)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "CreatePreparedStatement not implemented")
}

func TestBaseServer(t *testing.T) {
	suite.Run(t, new(UnimplementedFlightSqlServerSuite))
}

type FlightSqliteServerSuite struct {
	suite.Suite

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
	s.srv, err = example.NewSQLiteFlightSQLServer()
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

	catalogName := scalar.MakeArrayOfNull(arrow.BinaryTypes.String, 3, s.mem)
	defer catalogName.Release()
	schemaName := scalar.MakeArrayOfNull(arrow.BinaryTypes.String, 3, s.mem)
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

	catalog := s.fromJSON(arrow.BinaryTypes.String, `[null]`)
	schema := s.fromJSON(arrow.BinaryTypes.String, `[null]`)
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

	catalogName := scalar.MakeArrayOfNull(arrow.BinaryTypes.String, 3, s.mem)
	defer catalogName.Release()
	schemaName := scalar.MakeArrayOfNull(arrow.BinaryTypes.String, 3, s.mem)
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

	catalog := s.fromJSON(arrow.BinaryTypes.String, `[null]`)
	schema := s.fromJSON(arrow.BinaryTypes.String, `[null]`)
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
	prep, err := s.cl.Prepare(ctx, s.mem, "SELECT * FROM intTable")
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
	stmt, err := s.cl.Prepare(ctx, s.mem, "SELECT * FROM intTable WHERE keyName LIKE ?")
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
	stmt, err := s.cl.Prepare(ctx, s.mem, "INSERT INTO intTable (keyName, value) VALUES ('new_value', ?)")
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
	stmt, err := s.cl.Prepare(ctx, s.mem, "INSERT INTO intTable (keyName, value) VALUES ('new_value', 999)")
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

func TestSqliteServer(t *testing.T) {
	suite.Run(t, new(FlightSqliteServerSuite))
}
