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
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/flight/flightsql"
	pb "github.com/apache/arrow/go/v13/arrow/flight/internal/flight"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type mockGrpcClientStream struct {
	mock.Mock
}

func (m *mockGrpcClientStream) Header() (metadata.MD, error)  { panic("unimplemented") }
func (m *mockGrpcClientStream) Trailer() metadata.MD          { panic("unimplemented") }
func (m *mockGrpcClientStream) CloseSend() error              { return m.Called().Error(0) }
func (m *mockGrpcClientStream) Context() context.Context      { return context.TODO() }
func (m *mockGrpcClientStream) SendMsg(msg interface{}) error { return m.Called(msg).Error(0) }
func (m *mockGrpcClientStream) RecvMsg(msg interface{}) error { return m.Called(msg).Error(0) }

type FlightServiceClientMock struct {
	mock.Mock
}

func (m *FlightServiceClientMock) Authenticate(_ context.Context, opts ...grpc.CallOption) error {
	return m.Called(opts).Error(0)
}

func (m *FlightServiceClientMock) AuthenticateBasicToken(_ context.Context, user, pass string, opts ...grpc.CallOption) (context.Context, error) {
	args := m.Called(user, pass, opts)
	return args.Get(0).(context.Context), args.Error(1)
}

func (m *FlightServiceClientMock) Close() error {
	return m.Called().Error(0)
}

func (m *FlightServiceClientMock) Handshake(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_HandshakeClient, error) {
	panic("not implemented") // TODO: Implement
}

func (m *FlightServiceClientMock) ListFlights(ctx context.Context, in *flight.Criteria, opts ...grpc.CallOption) (flight.FlightService_ListFlightsClient, error) {
	panic("not implemented") // TODO: Implement
}

func (m *FlightServiceClientMock) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	args := m.Called(in.Type, in.Cmd, opts)
	return args.Get(0).(*flight.FlightInfo), args.Error(1)
}

func (m *FlightServiceClientMock) GetSchema(ctx context.Context, in *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.SchemaResult, error) {
	panic("not implemented") // TODO: Implement
}

func (m *FlightServiceClientMock) DoGet(ctx context.Context, in *flight.Ticket, opts ...grpc.CallOption) (flight.FlightService_DoGetClient, error) {
	panic("not implemented") // TODO: Implement
}

func (m *FlightServiceClientMock) DoPut(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_DoPutClient, error) {
	args := m.Called(opts)
	return args.Get(0).(flight.FlightService_DoPutClient), args.Error(1)
}

func (m *FlightServiceClientMock) DoExchange(ctx context.Context, opts ...grpc.CallOption) (flight.FlightService_DoExchangeClient, error) {
	panic("not implemented") // TODO: Implement
}

func (m *FlightServiceClientMock) DoAction(ctx context.Context, in *flight.Action, opts ...grpc.CallOption) (flight.FlightService_DoActionClient, error) {
	args := m.Called(in.Type, in.Body, opts)
	return args.Get(0).(flight.FlightService_DoActionClient), args.Error(1)
}

func (m *FlightServiceClientMock) ListActions(ctx context.Context, in *flight.Empty, opts ...grpc.CallOption) (flight.FlightService_ListActionsClient, error) {
	panic("not implemented") // TODO: Implement
}

type FlightSqlClientSuite struct {
	suite.Suite

	mockClient FlightServiceClientMock
	callOpts   []grpc.CallOption
	sqlClient  flightsql.Client
}

func getDesc(cmd proto.Message) *flight.FlightDescriptor {
	var anycmd anypb.Any
	anycmd.MarshalFrom(cmd)

	data, _ := proto.Marshal(&anycmd)
	return &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  data,
	}
}

func getAction(cmd proto.Message) *flight.Action {
	var anycmd anypb.Any
	anycmd.MarshalFrom(cmd)

	data, _ := proto.Marshal(&anycmd)
	return &flight.Action{Body: data}
}

func (s *FlightSqlClientSuite) SetupTest() {
	s.mockClient = FlightServiceClientMock{}
	s.sqlClient.Client = &s.mockClient
	s.callOpts = []grpc.CallOption{grpc.EmptyCallOption{}}
}

func (s *FlightSqlClientSuite) TearDownTest() {
	s.mockClient.AssertExpectations(s.T())
}

var emptyFlightInfo flight.FlightInfo

func (s *FlightSqlClientSuite) TestGetCatalogs() {
	var cmd pb.CommandGetCatalogs
	desc := getDesc(&cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetCatalogs(context.Background(), s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestGetDBSchemas() {
	var (
		schemaFilterPattern = "schema_filter_pattern"
		catalog             = "catalog"
	)

	cmd := &pb.CommandGetDbSchemas{
		Catalog:               &catalog,
		DbSchemaFilterPattern: &schemaFilterPattern,
	}
	desc := getDesc(cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetDBSchemas(context.Background(), (*flightsql.GetDBSchemasOpts)(cmd), s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestGetTables() {
	var (
		catalog                = "catalog"
		schemaFilterPattern    = "schema_filter_pattern"
		tableNameFilterPattern = "table_name_filter_pattern"
		includeSchema          = true
		tableTypes             = []string{"type1", "type2"}
	)

	cmd := &pb.CommandGetTables{
		Catalog:                &catalog,
		DbSchemaFilterPattern:  &schemaFilterPattern,
		TableNameFilterPattern: &tableNameFilterPattern,
		IncludeSchema:          includeSchema,
		TableTypes:             tableTypes,
	}
	desc := getDesc(cmd)
	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetTables(context.Background(), (*flightsql.GetTablesOpts)(cmd), s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestGetTableTypes() {
	var cmd pb.CommandGetTableTypes
	desc := getDesc(&cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetTableTypes(context.Background(), s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestGetTypeInfo() {
	var cmd pb.CommandGetXdbcTypeInfo
	desc := getDesc(&cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetXdbcTypeInfo(context.Background(), nil, s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestGetExported() {
	var (
		catalog = "catalog"
		schema  = "schema"
		table   = "table"
	)

	cmd := &pb.CommandGetExportedKeys{
		Catalog:  &catalog,
		DbSchema: &schema,
		Table:    table,
	}
	desc := getDesc(cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetExportedKeys(context.Background(), flightsql.TableRef{&catalog, &schema, table}, s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestGetImported() {
	var (
		schema = "schema"
		table  = "table"
	)

	cmd := &pb.CommandGetImportedKeys{
		DbSchema: &schema,
		Table:    table,
	}
	desc := getDesc(cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetImportedKeys(context.Background(), flightsql.TableRef{nil, &schema, table}, s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestGetPrimary() {
	var (
		catalog = "catalog"
		table   = "table"
	)

	cmd := &pb.CommandGetPrimaryKeys{
		Catalog: &catalog,
		Table:   table,
	}
	desc := getDesc(cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetPrimaryKeys(context.Background(), flightsql.TableRef{&catalog, nil, table}, s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestGetCrossReference() {
	var (
		pkCatalog = "pk_catalog"
		pkSchema  = "pk_schema"
		pkTable   = "pk_table"
		fkCatalog = "fk_catalog"
		fkSchema  = "fk_schema"
		fkTable   = "fk_table"
	)

	cmd := &pb.CommandGetCrossReference{
		PkCatalog:  &pkCatalog,
		PkDbSchema: &pkSchema,
		PkTable:    pkTable,
		FkCatalog:  &fkCatalog,
		FkDbSchema: &fkSchema,
		FkTable:    fkTable,
	}
	desc := getDesc(cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetCrossReference(context.Background(),
		flightsql.TableRef{&pkCatalog, &pkSchema, pkTable},
		flightsql.TableRef{&fkCatalog, &fkSchema, fkTable}, s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestExecute() {
	var query = "query"

	cmd := &pb.CommandStatementQuery{Query: query}
	desc := getDesc(cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.Execute(context.Background(), query, s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

type mockDoActionClient struct {
	mockGrpcClientStream
}

func (m *mockDoActionClient) Recv() (*pb.Result, error) {
	args := m.Called()
	return args.Get(0).(*pb.Result), args.Error(1)
}

type mockDoPutClient struct {
	mockGrpcClientStream
}

func (m *mockDoPutClient) Send(fd *flight.FlightData) error {
	return m.Called(fd).Error(0)
}

func (m *mockDoPutClient) Recv() (*pb.PutResult, error) {
	args := m.Called()
	return args.Get(0).(*pb.PutResult), args.Error(1)
}

func (s *FlightSqlClientSuite) TestPreparedStatementExecute() {
	const query = "query"

	cmd := &pb.ActionCreatePreparedStatementRequest{Query: query}
	action := getAction(cmd)
	action.Type = flightsql.CreatePreparedStatementActionType
	closeAct := getAction(&pb.ActionClosePreparedStatementRequest{PreparedStatementHandle: []byte(query)})
	closeAct.Type = flightsql.ClosePreparedStatementActionType

	result := &pb.ActionCreatePreparedStatementResult{PreparedStatementHandle: []byte(query)}
	var out anypb.Any
	out.MarshalFrom(result)
	data, _ := proto.Marshal(&out)

	createRsp := &mockDoActionClient{}
	defer createRsp.AssertExpectations(s.T())
	createRsp.On("Recv").Return(&pb.Result{Body: data}, nil).Once()
	createRsp.On("Recv").Return(&pb.Result{}, io.EOF)
	createRsp.On("CloseSend").Return(nil)

	closeRsp := &mockDoActionClient{}
	defer closeRsp.AssertExpectations(s.T())
	closeRsp.On("Recv").Return(&pb.Result{}, io.EOF)
	closeRsp.On("CloseSend").Return(nil)

	s.mockClient.On("DoAction", flightsql.CreatePreparedStatementActionType, action.Body, s.callOpts).
		Return(createRsp, nil)
	s.mockClient.On("DoAction", flightsql.ClosePreparedStatementActionType, closeAct.Body, s.callOpts).
		Return(closeRsp, nil)

	infoCmd := &pb.CommandPreparedStatementQuery{PreparedStatementHandle: []byte(query)}
	desc := getDesc(infoCmd)
	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)

	prepared, err := s.sqlClient.Prepare(context.TODO(), query, s.callOpts...)
	s.NoError(err)
	defer prepared.Close(context.TODO(), s.callOpts...)

	info, err := prepared.Execute(context.TODO(), s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestPreparedStatementExecuteParamBinding() {
	const query = "query"

	// create and close actions
	cmd := &pb.ActionCreatePreparedStatementRequest{Query: query}
	action := getAction(cmd)
	action.Type = flightsql.CreatePreparedStatementActionType
	closeAct := getAction(&pb.ActionClosePreparedStatementRequest{PreparedStatementHandle: []byte(query)})
	closeAct.Type = flightsql.ClosePreparedStatementActionType

	// results from createprepared statement
	result := &pb.ActionCreatePreparedStatementResult{
		PreparedStatementHandle: []byte(query),
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	result.ParameterSchema = flight.SerializeSchema(schema, memory.DefaultAllocator)

	// mocked client stream
	var out anypb.Any
	out.MarshalFrom(result)
	data, _ := proto.Marshal(&out)

	createRsp := &mockDoActionClient{}
	defer createRsp.AssertExpectations(s.T())
	createRsp.On("Recv").Return(&pb.Result{Body: data}, nil).Once()
	createRsp.On("Recv").Return(&pb.Result{}, io.EOF)
	createRsp.On("CloseSend").Return(nil)

	closeRsp := &mockDoActionClient{}
	defer closeRsp.AssertExpectations(s.T())
	closeRsp.On("Recv").Return(&pb.Result{}, io.EOF)
	closeRsp.On("CloseSend").Return(nil)

	// expect two actions: one to create and one to close the prepared statement
	s.mockClient.On("DoAction", flightsql.CreatePreparedStatementActionType, action.Body, s.callOpts).Return(createRsp, nil)
	s.mockClient.On("DoAction", flightsql.ClosePreparedStatementActionType, closeAct.Body, s.callOpts).Return(closeRsp, nil)

	expectedDesc := getDesc(&pb.CommandPreparedStatementQuery{PreparedStatementHandle: []byte(query)})

	// mocked client stream for DoPut
	mockedPut := &mockDoPutClient{}
	s.mockClient.On("DoPut", s.callOpts).Return(mockedPut, nil)
	mockedPut.On("Send", mock.MatchedBy(func(fd *flight.FlightData) bool {
		return proto.Equal(expectedDesc, fd.FlightDescriptor)
	})).Return(nil).Twice() // first sends schema message, second sends data
	mockedPut.On("CloseSend").Return(nil)
	mockedPut.On("Recv").Return((*pb.PutResult)(nil), nil)

	infoCmd := &pb.CommandPreparedStatementQuery{PreparedStatementHandle: []byte(query)}
	desc := getDesc(infoCmd)
	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)

	prepared, err := s.sqlClient.Prepare(context.TODO(), query, s.callOpts...)
	s.NoError(err)
	defer prepared.Close(context.TODO(), s.callOpts...)

	paramSchema := prepared.ParameterSchema()
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, paramSchema, strings.NewReader(`[{"id": 1}]`))
	s.NoError(err)
	defer rec.Release()

	prepared.SetParameters(rec)
	info, err := prepared.Execute(context.TODO(), s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestPreparedStatementExecuteReaderBinding() {
	const query = "query"

	// create and close actions
	cmd := &pb.ActionCreatePreparedStatementRequest{Query: query}
	action := getAction(cmd)
	action.Type = flightsql.CreatePreparedStatementActionType
	closeAct := getAction(&pb.ActionClosePreparedStatementRequest{PreparedStatementHandle: []byte(query)})
	closeAct.Type = flightsql.ClosePreparedStatementActionType

	// results from createprepared statement
	result := &pb.ActionCreatePreparedStatementResult{
		PreparedStatementHandle: []byte(query),
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	result.ParameterSchema = flight.SerializeSchema(schema, memory.DefaultAllocator)

	// mocked client stream
	var out anypb.Any
	out.MarshalFrom(result)
	data, _ := proto.Marshal(&out)

	createRsp := &mockDoActionClient{}
	defer createRsp.AssertExpectations(s.T())
	createRsp.On("Recv").Return(&pb.Result{Body: data}, nil).Once()
	createRsp.On("Recv").Return(&pb.Result{}, io.EOF)
	createRsp.On("CloseSend").Return(nil)

	closeRsp := &mockDoActionClient{}
	defer closeRsp.AssertExpectations(s.T())
	closeRsp.On("Recv").Return(&pb.Result{}, io.EOF)
	closeRsp.On("CloseSend").Return(nil)

	// expect two actions: one to create and one to close the prepared statement
	s.mockClient.On("DoAction", flightsql.CreatePreparedStatementActionType, action.Body, s.callOpts).Return(createRsp, nil)
	s.mockClient.On("DoAction", flightsql.ClosePreparedStatementActionType, closeAct.Body, s.callOpts).Return(closeRsp, nil)

	expectedDesc := getDesc(&pb.CommandPreparedStatementQuery{PreparedStatementHandle: []byte(query)})

	// mocked client stream for DoPut
	mockedPut := &mockDoPutClient{}
	s.mockClient.On("DoPut", s.callOpts).Return(mockedPut, nil)
	// 1x schema
	mockedPut.On("Send", mock.MatchedBy(func(fd *flight.FlightData) bool {
		return proto.Equal(expectedDesc, fd.FlightDescriptor)
	})).Return(nil)
	// 3x bind parameters
	mockedPut.On("Send", mock.MatchedBy(func(fd *flight.FlightData) bool {
		return fd.FlightDescriptor == nil
	})).Return(nil).Times(3)
	mockedPut.On("CloseSend").Return(nil)
	mockedPut.On("Recv").Return((*pb.PutResult)(nil), nil)

	infoCmd := &pb.CommandPreparedStatementQuery{PreparedStatementHandle: []byte(query)}
	desc := getDesc(infoCmd)
	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)

	prepared, err := s.sqlClient.Prepare(context.TODO(), query, s.callOpts...)
	s.NoError(err)
	defer prepared.Close(context.TODO(), s.callOpts...)

	paramSchema := prepared.ParameterSchema()
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, paramSchema, strings.NewReader(`[{"id": 1}]`))
	s.NoError(err)
	defer rec.Release()

	rdr, err := array.NewRecordReader(rec.Schema(), []arrow.Record{rec, rec, rec})
	s.NoError(err)
	prepared.SetRecordReader(rdr)

	info, err := prepared.Execute(context.TODO(), s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func (s *FlightSqlClientSuite) TestPreparedStatementClose() {
	// Setup
	const query = "query"

	// create and close actions
	cmd := &pb.ActionCreatePreparedStatementRequest{Query: query}
	action := getAction(cmd)
	action.Type = flightsql.CreatePreparedStatementActionType
	closeAct := getAction(&pb.ActionClosePreparedStatementRequest{PreparedStatementHandle: []byte(query)})
	closeAct.Type = flightsql.ClosePreparedStatementActionType

	// results from createprepared statement
	result := &pb.ActionCreatePreparedStatementResult{
		PreparedStatementHandle: []byte(query),
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	result.ParameterSchema = flight.SerializeSchema(schema, memory.DefaultAllocator)

	// mocked client stream
	var out anypb.Any
	out.MarshalFrom(result)
	data, _ := proto.Marshal(&out)

	createRsp := &mockDoActionClient{}
	defer createRsp.AssertExpectations(s.T())
	createRsp.On("Recv").Return(&pb.Result{Body: data}, nil).Once()
	createRsp.On("Recv").Return(&pb.Result{}, io.EOF)
	createRsp.On("CloseSend").Return(nil)

	closeRsp := &mockDoActionClient{}
	defer closeRsp.AssertExpectations(s.T())
	closeRsp.On("Recv").Return(&pb.Result{}, io.EOF)
	closeRsp.On("CloseSend").Return(nil)

	// expect two actions: one to create and one to close the prepared statement
	s.mockClient.On("DoAction", flightsql.CreatePreparedStatementActionType, action.Body, s.callOpts).Return(createRsp, nil)
	s.mockClient.On("DoAction", flightsql.ClosePreparedStatementActionType, closeAct.Body, s.callOpts).Return(closeRsp, nil)

	// Mocked calls
	prepared, err := s.sqlClient.Prepare(context.TODO(), query, s.callOpts...)
	s.NoError(err)

	err = prepared.Close(context.TODO(), s.callOpts...)
	s.NoError(err)
}

func (s *FlightSqlClientSuite) TestExecuteUpdate() {
	const query = "query"

	cmd := &pb.CommandStatementUpdate{Query: query}
	desc := getDesc(cmd)
	result := &pb.DoPutUpdateResult{RecordCount: 100}
	resdata, _ := proto.Marshal(result)

	mockedPut := &mockDoPutClient{}
	mockedPut.On("Send", mock.MatchedBy(func(fd *flight.FlightData) bool {
		return proto.Equal(desc, fd.FlightDescriptor)
	})).Return(nil)
	mockedPut.On("CloseSend").Return(nil)
	mockedPut.On("Recv").Return(&pb.PutResult{AppMetadata: resdata}, nil)
	s.mockClient.On("DoPut", s.callOpts).Return(mockedPut, nil)

	num, err := s.sqlClient.ExecuteUpdate(context.TODO(), query, s.callOpts...)
	s.NoError(err)
	s.EqualValues(100, num)
}

func (s *FlightSqlClientSuite) TestGetSqlInfo() {
	sqlInfo := []flightsql.SqlInfo{
		flightsql.SqlInfoFlightSqlServerName,
		flightsql.SqlInfoFlightSqlServerVersion,
		flightsql.SqlInfoFlightSqlServerArrowVersion,
	}

	cmd := &pb.CommandGetSqlInfo{Info: make([]uint32, len(sqlInfo))}
	for i, info := range sqlInfo {
		cmd.Info[i] = uint32(info)
	}
	desc := getDesc(cmd)

	s.mockClient.On("GetFlightInfo", desc.Type, desc.Cmd, s.callOpts).Return(&emptyFlightInfo, nil)
	info, err := s.sqlClient.GetSqlInfo(context.TODO(), sqlInfo, s.callOpts...)
	s.NoError(err)
	s.Equal(&emptyFlightInfo, info)
}

func TestFlightSqlClient(t *testing.T) {
	suite.Run(t, new(FlightSqlClientSuite))
}
