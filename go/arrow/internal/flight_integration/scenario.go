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

package flight_integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/flight"
	"github.com/apache/arrow/go/v11/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v11/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v11/arrow/internal/arrjson"
	"github.com/apache/arrow/go/v11/arrow/internal/testing/types"
	"github.com/apache/arrow/go/v11/arrow/ipc"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Scenario interface {
	MakeServer(port int) flight.Server
	RunClient(addr string, opts ...grpc.DialOption) error
}

func GetScenario(name string, args ...string) Scenario {
	switch name {
	case "auth:basic_proto":
		return &authBasicProtoTester{}
	case "middleware":
		return &middlewareScenarioTester{}
	case "flight_sql":
		return &flightSqlScenarioTester{}
	case "":
		if len(args) > 0 {
			return &defaultIntegrationTester{path: args[0]}
		}
		return &defaultIntegrationTester{}
	}
	panic(fmt.Errorf("scenario not found: %s", name))
}

func initServer(port int, srv flight.Server) int {
	srv.Init(fmt.Sprintf("0.0.0.0:%d", port))
	_, p, _ := net.SplitHostPort(srv.Addr().String())
	port, _ = strconv.Atoi(p)
	return port
}

type integrationDataSet struct {
	schema *arrow.Schema
	chunks []arrow.Record
}

func consumeFlightLocation(ctx context.Context, loc *flight.Location, tkt *flight.Ticket, orig []arrow.Record, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(loc.GetUri(), nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	stream, err := client.DoGet(ctx, tkt)
	if err != nil {
		return err
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	defer rdr.Release()

	for i, chunk := range orig {
		if !rdr.Next() {
			return fmt.Errorf("got fewer batches than expected, received so far: %d, expected: %d", i, len(orig))
		}

		if !array.RecordEqual(chunk, rdr.Record()) {
			return fmt.Errorf("batch %d doesn't match", i)
		}

		if string(rdr.LatestAppMetadata()) != strconv.Itoa(i) {
			return fmt.Errorf("expected metadata value: %s, but got: %s", strconv.Itoa(i), string(rdr.LatestAppMetadata()))
		}
	}

	if rdr.Next() {
		return fmt.Errorf("got more batches than the expected: %d", len(orig))
	}

	return nil
}

type defaultIntegrationTester struct {
	flight.BaseFlightServer

	port           int
	path           string
	uploadedChunks map[string]integrationDataSet
}

func (s *defaultIntegrationTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()

	arrow.RegisterExtensionType(types.NewUUIDType())
	defer arrow.UnregisterExtensionType("uuid")

	descr := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{s.path},
	}

	fmt.Println("Opening JSON file '", s.path, "'")
	r, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("could not open JSON file: %q: %w", s.path, err)
	}

	rdr, err := arrjson.NewReader(r)
	if err != nil {
		return fmt.Errorf("could not create JSON file reader from file: %q: %w", s.path, err)
	}

	dataSet := integrationDataSet{
		chunks: make([]arrow.Record, 0),
		schema: rdr.Schema(),
	}

	for {
		rec, err := rdr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		defer rec.Release()
		dataSet.chunks = append(dataSet.chunks, rec)
	}

	stream, err := client.DoPut(ctx)
	if err != nil {
		return err
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(dataSet.schema))
	wr.SetFlightDescriptor(descr)

	for i, rec := range dataSet.chunks {
		metadata := []byte(strconv.Itoa(i))
		if err := wr.WriteWithAppMetadata(rec, metadata); err != nil {
			return err
		}

		pr, err := stream.Recv()
		if err != nil {
			return err
		}

		acked := pr.GetAppMetadata()
		switch {
		case len(acked) == 0:
			return fmt.Errorf("expected metadata value: %s, but got nothing", string(metadata))
		case !bytes.Equal(metadata, acked):
			return fmt.Errorf("expected metadata value: %s, but got: %s", string(metadata), string(acked))
		}
	}

	wr.Close()

	if err := stream.CloseSend(); err != nil {
		return err
	}

	for {
		_, err = stream.Recv()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}

	info, err := client.GetFlightInfo(ctx, descr)
	if err != nil {
		return err
	}

	if len(info.Endpoint) == 0 {
		fmt.Fprintln(os.Stderr, "no endpoints returned from flight server.")
		return fmt.Errorf("no endpoints returned from flight server")
	}

	for _, ep := range info.Endpoint {
		if len(ep.Location) == 0 {
			return fmt.Errorf("no locations returned from flight server")
		}

		for _, loc := range ep.Location {
			consumeFlightLocation(ctx, loc, ep.Ticket, dataSet.chunks, opts...)
		}
	}

	return nil
}

func (s *defaultIntegrationTester) MakeServer(port int) flight.Server {
	s.uploadedChunks = make(map[string]integrationDataSet)
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(s)
	s.port = initServer(port, srv)
	return srv
}

func (s *defaultIntegrationTester) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if in.Type == flight.DescriptorPATH {
		if len(in.Path) == 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid path")
		}

		data, ok := s.uploadedChunks[in.Path[0]]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "could not find flight: %s", in.Path[0])
		}

		flightData := &flight.FlightInfo{
			Schema:           flight.SerializeSchema(data.schema, memory.DefaultAllocator),
			FlightDescriptor: in,
			Endpoint: []*flight.FlightEndpoint{{
				Ticket:   &flight.Ticket{Ticket: []byte(in.Path[0])},
				Location: []*flight.Location{{Uri: fmt.Sprintf("grpc+tcp://127.0.0.1:%d", s.port)}},
			}},
			TotalRecords: 0,
			TotalBytes:   -1,
		}
		for _, r := range data.chunks {
			flightData.TotalRecords += r.NumRows()
		}
		return flightData, nil
	}
	return nil, status.Error(codes.Unimplemented, in.Type.String())
}

func (s *defaultIntegrationTester) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	data, ok := s.uploadedChunks[string(tkt.Ticket)]
	if !ok {
		return status.Errorf(codes.NotFound, "could not find flight: %s", string(tkt.Ticket))
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(data.schema))
	defer wr.Close()
	for i, rec := range data.chunks {
		wr.WriteWithAppMetadata(rec, []byte(strconv.Itoa(i)))
	}

	return nil
}

func (s *defaultIntegrationTester) DoPut(stream flight.FlightService_DoPutServer) error {
	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	var (
		key     string
		dataset integrationDataSet
	)

	// creating the reader should have gotten the first message which would
	// have the schema, which should have a populated flight descriptor
	desc := rdr.LatestFlightDescriptor()
	if desc.Type != flight.DescriptorPATH || len(desc.Path) < 1 {
		return status.Error(codes.InvalidArgument, "must specify a path")
	}

	key = desc.Path[0]
	dataset.schema = rdr.Schema()
	dataset.chunks = make([]arrow.Record, 0)
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()

		dataset.chunks = append(dataset.chunks, rec)
		if len(rdr.LatestAppMetadata()) > 0 {
			stream.Send(&flight.PutResult{AppMetadata: rdr.LatestAppMetadata()})
		}
	}
	s.uploadedChunks[key] = dataset
	return nil
}

func CheckActionResults(ctx context.Context, client flight.Client, action *flight.Action, results []string) error {
	stream, err := client.DoAction(ctx, action)
	if err != nil {
		return err
	}
	defer stream.CloseSend()

	for _, expected := range results {
		res, err := stream.Recv()
		if err != nil {
			return err
		}

		actual := string(res.Body)
		if expected != actual {
			return fmt.Errorf("got wrong result: expected: %s, got: %s", expected, actual)
		}
	}

	res, err := stream.Recv()
	if res != nil || err != io.EOF {
		return xerrors.New("action result stream had too many entries")
	}
	return nil
}

const (
	authUsername = "arrow"
	authPassword = "flight"
)

type authBasicValidator struct {
	auth flight.BasicAuth
}

func (a *authBasicValidator) Authenticate(conn flight.AuthConn) error {
	token, err := conn.Read()
	if err != nil {
		return err
	}

	var incoming flight.BasicAuth
	if err = proto.Unmarshal(token, &incoming); err != nil {
		return err
	}

	if incoming.Username != a.auth.Username || incoming.Password != a.auth.Password {
		return status.Error(codes.Unauthenticated, "invalid token")
	}

	return conn.Send([]byte(a.auth.Username))
}

func (a *authBasicValidator) IsValid(token string) (interface{}, error) {
	if token != a.auth.Username {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	return token, nil
}

type clientAuthBasic struct {
	auth  *flight.BasicAuth
	token string
}

func (c *clientAuthBasic) Authenticate(_ context.Context, conn flight.AuthConn) error {
	if c.auth != nil {
		data, err := proto.Marshal(c.auth)
		if err != nil {
			return err
		}
		if err = conn.Send(data); err != nil {
			return err
		}

		token, err := conn.Read()
		c.token = string(token)
		if err != io.EOF {
			return err
		}
	}
	return nil
}

func (c *clientAuthBasic) GetToken(context.Context) (string, error) {
	return c.token, nil
}

type authBasicProtoTester struct {
	flight.BaseFlightServer
}

func (s *authBasicProtoTester) RunClient(addr string, opts ...grpc.DialOption) error {
	auth := &clientAuthBasic{}

	client, err := flight.NewClientWithMiddleware(addr, auth, nil, opts...)
	if err != nil {
		return err
	}

	ctx := context.Background()
	stream, err := client.DoAction(ctx, &flight.Action{})
	if err != nil {
		return err
	}

	// should fail unauthenticated
	_, err = stream.Recv()
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	if st.Code() != codes.Unauthenticated {
		return fmt.Errorf("expected Unauthenticated, got %s", st.Code())
	}

	auth.auth = &flight.BasicAuth{Username: authUsername, Password: authPassword}
	if err := client.Authenticate(ctx); err != nil {
		return err
	}
	return CheckActionResults(ctx, client, &flight.Action{}, []string{authUsername})
}

func (s *authBasicProtoTester) MakeServer(port int) flight.Server {
	s.SetAuthHandler(&authBasicValidator{
		auth: flight.BasicAuth{Username: authUsername, Password: authPassword}})
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(s)
	initServer(port, srv)
	return srv
}

func (authBasicProtoTester) DoAction(_ *flight.Action, stream flight.FlightService_DoActionServer) error {
	auth := flight.AuthFromContext(stream.Context())
	stream.Send(&flight.Result{Body: []byte(auth.(string))})
	return nil
}

type middlewareScenarioTester struct {
	flight.BaseFlightServer
}

func (m *middlewareScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	tm := &testClientMiddleware{}
	client, err := flight.NewClientWithMiddleware(addr, nil, []flight.ClientMiddleware{
		flight.CreateClientMiddleware(tm)}, opts...)
	if err != nil {
		return err
	}

	ctx := context.Background()
	// this call is expected to fail
	_, err = client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD})
	if err == nil {
		return xerrors.New("expected call to fail")
	}

	if tm.received != "expected value" {
		return fmt.Errorf("expected to receive header 'x-middleware: expected value', but instead got %s", tm.received)
	}

	fmt.Fprintln(os.Stderr, "Headers received successfully on failing call.")
	tm.received = ""
	_, err = client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("success")})
	if err != nil {
		return err
	}

	if tm.received != "expected value" {
		return fmt.Errorf("expected to receive header 'x-middleware: expected value', but instead got %s", tm.received)
	}
	fmt.Fprintln(os.Stderr, "Headers received successfully on passing call.")
	return nil
}

func (m *middlewareScenarioTester) MakeServer(port int) flight.Server {
	srv := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(testServerMiddleware{})})
	srv.RegisterFlightService(m)
	initServer(port, srv)
	return srv
}

func (m *middlewareScenarioTester) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if desc.Type != flight.DescriptorCMD || string(desc.Cmd) != "success" {
		return nil, status.Error(codes.Unknown, "unknown")
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(arrow.NewSchema([]arrow.Field{}, nil), memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket:   &flight.Ticket{Ticket: []byte("foo")},
			Location: []*flight.Location{{Uri: "grpc+tcp://localhost:10010"}},
		}},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

var (
	// Schema to be returned for mocking the statement/prepared statement
	// results. Must be the same across all languages
	QuerySchema = arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
		Metadata: flightsql.NewColumnMetadataBuilder().
			TableName("test").IsAutoIncrement(true).IsCaseSensitive(false).
			TypeName("type_test").SchemaName("schema_test").IsSearchable(true).
			CatalogName("catalog_test").Precision(100).Metadata(),
	}}, nil)
)

const (
	updateStatementExpectedRows         int64 = 10000
	updatePreparedStatementExpectedRows int64 = 20000
)

type flightSqlScenarioTester struct {
	flightsql.BaseServer
}

func (m *flightSqlScenarioTester) flightInfoForCommand(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: desc.Cmd}},
		},
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

func (m *flightSqlScenarioTester) MakeServer(port int) flight.Server {
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(flightsql.NewFlightServer(m))
	initServer(port, srv)
	return srv
}

func assertEq(expected, actual interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(actual))
	if !reflect.DeepEqual(expected, v.Interface()) {
		return fmt.Errorf("expected: '%s', got: '%s'", expected, actual)
	}
	return nil
}

func (m *flightSqlScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flightsql.NewClient(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := m.ValidateMetadataRetrieval(client); err != nil {
		return err
	}

	if err := m.ValidateStatementExecution(client); err != nil {
		return err
	}

	return m.ValidatePreparedStatementExecution(client)
}

func (m *flightSqlScenarioTester) validate(expected *arrow.Schema, result *flight.FlightInfo, client *flightsql.Client) error {
	rdr, err := client.DoGet(context.Background(), result.Endpoint[0].Ticket)
	if err != nil {
		return err
	}

	if !expected.Equal(rdr.Schema()) {
		return fmt.Errorf("expected: %s, got: %s", expected, rdr.Schema())
	}
	for {
		_, err := rdr.Read()
		if err == io.EOF { break }
		if err != nil { return err }
	}
	return nil
}

func (m *flightSqlScenarioTester) validateSchema(expected *arrow.Schema, result *flight.SchemaResult) error {
	schema, err := flight.DeserializeSchema(result.GetSchema(), memory.DefaultAllocator)
	if err != nil {
		return err
	}
	if !expected.Equal(schema) {
		return fmt.Errorf("expected: %s, got: %s", expected, schema)
	}
	return nil
}

func (m *flightSqlScenarioTester) ValidateMetadataRetrieval(client *flightsql.Client) error {
	var (
		catalog               = "catalog"
		dbSchemaFilterPattern = "db_schema_filter_pattern"
		tableFilterPattern    = "table_filter_pattern"
		table                 = "table"
		dbSchema              = "db_schema"
		tableTypes            = []string{"table", "view"}

		ref   = flightsql.TableRef{Catalog: &catalog, DBSchema: &dbSchema, Table: table}
		pkRef = flightsql.TableRef{Catalog: proto.String("pk_catalog"), DBSchema: proto.String("pk_db_schema"), Table: "pk_table"}
		fkRef = flightsql.TableRef{Catalog: proto.String("fk_catalog"), DBSchema: proto.String("fk_db_schema"), Table: "fk_table"}

		ctx = context.Background()
	)

	info, err := client.GetCatalogs(ctx)
	if err != nil {
		return err
	}
	if err := m.validate(schema_ref.Catalogs, info, client); err != nil {
		return err
	}

	schema, err := client.GetCatalogsSchema(ctx)
	if err != nil {
		return err
	}
	if err := m.validateSchema(schema_ref.Catalogs, schema); err != nil {
		return err
	}

	info, err = client.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{Catalog: &catalog, DbSchemaFilterPattern: &dbSchemaFilterPattern})
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.DBSchemas, info, client); err != nil {
		return err
	}

	schema, err = client.GetDBSchemasSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.DBSchemas, schema); err != nil {
		return err
	}

	info, err = client.GetTables(ctx, &flightsql.GetTablesOpts{Catalog: &catalog, DbSchemaFilterPattern: &dbSchemaFilterPattern, TableNameFilterPattern: &tableFilterPattern, IncludeSchema: true, TableTypes: tableTypes})
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.TablesWithIncludedSchema, info, client); err != nil {
		return err
	}

	schema, err = client.GetTablesSchema(ctx, &flightsql.GetTablesOpts{IncludeSchema: true})
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.TablesWithIncludedSchema, schema); err != nil {
		return err
	}

	schema, err = client.GetTablesSchema(ctx, &flightsql.GetTablesOpts{IncludeSchema: false})
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.Tables, schema); err != nil {
		return err
	}

	info, err = client.GetTableTypes(ctx)
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.TableTypes, info, client); err != nil {
		return err
	}

	schema, err = client.GetTableTypesSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.TableTypes, schema); err != nil {
		return err
	}

	info, err = client.GetPrimaryKeys(ctx, ref)
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.PrimaryKeys, info, client); err != nil {
		return err
	}

	schema, err = client.GetPrimaryKeysSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.PrimaryKeys, schema); err != nil {
		return err
	}

	info, err = client.GetExportedKeys(ctx, ref)
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.ExportedKeys, info, client); err != nil {
		return err
	}

	schema, err = client.GetExportedKeysSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.ExportedKeys, schema); err != nil {
		return err
	}

	info, err = client.GetImportedKeys(ctx, ref)
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.ImportedKeys, info, client); err != nil {
		return err
	}

	schema, err = client.GetImportedKeysSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.ImportedKeys, schema); err != nil {
		return err
	}

	info, err = client.GetCrossReference(ctx, pkRef, fkRef)
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.CrossReference, info, client); err != nil {
		return err
	}

	schema, err = client.GetCrossReferenceSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.CrossReference, schema); err != nil {
		return err
	}

	info, err = client.GetXdbcTypeInfo(ctx, nil)
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.XdbcTypeInfo, info, client); err != nil {
		return err
	}

	schema, err = client.GetXdbcTypeInfoSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.XdbcTypeInfo, schema); err != nil {
		return err
	}

	info, err = client.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerName, flightsql.SqlInfoFlightSqlServerReadOnly})
	if err != nil {
		return err
	}
	if err = m.validate(schema_ref.SqlInfo, info, client); err != nil {
		return err
	}

	schema, err = client.GetSqlInfoSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(schema_ref.SqlInfo, schema); err != nil {
		return err
	}

	return nil
}

func (m *flightSqlScenarioTester) ValidateStatementExecution(client *flightsql.Client) error {
	ctx := context.Background()
	info, err := client.Execute(ctx, "SELECT STATEMENT")
	if err != nil {
		return err
	}
	if err = m.validate(QuerySchema, info, client); err != nil {
		return err
	}

	schema, err := client.GetExecuteSchema(ctx, "SELECT STATEMENT")
	if err != nil {
		return err
	}
	if err = m.validateSchema(QuerySchema, schema); err != nil {
		return err
	}

	updateResult, err := client.ExecuteUpdate(ctx, "UPDATE STATEMENT")
	if err != nil {
		return err
	}
	if updateResult != updateStatementExpectedRows {
		return fmt.Errorf("expected 'UPDATE STATEMENT' return %d got %d", updateStatementExpectedRows, updateResult)
	}
	return nil
}

func (m *flightSqlScenarioTester) ValidatePreparedStatementExecution(client *flightsql.Client) error {
	ctx := context.Background()
	prepared, err := client.Prepare(ctx, memory.DefaultAllocator, "SELECT PREPARED STATEMENT")
	if err != nil {
		return err
	}

	arr, _, _ := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64, strings.NewReader("[1]"))
	defer arr.Release()
	params := array.NewRecord(QuerySchema, []arrow.Array{arr}, 1)
	prepared.SetParameters(params)

	info, err := prepared.Execute(ctx)
	if err != nil {
		return err
	}
	if err = m.validate(QuerySchema, info, client); err != nil {
		return err
	}
	schema, err := prepared.GetSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(QuerySchema, schema); err != nil {
		return err
	}

	if err = prepared.Close(ctx); err != nil {
		return err
	}

	updatePrepared, err := client.Prepare(ctx, memory.DefaultAllocator, "UPDATE PREPARED STATEMENT")
	if err != nil {
		return err
	}
	updateResult, err := updatePrepared.ExecuteUpdate(ctx)
	if err != nil {
		return err
	}

	if updateResult != updatePreparedStatementExpectedRows {
		return fmt.Errorf("expected 'UPDATE STATEMENT' return %d got %d", updatePreparedStatementExpectedRows, updateResult)
	}
	return updatePrepared.Close(ctx)
}

func (m *flightSqlScenarioTester) doGetForTestCase(schema *arrow.Schema) chan flight.StreamChunk {
	ch := make(chan flight.StreamChunk)
	close(ch)
	return ch
}

func (m *flightSqlScenarioTester) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq("SELECT STATEMENT", cmd.GetQuery()); err != nil {
		return nil, err
	}

	handle, err := flightsql.CreateStatementQueryTicket([]byte("SELECT STATEMENT HANDLE"))
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: handle}},
		},
		Schema:           flight.SerializeSchema(QuerySchema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (m *flightSqlScenarioTester) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return &flight.SchemaResult{Schema: flight.SerializeSchema(QuerySchema, memory.DefaultAllocator)}, nil
}

func (m *flightSqlScenarioTester) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return QuerySchema, m.doGetForTestCase(QuerySchema), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	err := assertEq([]byte("SELECT PREPARED STATEMENT HANDLE"), cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}
	return m.flightInfoForCommand(desc, QuerySchema), nil
}

func (m *flightSqlScenarioTester) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return &flight.SchemaResult{Schema: flight.SerializeSchema(QuerySchema, memory.DefaultAllocator)}, nil
}

func (m *flightSqlScenarioTester) DoGetPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return QuerySchema, m.doGetForTestCase(QuerySchema), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return m.flightInfoForCommand(desc, schema_ref.Catalogs), nil
}

func (m *flightSqlScenarioTester) DoGetCatalogs(_ context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.Catalogs, m.doGetForTestCase(schema_ref.Catalogs), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoXdbcTypeInfo(_ context.Context, cmd flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return m.flightInfoForCommand(desc, schema_ref.XdbcTypeInfo), nil
}

func (m *flightSqlScenarioTester) DoGetXdbcTypeInfo(context.Context, flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.XdbcTypeInfo, m.doGetForTestCase(schema_ref.XdbcTypeInfo), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoSqlInfo(_ context.Context, cmd flightsql.GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq(int(2), len(cmd.GetInfo())); err != nil {
		return nil, err
	}
	if err := assertEq(flightsql.SqlInfoFlightSqlServerName, flightsql.SqlInfo(cmd.GetInfo()[0])); err != nil {
		return nil, err
	}
	if err := assertEq(flightsql.SqlInfoFlightSqlServerReadOnly, flightsql.SqlInfo(cmd.GetInfo()[1])); err != nil {
		return nil, err
	}

	return m.flightInfoForCommand(desc, schema_ref.SqlInfo), nil
}

func (m *flightSqlScenarioTester) DoGetSqlInfo(context.Context, flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.SqlInfo, m.doGetForTestCase(schema_ref.SqlInfo), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoSchemas(_ context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq("catalog", cmd.GetCatalog()); err != nil {
		return nil, err
	}

	if err := assertEq("db_schema_filter_pattern", cmd.GetDBSchemaFilterPattern()); err != nil {
		return nil, err
	}

	return m.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (m *flightSqlScenarioTester) DoGetDBSchemas(context.Context, flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.DBSchemas, m.doGetForTestCase(schema_ref.DBSchemas), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq("catalog", cmd.GetCatalog()); err != nil {
		return nil, err
	}

	if err := assertEq("db_schema_filter_pattern", cmd.GetDBSchemaFilterPattern()); err != nil {
		return nil, err
	}

	if err := assertEq("table_filter_pattern", cmd.GetTableNameFilterPattern()); err != nil {
		return nil, err
	}

	if err := assertEq(int(2), len(cmd.GetTableTypes())); err != nil {
		return nil, err
	}

	if err := assertEq("table", cmd.GetTableTypes()[0]); err != nil {
		return nil, err
	}

	if err := assertEq("view", cmd.GetTableTypes()[1]); err != nil {
		return nil, err
	}

	if err := assertEq(true, cmd.GetIncludeSchema()); err != nil {
		return nil, err
	}

	return m.flightInfoForCommand(desc, schema_ref.TablesWithIncludedSchema), nil
}

func (m *flightSqlScenarioTester) DoGetTables(context.Context, flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.TablesWithIncludedSchema, m.doGetForTestCase(schema_ref.TablesWithIncludedSchema), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return m.flightInfoForCommand(desc, schema_ref.TableTypes), nil
}

func (m *flightSqlScenarioTester) DoGetTableTypes(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.TableTypes, m.doGetForTestCase(schema_ref.TableTypes), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoPrimaryKeys(_ context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq("catalog", cmd.Catalog); err != nil {
		return nil, err
	}

	if err := assertEq("db_schema", cmd.DBSchema); err != nil {
		return nil, err
	}

	if err := assertEq("table", cmd.Table); err != nil {
		return nil, err
	}

	return m.flightInfoForCommand(desc, schema_ref.PrimaryKeys), nil
}

func (m *flightSqlScenarioTester) DoGetPrimaryKeys(context.Context, flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.PrimaryKeys, m.doGetForTestCase(schema_ref.PrimaryKeys), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoExportedKeys(_ context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq("catalog", cmd.Catalog); err != nil {
		return nil, err
	}

	if err := assertEq("db_schema", cmd.DBSchema); err != nil {
		return nil, err
	}

	if err := assertEq("table", cmd.Table); err != nil {
		return nil, err
	}

	return m.flightInfoForCommand(desc, schema_ref.ExportedKeys), nil
}

func (m *flightSqlScenarioTester) DoGetExportedKeys(context.Context, flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.ExportedKeys, m.doGetForTestCase(schema_ref.ExportedKeys), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoImportedKeys(_ context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq("catalog", cmd.Catalog); err != nil {
		return nil, err
	}

	if err := assertEq("db_schema", cmd.DBSchema); err != nil {
		return nil, err
	}

	if err := assertEq("table", cmd.Table); err != nil {
		return nil, err
	}

	return m.flightInfoForCommand(desc, schema_ref.ImportedKeys), nil
}

func (m *flightSqlScenarioTester) DoGetImportedKeys(context.Context, flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.ImportedKeys, m.doGetForTestCase(schema_ref.ImportedKeys), nil
}

func (m *flightSqlScenarioTester) GetFlightInfoCrossReference(_ context.Context, cmd flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq("pk_catalog", cmd.PKRef.Catalog); err != nil {
		return nil, err
	}

	if err := assertEq("pk_db_schema", cmd.PKRef.DBSchema); err != nil {
		return nil, err
	}

	if err := assertEq("pk_table", cmd.PKRef.Table); err != nil {
		return nil, err
	}

	if err := assertEq("fk_catalog", cmd.FKRef.Catalog); err != nil {
		return nil, err
	}

	if err := assertEq("fk_db_schema", cmd.FKRef.DBSchema); err != nil {
		return nil, err
	}

	if err := assertEq("fk_table", cmd.FKRef.Table); err != nil {
		return nil, err
	}

	return m.flightInfoForCommand(desc, schema_ref.TableTypes), nil
}

func (m *flightSqlScenarioTester) DoGetCrossReference(context.Context, flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return schema_ref.CrossReference, m.doGetForTestCase(schema_ref.CrossReference), nil
}

func (m *flightSqlScenarioTester) DoPutCommandStatementUpdate(_ context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	if err := assertEq("UPDATE STATEMENT", cmd.GetQuery()); err != nil {
		return 0, err
	}

	return updateStatementExpectedRows, nil
}

func (m *flightSqlScenarioTester) CreatePreparedStatement(_ context.Context, request flightsql.ActionCreatePreparedStatementRequest) (res flightsql.ActionCreatePreparedStatementResult, err error) {
	err = assertEq(true, request.GetQuery() == "SELECT PREPARED STATEMENT" || request.GetQuery() == "UPDATE PREPARED STATEMENT")
	if err != nil {
		return
	}

	res.Handle = []byte(request.GetQuery() + " HANDLE")
	return
}

func (m *flightSqlScenarioTester) ClosePreparedStatement(context.Context, flightsql.ActionClosePreparedStatementRequest) error {
	return nil
}

func (m *flightSqlScenarioTester) DoPutPreparedStatementQuery(_ context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, _ flight.MetadataWriter) error {
	err := assertEq([]byte("SELECT PREPARED STATEMENT HANDLE"), cmd.GetPreparedStatementHandle())
	if err != nil {
		return err
	}

	actualSchema := rdr.Schema()
	if err = assertEq(true, actualSchema.Equal(QuerySchema)); err != nil {
		return err
	}

	return nil
}

func (m *flightSqlScenarioTester) DoPutPreparedStatementUpdate(_ context.Context, cmd flightsql.PreparedStatementUpdate, _ flight.MessageReader) (int64, error) {
	err := assertEq([]byte("UPDATE PREPARED STATEMENT HANDLE"), cmd.GetPreparedStatementHandle())
	if err != nil {
		return 0, err
	}

	return updatePreparedStatementExpectedRows, nil
}
