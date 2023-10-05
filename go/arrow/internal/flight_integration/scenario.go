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
	"math"
	"net"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v14/arrow/internal/arrjson"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/internal/types"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
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
	case "ordered":
		return &orderedScenarioTester{}
	case "expiration_time:do_get":
		return &expirationTimeDoGetScenarioTester{}
	case "expiration_time:list_actions":
		return &expirationTimeListActionsScenarioTester{}
	case "expiration_time:cancel_flight_info":
		return &expirationTimeCancelFlightInfoScenarioTester{}
	case "expiration_time:renew_flight_endpoint":
		return &expirationTimeRenewFlightEndpointScenarioTester{}
	case "poll_flight_info":
		return &pollFlightInfoScenarioTester{}
	case "app_metadata_flight_info_endpoint":
		return &appMetadataFlightInfoEndpointScenarioTester{}
	case "flight_sql":
		return &flightSqlScenarioTester{}
	case "flight_sql:extension":
		return &flightSqlExtensionScenarioTester{}
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

type orderedScenarioTester struct {
	flight.BaseFlightServer
}

func (o *orderedScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("ordered")})
	if err != nil {
		return err
	}

	if !info.GetOrdered() {
		return fmt.Errorf("expected to server return FlightInfo.ordered = true")
	}

	var recs []arrow.Record
	for _, ep := range info.Endpoint {
		if len(ep.Location) != 0 {
			return fmt.Errorf("expected to receive empty locations to use the original service: %s",
				ep.Location)
		}

		stream, err := client.DoGet(ctx, ep.Ticket)
		if err != nil {
			return err
		}

		rdr, err := flight.NewRecordReader(stream)
		if err != nil {
			return err
		}
		defer rdr.Release()

		for rdr.Next() {
			record := rdr.Record()
			record.Retain()
			defer record.Release()
			recs = append(recs, record)
		}
		if rdr.Err() != nil {
			return rdr.Err()
		}
	}

	// Build expected records
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
	expected_table, _ := array.TableFromJSON(mem, schema, []string{
		`[
                   {"number": 1},
                   {"number": 2},
                   {"number": 3}
                 ]`,
		`[
                   {"number": 10},
                   {"number": 20},
                   {"number": 30}
                 ]`,
		`[
                   {"number": 100},
                   {"number": 200},
                   {"number": 300}
                 ]`,
	})
	defer expected_table.Release()

	table := array.NewTableFromRecords(schema, recs)
	defer table.Release()
	if !array.TableEqual(table, expected_table) {
		return fmt.Errorf("read data isn't expected\n"+
			"Expected:\n"+
			"%s\n"+
			"num-rows: %d\n"+
			"num-cols: %d\n"+
			"Actual:\n"+
			"%s\n"+
			"num-rows: %d\n"+
			"num-cols: %d",
			expected_table.Schema(),
			expected_table.NumRows(),
			expected_table.NumCols(),
			table.Schema(),
			table.NumRows(),
			table.NumCols())
	}

	return nil
}

func (o *orderedScenarioTester) MakeServer(port int) flight.Server {
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(o)
	initServer(port, srv)
	return srv
}

func (o *orderedScenarioTester) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	ordered := desc.Type == flight.DescriptorCMD && string(desc.Cmd) == "ordered"
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket:   &flight.Ticket{Ticket: []byte("1")},
				Location: []*flight.Location{},
			},
			{
				Ticket:   &flight.Ticket{Ticket: []byte("2")},
				Location: []*flight.Location{},
			},
			{
				Ticket:   &flight.Ticket{Ticket: []byte("3")},
				Location: []*flight.Location{},
			},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
		Ordered:      ordered,
	}, nil
}

func (o *orderedScenarioTester) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	if string(tkt.GetTicket()) == "1" {
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	} else if string(tkt.GetTicket()) == "2" {
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20, 30}, nil)
	} else if string(tkt.GetTicket()) == "3" {
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{100, 200, 300}, nil)
	}
	w := flight.NewRecordWriter(fs, ipc.WithSchema(schema))
	rec := b.NewRecord()
	defer rec.Release()
	w.Write(rec)

	return nil
}

type expirationTimeEndpointStatus struct {
	expirationTime *time.Time
	numGets        uint32
	cancelled      bool
}

type expirationTimeScenarioTester struct {
	flight.BaseFlightServer
	statuses map[int]expirationTimeEndpointStatus
}

func (tester *expirationTimeScenarioTester) MakeServer(port int) flight.Server {
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(tester)
	initServer(port, srv)
	return srv
}

func (tester *expirationTimeScenarioTester) AppendGetFlightInfo(endpoints []*flight.FlightEndpoint, ticket string, expirationTime *time.Time) []*flight.FlightEndpoint {
	index := len(tester.statuses)
	endpoint := flight.FlightEndpoint{
		Ticket:   &flight.Ticket{Ticket: []byte(strconv.Itoa(index) + ": " + ticket)},
		Location: []*flight.Location{},
	}
	if expirationTime != nil {
		endpoint.ExpirationTime = timestamppb.New(*expirationTime)
	}
	endpoints = append(endpoints, &endpoint)
	tester.statuses[index] = expirationTimeEndpointStatus{
		expirationTime: expirationTime,
		numGets:        0,
		cancelled:      false,
	}
	return endpoints
}

func (tester *expirationTimeScenarioTester) ExtractIndexFromTicket(ticket string) (int, error) {
	indexString := strings.SplitN(ticket, ":", 2)[0]
	index, err := strconv.Atoi(indexString)
	if err != nil {
		return 0, fmt.Errorf("invalid flight: no index: %s: %s", ticket, err)
	}
	if index >= len(tester.statuses) {
		return 0, fmt.Errorf("invalid flight: out of index: %s", ticket)
	}
	return index, nil
}

func (tester *expirationTimeScenarioTester) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	tester.statuses = make(map[int]expirationTimeEndpointStatus)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)
	var endpoints []*flight.FlightEndpoint
	endpoints = tester.AppendGetFlightInfo(endpoints, "No expiration time", nil)
	expirationTime5 := time.Now().Add(time.Second * 5)
	endpoints = tester.AppendGetFlightInfo(endpoints, "5 seconds", &expirationTime5)
	expirationTime6 := time.Now().Add(time.Second * 6)
	endpoints = tester.AppendGetFlightInfo(endpoints, "6 seconds", &expirationTime6)
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint:         endpoints,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (tester *expirationTimeScenarioTester) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	ticket := string(tkt.GetTicket())
	index, err := tester.ExtractIndexFromTicket(ticket)
	if err != nil {
		return err
	}
	st := tester.statuses[index]
	if st.cancelled {
		return status.Errorf(codes.InvalidArgument,
			"Invalid flight: cancelled: %s", ticket)
	}
	if st.expirationTime == nil {
		if st.numGets > 0 {
			return status.Errorf(codes.InvalidArgument,
				"Invalid flight: "+
					"can't read multiple times: %s", ticket)
		}
	} else {
		availableDuration := time.Until(*st.expirationTime)
		if availableDuration < 0 {
			return status.Errorf(codes.InvalidArgument,
				"Invalid flight: expired: %s", ticket)
		}
	}
	st.numGets++
	tester.statuses[index] = st
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Uint32Builder).AppendValues([]uint32{uint32(index)}, nil)
	w := flight.NewRecordWriter(fs, ipc.WithSchema(schema))
	rec := b.NewRecord()
	defer rec.Release()
	w.Write(rec)

	return nil
}

func (tester *expirationTimeScenarioTester) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	actions := []string{
		flight.CancelFlightInfoActionType,
		flight.RenewFlightEndpointActionType,
	}

	for _, a := range actions {
		if err := stream.Send(&flight.ActionType{Type: a}); err != nil {
			return err
		}
	}

	return nil
}

func packActionResult(msg proto.Message) (*flight.Result, error) {
	ret := &flight.Result{}
	var err error
	if ret.Body, err = proto.Marshal(msg); err != nil {
		return nil, fmt.Errorf("%w: unable to marshal final response", err)
	}
	return ret, nil
}

func (tester *expirationTimeScenarioTester) DoAction(cmd *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch cmd.Type {
	case flight.CancelFlightInfoActionType:
		var request flight.CancelFlightInfoRequest
		if err := proto.Unmarshal(cmd.Body, &request); err != nil {
			return status.Errorf(codes.InvalidArgument, "unable to parse command: %s", err.Error())
		}

		cancelStatus := flight.CancelStatusUnspecified
		for _, ep := range request.Info.Endpoint {
			ticket := string(ep.Ticket.Ticket)
			index, err := tester.ExtractIndexFromTicket(ticket)
			if err == nil {
				st := tester.statuses[index]
				if st.cancelled {
					cancelStatus = flight.CancelStatusNotCancellable
				} else {
					st.cancelled = true
					if cancelStatus == flight.CancelStatusUnspecified {
						cancelStatus = flight.CancelStatusCancelled
					}
					tester.statuses[index] = st
				}
			} else {
				cancelStatus = flight.CancelStatusNotCancellable
			}
		}
		result := flight.CancelFlightInfoResult{Status: cancelStatus}
		out, err := packActionResult(&result)
		if err != nil {
			return err
		}
		if err = stream.Send(out); err != nil {
			return err
		}
		return nil
	case flight.RenewFlightEndpointActionType:
		var request flight.RenewFlightEndpointRequest
		if err := proto.Unmarshal(cmd.Body, &request); err != nil {
			return status.Errorf(codes.InvalidArgument, "unable to parse command: %s", err.Error())
		}

		endpoint := request.Endpoint
		ticket := string(endpoint.Ticket.Ticket)
		index, err := tester.ExtractIndexFromTicket(ticket)
		if err != nil {
			return err
		}
		endpoint.Ticket.Ticket = []byte(string(endpoint.Ticket.Ticket) + ": renewed (+ 10 seconds)")
		renewedExpirationTime := time.Now().Add(time.Second * 10)
		endpoint.ExpirationTime = timestamppb.New(renewedExpirationTime)
		st := tester.statuses[index]
		st.expirationTime = &renewedExpirationTime
		tester.statuses[index] = st
		out, err := packActionResult(endpoint)
		if err != nil {
			return err
		}
		if err = stream.Send(out); err != nil {
			return err
		}
		return nil
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported action: %s", cmd.Type)
	}
}

type expirationTimeDoGetScenarioTester struct {
	expirationTimeScenarioTester
}

func (tester *expirationTimeDoGetScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("expiration_time")})
	if err != nil {
		return err
	}

	var recs []arrow.Record
	for _, ep := range info.Endpoint {
		if len(recs) == 0 {
			if ep.ExpirationTime != nil {
				return fmt.Errorf("endpoints[0] must not have " +
					"expiration time")
			}
		} else {
			if ep.ExpirationTime == nil {
				return fmt.Errorf("endpoints[1] must have " +
					"expiration time")
			}
		}

		if len(ep.Location) != 0 {
			return fmt.Errorf("expected to receive empty locations to use the original service: %s",
				ep.Location)
		}

		stream, err := client.DoGet(ctx, ep.Ticket)
		if err != nil {
			return err
		}

		rdr, err := flight.NewRecordReader(stream)
		if err != nil {
			return err
		}
		defer rdr.Release()

		for rdr.Next() {
			record := rdr.Record()
			record.Retain()
			defer record.Release()
			recs = append(recs, record)
		}
		if rdr.Err() != nil {
			return rdr.Err()
		}
	}

	// Build expected records
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)
	expectedTable, _ := array.TableFromJSON(mem, schema, []string{
		`[{"number": 0}]`,
		`[{"number": 1}]`,
		`[{"number": 2}]`,
	})
	defer expectedTable.Release()

	table := array.NewTableFromRecords(schema, recs)
	defer table.Release()
	if !array.TableEqual(table, expectedTable) {
		return fmt.Errorf("read data isn't expected\n"+
			"Expected:\n"+
			"%s\n"+
			"numRows: %d\n"+
			"numCols: %d\n"+
			"Actual:\n"+
			"%s\n"+
			"numRows: %d\n"+
			"numCols: %d",
			expectedTable.Schema(),
			expectedTable.NumRows(),
			expectedTable.NumCols(),
			table.Schema(),
			table.NumRows(),
			table.NumCols())
	}

	return nil
}

type expirationTimeListActionsScenarioTester struct {
	expirationTimeScenarioTester
}

func (tester *expirationTimeListActionsScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	stream, err := client.ListActions(ctx, &flight.Empty{})
	if err != nil {
		return err
	}

	var actionTypeNames []string
	for {
		actionType, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		actionTypeNames = append(actionTypeNames, actionType.Type)
	}
	sort.Strings(actionTypeNames)
	expectedActionTypeNames := []string{
		"CancelFlightInfo",
		"RenewFlightEndpoint",
	}
	if !reflect.DeepEqual(actionTypeNames, expectedActionTypeNames) {
		return fmt.Errorf("action types aren't expected\n"+
			"Expected:\n"+
			"%s\n"+
			"Actual:\n"+
			"%s",
			expectedActionTypeNames,
			actionTypeNames)
	}

	return nil
}

type expirationTimeCancelFlightInfoScenarioTester struct {
	expirationTimeScenarioTester
}

func (tester *expirationTimeCancelFlightInfoScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("expiration_time")})
	if err != nil {
		return err
	}

	request := flight.CancelFlightInfoRequest{Info: info}
	result, err := client.CancelFlightInfo(ctx, &request)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if result.Status != flight.CancelStatusCancelled {
		return fmt.Errorf("invalid: CancelFlightInfo must return CANCEL_STATUS_CANCELLED: %s", result.Status)
	}
	for _, ep := range info.Endpoint {
		stream, err := client.DoGet(ctx, ep.Ticket)
		if err != nil {
			return err
		}
		rdr, err := flight.NewRecordReader(stream)
		if err == nil {
			rdr.Release()
			return fmt.Errorf("invalid: DoGet after CancelFlightInfo must be failed")
		}
	}

	return nil
}

type expirationTimeRenewFlightEndpointScenarioTester struct {
	expirationTimeScenarioTester
}

func (tester *expirationTimeRenewFlightEndpointScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("expiration_time")})
	if err != nil {
		return err
	}

	// Renew all endpoints that have expiration time
	for _, ep := range info.Endpoint {
		if ep.ExpirationTime == nil {
			continue
		}
		expirationTime := ep.ExpirationTime.AsTime()
		request := flight.RenewFlightEndpointRequest{Endpoint: ep}
		renewedEndpoint, err := client.RenewFlightEndpoint(ctx, &request)
		if err != nil {
			return err
		}
		if renewedEndpoint.ExpirationTime == nil {
			return fmt.Errorf("renewed endpoint must have expiration time: %s",
				renewedEndpoint)
		}
		renewedExpirationTime := renewedEndpoint.ExpirationTime.AsTime()
		if renewedExpirationTime.Sub(expirationTime) <= 0 {
			return fmt.Errorf("renewed endpoint must have newer expiration time\n"+
				"Original: %s\nRenewed: %s",
				ep, renewedEndpoint)
		}
	}

	return nil
}

type pollFlightInfoScenarioTester struct {
	flight.BaseFlightServer
}

func (tester *pollFlightInfoScenarioTester) MakeServer(port int) flight.Server {
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(tester)
	initServer(port, srv)
	return srv
}

func (tester *pollFlightInfoScenarioTester) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)
	endpoints := []*flight.FlightEndpoint{
		{
			Ticket:   &flight.Ticket{Ticket: []byte("long-running query")},
			Location: []*flight.Location{},
		},
	}
	info := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint:         endpoints,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
	pollDesc := flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte("poll"),
	}
	if desc.Type == pollDesc.Type && string(desc.Cmd) == string(pollDesc.Cmd) {
		progress := float64(1.0)
		return &flight.PollInfo{
			Info:             info,
			FlightDescriptor: nil,
			Progress:         &progress,
			ExpirationTime:   nil,
		}, nil
	} else {
		progress := float64(0.1)
		return &flight.PollInfo{
			Info:             info,
			FlightDescriptor: &pollDesc,
			Progress:         &progress,
			ExpirationTime:   timestamppb.New(time.Now().Add(time.Second * 10)),
		}, nil
	}
}

func (tester *pollFlightInfoScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	desc := flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte("heavy query"),
	}
	info, err := client.PollFlightInfo(ctx, &desc)
	if err != nil {
		return err
	}
	switch {
	case info.FlightDescriptor == nil:
		return fmt.Errorf("description is missing: %s", info.String())
	case info.Progress == nil:
		return fmt.Errorf("progress is missing: %s", info.String())
	case !(0.0 <= *info.Progress && *info.Progress <= 1.0):
		return fmt.Errorf("invalid progress: %s", info.String())
	case info.ExpirationTime == nil:
		return fmt.Errorf("expiration time is missing: %s", info.String())
	}

	info, err = client.PollFlightInfo(ctx, info.FlightDescriptor)
	if err != nil {
		return err
	}
	switch {
	case info.FlightDescriptor != nil:
		return fmt.Errorf("retried but no finished yet: %s", info.String())
	case info.Progress == nil:
		return fmt.Errorf("progress is missing in finished query: %s",
			info.String())
	case math.Abs(*info.Progress-1.0) > 1e-5:
		return fmt.Errorf("progress for finished query isn't 1.0: %s",
			info.String())
	case info.ExpirationTime != nil:
		return fmt.Errorf("expiration time must not be set for finished query: %s",
			info.String())
	}

	return nil
}

type appMetadataFlightInfoEndpointScenarioTester struct {
	flight.BaseFlightServer
}

func (tester *appMetadataFlightInfoEndpointScenarioTester) MakeServer(port int) flight.Server {
	srv := flight.NewServerWithMiddleware(nil)
	srv.RegisterFlightService(tester)
	initServer(port, srv)
	return srv
}

func (tester *appMetadataFlightInfoEndpointScenarioTester) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "number", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)

	if desc.Type != flight.DescriptorCMD {
		return nil, fmt.Errorf("%w: should have received CMD descriptor", arrow.ErrInvalid)
	}
	endpoints := []*flight.FlightEndpoint{{AppMetadata: desc.Cmd}}
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		Endpoint:         endpoints,
		TotalRecords:     -1,
		TotalBytes:       -1,
		AppMetadata:      desc.Cmd,
	}, nil
}

func (tester *appMetadataFlightInfoEndpointScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx := context.Background()
	desc := flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  []byte("foobar"),
	}
	info, err := client.GetFlightInfo(ctx, &desc)
	if err != nil {
		return err
	}
	switch {
	case !bytes.Equal(desc.Cmd, info.AppMetadata):
		return fmt.Errorf("invalid flight info app_metadata: %s, expected: %s", info.AppMetadata, desc.Cmd)
	case len(info.Endpoint) != 1:
		return fmt.Errorf("expected exactly 1 flight endpoint, got: %d", len(info.Endpoint))
	case !bytes.Equal(desc.Cmd, info.Endpoint[0].AppMetadata):
		return fmt.Errorf("invalid flight endpoint app_metadata: %s, expected: %s", info.Endpoint[0].AppMetadata, desc.Cmd)
	}
	return nil
}

const (
	updateStatementExpectedRows                        int64 = 10000
	updateStatementWithTransactionExpectedRows         int64 = 15000
	updatePreparedStatementExpectedRows                int64 = 20000
	updatePreparedStatementWithTransactionExpectedRows int64 = 25000
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
	m.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerSql, false)
	m.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerSubstrait, true)
	m.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerSubstraitMinVersion, "min_version")
	m.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerSubstraitMaxVersion, "max_version")
	m.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlTransactionSavepoint))
	m.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerCancel, true)
	m.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerStatementTimeout, int32(42))
	m.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransactionTimeout, int32(7))

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
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
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
	if err = m.validate(getQuerySchema(), info, client); err != nil {
		return err
	}

	schema, err := client.GetExecuteSchema(ctx, "SELECT STATEMENT")
	if err != nil {
		return err
	}
	if err = m.validateSchema(getQuerySchema(), schema); err != nil {
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
	prepared, err := client.Prepare(ctx, "SELECT PREPARED STATEMENT")
	if err != nil {
		return err
	}

	arr, _, _ := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64, strings.NewReader("[1]"))
	defer arr.Release()
	params := array.NewRecord(getQuerySchema(), []arrow.Array{arr}, 1)
	defer params.Release()
	prepared.SetParameters(params)

	info, err := prepared.Execute(ctx)
	if err != nil {
		return err
	}
	if err = m.validate(getQuerySchema(), info, client); err != nil {
		return err
	}
	schema, err := prepared.GetSchema(ctx)
	if err != nil {
		return err
	}
	if err = m.validateSchema(getQuerySchema(), schema); err != nil {
		return err
	}

	if err = prepared.Close(ctx); err != nil {
		return err
	}

	updatePrepared, err := client.Prepare(ctx, "UPDATE PREPARED STATEMENT")
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
	if err := assertEq(selectStatement, cmd.GetQuery()); err != nil {
		return nil, err
	}

	var (
		ticket []byte
		schema *arrow.Schema
	)
	if len(cmd.GetTransactionId()) == 0 {
		ticket = []byte("SELECT STATEMENT HANDLE")
		schema = getQuerySchema()
	} else {
		ticket = []byte("SELECT STATEMENT WITH TXN HANDLE")
		schema = getQueryWithTransactionSchema()
	}

	handle, err := flightsql.CreateStatementQueryTicket(ticket)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: handle}},
		},
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (m *flightSqlScenarioTester) GetFlightInfoSubstraitPlan(ctx context.Context, cmd flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := assertEq([]byte(substraitPlanText), cmd.GetPlan().Plan); err != nil {
		return nil, fmt.Errorf("%w: unexpected plan in GetFlightInfoSubstraitPlan", err)
	}

	if err := assertEq(substraitPlanVersion, cmd.GetPlan().Version); err != nil {
		return nil, fmt.Errorf("%w: unexpected version in GetFlightInfoSubstraitPlan", err)
	}

	var (
		ticket []byte
		schema *arrow.Schema
	)
	if len(cmd.GetTransactionId()) == 0 {
		ticket = []byte("PLAN HANDLE")
		schema = getQuerySchema()
	} else {
		ticket = []byte("PLAN WITH TXN HANDLE")
		schema = getQueryWithTransactionSchema()
	}

	handle, err := flightsql.CreateStatementQueryTicket(ticket)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: handle}},
		},
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (m *flightSqlScenarioTester) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if err := assertEq(selectStatement, cmd.GetQuery()); err != nil {
		return nil, fmt.Errorf("%w: unexpected statement in GetSchemaStatement", err)
	}

	if len(cmd.GetTransactionId()) == 0 {
		return &flight.SchemaResult{Schema: flight.SerializeSchema(getQuerySchema(), memory.DefaultAllocator)}, nil
	}

	return &flight.SchemaResult{Schema: flight.SerializeSchema(getQueryWithTransactionSchema(), memory.DefaultAllocator)}, nil
}

func (m *flightSqlScenarioTester) GetSchemaSubstraitPlan(ctx context.Context, cmd flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if err := assertEq([]byte(substraitPlanText), cmd.GetPlan().Plan); err != nil {
		return nil, fmt.Errorf("%w: unexpected plan in GetFlightInfoSubstraitPlan", err)
	}

	if err := assertEq(substraitPlanVersion, cmd.GetPlan().Version); err != nil {
		return nil, fmt.Errorf("%w: unexpected version in GetFlightInfoSubstraitPlan", err)
	}

	if len(cmd.GetTransactionId()) == 0 {
		return &flight.SchemaResult{Schema: flight.SerializeSchema(getQuerySchema(), memory.DefaultAllocator)}, nil
	}

	return &flight.SchemaResult{Schema: flight.SerializeSchema(getQueryWithTransactionSchema(), memory.DefaultAllocator)}, nil
}

func (m *flightSqlScenarioTester) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	switch string(cmd.GetStatementHandle()) {
	case "SELECT STATEMENT HANDLE", "PLAN HANDLE":
		return getQuerySchema(), m.doGetForTestCase(getQuerySchema()), nil
	case "SELECT STATEMENT WITH TXN HANDLE", "PLAN WITH TXN HANDLE":
		return getQueryWithTransactionSchema(), m.doGetForTestCase(getQueryWithTransactionSchema()), nil
	}

	return nil, nil, fmt.Errorf("%w: unknown handle %s", arrow.ErrInvalid, string(cmd.GetStatementHandle()))
}

func (m *flightSqlScenarioTester) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	switch string(cmd.GetPreparedStatementHandle()) {
	case "SELECT PREPARED STATEMENT HANDLE", "PLAN HANDLE":
		return m.flightInfoForCommand(desc, getQuerySchema()), nil
	case "SELECT PREPARED STATEMENT WITH TXN HANDLE", "PLAN WITH TXN HANDLE":
		return m.flightInfoForCommand(desc, getQueryWithTransactionSchema()), nil
	}
	return nil, fmt.Errorf("%w: invalid handle for GetFlightInfoPreparedStatement %s",
		arrow.ErrInvalid, string(cmd.GetPreparedStatementHandle()))
}

func (m *flightSqlScenarioTester) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	switch string(cmd.GetPreparedStatementHandle()) {
	case "SELECT PREPARED STATEMENT HANDLE", "PLAN HANDLE":
		return &flight.SchemaResult{Schema: flight.SerializeSchema(getQuerySchema(), memory.DefaultAllocator)}, nil
	case "SELECT PREPARED STATEMENT WITH TXN HANDLE", "PLAN WITH TXN HANDLE":
		return &flight.SchemaResult{Schema: flight.SerializeSchema(getQueryWithTransactionSchema(), memory.DefaultAllocator)}, nil
	}
	return nil, fmt.Errorf("%w: invalid handle for GetSchemaPreparedStaement %s",
		arrow.ErrInvalid, string(cmd.GetPreparedStatementHandle()))
}

func (m *flightSqlScenarioTester) DoGetPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	switch string(cmd.GetPreparedStatementHandle()) {
	case "SELECT PREPARED STATEMENT HANDLE", "PLAN HANDLE":
		return getQuerySchema(), m.doGetForTestCase(getQuerySchema()), nil
	case "SELECT PREPARED STATEMENT WITH TXN HANDLE", "PLAN WITH TXN HANDLE":
		return getQueryWithTransactionSchema(), m.doGetForTestCase(getQueryWithTransactionSchema()), nil
	}
	return nil, nil, fmt.Errorf("%w: invalid handle: %s",
		arrow.ErrInvalid, string(cmd.GetPreparedStatementHandle()))
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

func (m *flightSqlScenarioTester) GetFlightInfoSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(cmd.GetInfo()) == 2 {
		// integration test for the protocol messages

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

	// integration test for the values themselves
	return m.BaseServer.GetFlightInfoSqlInfo(ctx, cmd, desc)
}

func (m *flightSqlScenarioTester) DoGetSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if len(cmd.GetInfo()) == 2 {
		return schema_ref.SqlInfo, m.doGetForTestCase(schema_ref.SqlInfo), nil
	}

	return m.BaseServer.DoGetSqlInfo(ctx, cmd)
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

	if len(cmd.GetTransactionId()) == 0 {
		return updateStatementExpectedRows, nil
	}
	return updateStatementWithTransactionExpectedRows, nil
}

func (m *flightSqlScenarioTester) DoPutCommandSubstraitPlan(_ context.Context, cmd flightsql.StatementSubstraitPlan) (int64, error) {
	if err := assertEq([]byte(substraitPlanText), cmd.GetPlan().Plan); err != nil {
		return 0, fmt.Errorf("%w: wrong plan for DoPutCommandSubstraitPlan", err)
	}

	if err := assertEq(substraitPlanVersion, cmd.GetPlan().Version); err != nil {
		return 0, fmt.Errorf("%w: unexpected version in DoPutCommandSubstraitPlan", err)
	}

	if len(cmd.GetTransactionId()) == 0 {
		return updateStatementExpectedRows, nil
	}
	return updateStatementWithTransactionExpectedRows, nil
}

func (m *flightSqlScenarioTester) CreatePreparedStatement(_ context.Context, request flightsql.ActionCreatePreparedStatementRequest) (res flightsql.ActionCreatePreparedStatementResult, err error) {
	switch request.GetQuery() {
	case "SELECT PREPARED STATEMENT", "UPDATE PREPARED STATEMENT":
	default:
		return res, fmt.Errorf("%w: unexpected query %s", arrow.ErrInvalid, request.GetQuery())
	}

	handle := request.GetQuery()
	if len(request.GetTransactionId()) != 0 {
		handle += " WITH TXN"
	}
	res.Handle = []byte(handle + " HANDLE")
	return
}

func (m *flightSqlScenarioTester) CreatePreparedSubstraitPlan(_ context.Context, request flightsql.ActionCreatePreparedSubstraitPlanRequest) (res flightsql.ActionCreatePreparedStatementResult, err error) {
	if err := assertEq([]byte(substraitPlanText), request.GetPlan().Plan); err != nil {
		return res, fmt.Errorf("%w: wrong plan for CreatePreparedSubstraitPlan", err)
	}

	if err := assertEq(substraitPlanVersion, request.GetPlan().Version); err != nil {
		return res, fmt.Errorf("%w: unexpected version in DoPutCommandSubstraitPlan", err)
	}

	if len(request.GetTransactionId()) == 0 {
		res.Handle = []byte("PLAN HANDLE")
	} else {
		res.Handle = []byte("PLAN WITH TXN HANDLE")
	}
	return
}

func (m *flightSqlScenarioTester) ClosePreparedStatement(_ context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	switch string(request.GetPreparedStatementHandle()) {
	case "SELECT PREPARED STATEMENT HANDLE",
		"UPDATE PREPARED STATEMENT HANDLE",
		"PLAN HANDLE",
		"SELECT PREPARED STATEMENT WITH TXN HANDLE",
		"UPDATE PREPARED STATEMENT WITH TXN HANDLE",
		"PLAN WITH TXN HANDLE":
	default:
		return fmt.Errorf("%w: invalid handle for ClosePreparedStatement: %s",
			arrow.ErrInvalid, string(request.GetPreparedStatementHandle()))
	}

	return nil
}

func (m *flightSqlScenarioTester) DoPutPreparedStatementQuery(_ context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, _ flight.MetadataWriter) error {
	switch string(cmd.GetPreparedStatementHandle()) {
	case "SELECT PREPARED STATEMENT HANDLE",
		"SELECT PREPARED STATEMENT WITH TXN HANDLE",
		"PLAN HANDLE", "PLAN WITH TXN HANDLE":
		actualSchema := rdr.Schema()
		return assertEq(true, actualSchema.Equal(getQuerySchema()))
	}

	return fmt.Errorf("%w: handle for DoPutPreparedStatementQuery '%s'",
		arrow.ErrInvalid, string(cmd.GetPreparedStatementHandle()))
}

func (m *flightSqlScenarioTester) DoPutPreparedStatementUpdate(_ context.Context, cmd flightsql.PreparedStatementUpdate, _ flight.MessageReader) (int64, error) {
	switch string(cmd.GetPreparedStatementHandle()) {
	case "UPDATE PREPARED STATEMENT HANDLE", "PLAN HANDLE":
		return updatePreparedStatementExpectedRows, nil
	case "UPDATE PREPARED STATEMENT WITH TXN HANDLE", "PLAN WITH TXN HANDLE":
		return updatePreparedStatementWithTransactionExpectedRows, nil
	}

	return 0, fmt.Errorf("%w: handle for DoPutPreparedStatementUpdate '%s'",
		arrow.ErrInvalid, string(cmd.GetPreparedStatementHandle()))
}

func (m *flightSqlScenarioTester) BeginSavepoint(_ context.Context, request flightsql.ActionBeginSavepointRequest) ([]byte, error) {
	if err := assertEq(savepointName, request.GetName()); err != nil {
		return nil, fmt.Errorf("%w: unexpected savepoint name in BeginSavepoint", err)
	}

	if err := assertEq([]byte(transactionID), request.GetTransactionId()); err != nil {
		return nil, fmt.Errorf("%w: unexpected transaction ID in BeginSavepoint", err)
	}

	return []byte(savepointID), nil
}

func (m *flightSqlScenarioTester) BeginTransaction(context.Context, flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	return []byte(transactionID), nil
}

func (m *flightSqlScenarioTester) CancelFlightInfo(_ context.Context, request *flight.CancelFlightInfoRequest) (flight.CancelFlightInfoResult, error) {
	result := flight.CancelFlightInfoResult{Status: flight.CancelStatusUnspecified}
	if err := assertEq(1, len(request.Info.Endpoint)); err != nil {
		return result, fmt.Errorf("%w: expected 1 endpoint for CancelQuery", err)
	}

	endpoint := request.Info.Endpoint[0]
	tkt, err := flightsql.GetStatementQueryTicket(endpoint.Ticket)
	if err != nil {
		return result, err
	}

	if err := assertEq([]byte("PLAN HANDLE"), tkt.GetStatementHandle()); err != nil {
		return result, fmt.Errorf("%w: unexpected ticket in CancelQuery", err)
	}

	result.Status = flight.CancelStatusCancelled
	return result, nil
}

func (m *flightSqlScenarioTester) EndSavepoint(_ context.Context, request flightsql.ActionEndSavepointRequest) error {
	switch request.GetAction() {
	case flightsql.EndSavepointRelease, flightsql.EndSavepointRollback:
		if err := assertEq([]byte(savepointID), request.GetSavepointId()); err != nil {
			return fmt.Errorf("%w: unexpected savepoint ID in EndSavepoint", err)
		}
		return nil
	}

	return fmt.Errorf("%w: unknown action %v", arrow.ErrInvalid, request.GetAction())
}

func (m *flightSqlScenarioTester) EndTransaction(_ context.Context, request flightsql.ActionEndTransactionRequest) error {
	switch request.GetAction() {
	case flightsql.EndTransactionCommit, flightsql.EndTransactionRollback:
		if err := assertEq([]byte(transactionID), request.GetTransactionId()); err != nil {
			return fmt.Errorf("%w: unexpected transaction ID in EndTransaction", err)
		}
		return nil
	}

	return fmt.Errorf("%w: unknown action %v", arrow.ErrInvalid, request.GetAction())
}

// schema to be returned for mocking the statement/prepared statement results
func getQuerySchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: *flightsql.NewColumnMetadataBuilder().
				TableName("test").
				IsAutoIncrement(true).
				IsCaseSensitive(false).
				TypeName("type_test").
				SchemaName("schema_test").
				IsSearchable(true).
				CatalogName("catalog_test").
				Precision(100).
				Build().Data}}, nil)
}

func getQueryWithTransactionSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "pkey", Type: arrow.PrimitiveTypes.Int32, Nullable: true,
			Metadata: *flightsql.NewColumnMetadataBuilder().
				TableName("test").
				IsAutoIncrement(true).
				IsCaseSensitive(false).
				TypeName("type_test").
				SchemaName("schema_test").
				IsSearchable(true).
				CatalogName("catalog_test").
				Precision(100).Build().Data}}, nil)
}

const (
	substraitPlanText    = "plan"
	substraitPlanVersion = "version"
	selectStatement      = "SELECT STATEMENT"
	savepointID          = "savepoint_id"
	savepointName        = "savepoint_name"
	transactionID        = "transaction_id"
)

var substraitPlan = flightsql.SubstraitPlan{
	Plan: []byte(substraitPlanText), Version: substraitPlanVersion}

type flightSqlExtensionScenarioTester struct {
	flightSqlScenarioTester
}

func (m *flightSqlExtensionScenarioTester) RunClient(addr string, opts ...grpc.DialOption) error {
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
	if err := m.ValidatePreparedStatementExecution(client); err != nil {
		return err
	}

	return m.ValidateTransactions(client)
}

func (m *flightSqlExtensionScenarioTester) ValidateMetadataRetrieval(client *flightsql.Client) error {
	sqlInfo := []flightsql.SqlInfo{
		flightsql.SqlInfoFlightSqlServerSql,
		flightsql.SqlInfoFlightSqlServerSubstrait,
		flightsql.SqlInfoFlightSqlServerSubstraitMinVersion,
		flightsql.SqlInfoFlightSqlServerSubstraitMaxVersion,
		flightsql.SqlInfoFlightSqlServerTransaction,
		flightsql.SqlInfoFlightSqlServerCancel,
		flightsql.SqlInfoFlightSqlServerStatementTimeout,
		flightsql.SqlInfoFlightSqlServerTransactionTimeout,
	}
	ctx := context.Background()

	info, err := client.GetSqlInfo(ctx, sqlInfo)
	if err != nil {
		return err
	}

	rdr, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return err
	}
	defer rdr.Release()

	actualSchema := rdr.Schema()
	if !schema_ref.SqlInfo.Equal(actualSchema) {
		return fmt.Errorf("%w: schemas did not match. expected: %s\n got: %s",
			arrow.ErrInvalid, schema_ref.SqlInfo, actualSchema)
	}

	infoValues := make(flightsql.SqlInfoResultMap)
	for rdr.Next() {
		rec := rdr.Record()
		names, values := rec.Column(0).(*array.Uint32), rec.Column(1).(*array.DenseUnion)

		for i := 0; i < int(rec.NumRows()); i++ {
			code := names.Value(i)
			if _, ok := infoValues[code]; ok {
				return fmt.Errorf("%w: duplicate SqlInfo value %d", arrow.ErrInvalid, code)
			}

			switch values.TypeCode(i) {
			case 0: // string
				infoValues[code] = values.Field(0).(*array.String).
					Value(int(values.ValueOffset(i)))
			case 1: // bool
				infoValues[code] = values.Field(1).(*array.Boolean).
					Value(int(values.ValueOffset(i)))
			case 2: // int64
				infoValues[code] = values.Field(2).(*array.Int64).
					Value(int(values.ValueOffset(i)))
			case 3: // int32
				infoValues[code] = values.Field(3).(*array.Int32).
					Value(int(values.ValueOffset(i)))
			default:
				return fmt.Errorf("%w: decoding SqlInfoResult of type code %d",
					arrow.ErrNotImplemented, values.TypeCode(i))
			}
		}
	}

	if rdr.Err() != nil {
		return rdr.Err()
	}

	for k, v := range infoValues {
		switch k {
		case uint32(flightsql.SqlInfoFlightSqlServerSql):
			if err := assertEq(false, v); err != nil {
				return fmt.Errorf("%w: %v did not match", err, k)
			}
		case uint32(flightsql.SqlInfoFlightSqlServerSubstrait):
			if err := assertEq(true, v); err != nil {
				return fmt.Errorf("%w: %v did not match", err, k)
			}
		case uint32(flightsql.SqlInfoFlightSqlServerSubstraitMinVersion):
			if err := assertEq("min_version", v); err != nil {
				return fmt.Errorf("%w: %v did not match", err, k)
			}
		case uint32(flightsql.SqlInfoFlightSqlServerSubstraitMaxVersion):
			if err := assertEq("max_version", v); err != nil {
				return fmt.Errorf("%w: %v did not match", err, k)
			}
		case uint32(flightsql.SqlInfoFlightSqlServerTransaction):
			if err := assertEq(int32(flightsql.SqlTransactionSavepoint), v); err != nil {
				return fmt.Errorf("%w: %v did not match", err, k)
			}
		case uint32(flightsql.SqlInfoFlightSqlServerCancel):
			if err := assertEq(true, v); err != nil {
				return fmt.Errorf("%w: %v did not match", err, k)
			}
		case uint32(flightsql.SqlInfoFlightSqlServerStatementTimeout):
			if err := assertEq(int32(42), v); err != nil {
				return fmt.Errorf("%w: %v did not match", err, k)
			}
		case uint32(flightsql.SqlInfoFlightSqlServerTransactionTimeout):
			if err := assertEq(int32(7), v); err != nil {
				return fmt.Errorf("%w: %v did not match", err, k)
			}
		}

	}

	return nil
}

func (m *flightSqlExtensionScenarioTester) ValidateStatementExecution(client *flightsql.Client) error {
	ctx := context.Background()
	info, err := client.ExecuteSubstrait(ctx, substraitPlan)
	if err != nil {
		return err
	}
	if err := m.validate(getQuerySchema(), info, client); err != nil {
		return err
	}

	schema, err := client.GetExecuteSubstraitSchema(ctx, substraitPlan)
	if err != nil {
		return err
	}

	if err := m.validateSchema(getQuerySchema(), schema); err != nil {
		return err
	}

	info, err = client.ExecuteSubstrait(ctx, substraitPlan)
	if err != nil {
		return err
	}

	//lint:ignore SA1019 for backward compatibility
	cancelResult, err := client.CancelQuery(ctx, info)
	if err != nil {
		return err
	}

	if err := assertEq(flightsql.CancelResultCancelled, cancelResult); err != nil {
		return fmt.Errorf("%w: wrong cancel result", err)
	}

	updatedRows, err := client.ExecuteSubstraitUpdate(ctx, substraitPlan)
	if err != nil {
		return err
	}

	if err := assertEq(updateStatementExpectedRows, updatedRows); err != nil {
		return fmt.Errorf("%w: wrong number of updated rows for ExecuteSubstraitUpdate", err)
	}

	return nil
}

func (m *flightSqlExtensionScenarioTester) ValidatePreparedStatementExecution(client *flightsql.Client) error {
	arr, _, _ := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64, strings.NewReader("[1]"))
	defer arr.Release()
	params := array.NewRecord(getQuerySchema(), []arrow.Array{arr}, 1)
	defer params.Release()

	ctx := context.Background()
	stmt, err := client.PrepareSubstrait(ctx, substraitPlan)
	if err != nil {
		return err
	}

	stmt.SetParameters(params)
	info, err := stmt.Execute(ctx)
	if err != nil {
		return err
	}

	if err := m.validate(getQuerySchema(), info, client); err != nil {
		return err
	}

	schema, err := stmt.GetSchema(ctx)
	if err != nil {
		return err
	}

	if err := m.validateSchema(getQuerySchema(), schema); err != nil {
		return err
	}

	if err := stmt.Close(ctx); err != nil {
		return err
	}

	updateStmt, err := client.PrepareSubstrait(ctx, substraitPlan)
	if err != nil {
		return err
	}

	updatedRows, err := updateStmt.ExecuteUpdate(ctx)
	if err != nil {
		return err
	}

	if err := assertEq(updatePreparedStatementExpectedRows, updatedRows); err != nil {
		return err
	}

	return updateStmt.Close(ctx)
}

func (m *flightSqlExtensionScenarioTester) ValidateTransactions(client *flightsql.Client) error {
	ctx := context.Background()
	txn, err := client.BeginTransaction(ctx)
	if err != nil {
		return err
	}

	if err := assertEq([]byte(transactionID), []byte(txn.ID())); err != nil {
		return err
	}

	sp, err := txn.BeginSavepoint(ctx, savepointName)
	if err != nil {
		return err
	}

	if err := assertEq([]byte(savepointID), []byte(sp)); err != nil {
		return err
	}

	info, err := txn.Execute(ctx, selectStatement)
	if err != nil {
		return err
	}

	if err := m.validate(getQueryWithTransactionSchema(), info, client); err != nil {
		return err
	}

	info, err = txn.ExecuteSubstrait(ctx, substraitPlan)
	if err != nil {
		return err
	}

	if err := m.validate(getQueryWithTransactionSchema(), info, client); err != nil {
		return err
	}

	schema, err := txn.GetExecuteSchema(ctx, selectStatement)
	if err != nil {
		return err
	}

	if err := m.validateSchema(getQueryWithTransactionSchema(), schema); err != nil {
		return err
	}

	schema, err = txn.GetExecuteSubstraitSchema(ctx, substraitPlan)
	if err != nil {
		return err
	}

	if err := m.validateSchema(getQueryWithTransactionSchema(), schema); err != nil {
		return err
	}

	updated, err := txn.ExecuteUpdate(ctx, "UPDATE STATEMENT")
	if err != nil {
		return err
	}

	if err := assertEq(updateStatementWithTransactionExpectedRows, updated); err != nil {
		return err
	}

	updated, err = txn.ExecuteSubstraitUpdate(ctx, substraitPlan)
	if err != nil {
		return err
	}

	if err := assertEq(updateStatementWithTransactionExpectedRows, updated); err != nil {
		return err
	}

	arr, _, _ := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64, strings.NewReader("[1]"))
	defer arr.Release()
	params := array.NewRecord(getQuerySchema(), []arrow.Array{arr}, 1)
	defer params.Release()

	prepared, err := txn.Prepare(ctx, "SELECT PREPARED STATEMENT")
	if err != nil {
		return err
	}
	prepared.SetParameters(params)

	info, err = prepared.Execute(ctx)
	if err != nil {
		return err
	}

	if err := m.validate(getQueryWithTransactionSchema(), info, client); err != nil {
		return err
	}

	schema, err = prepared.GetSchema(ctx)
	if err != nil {
		return err
	}

	if err := m.validateSchema(getQueryWithTransactionSchema(), schema); err != nil {
		return err
	}

	if err := prepared.Close(ctx); err != nil {
		return err
	}

	prepared, err = txn.PrepareSubstrait(ctx, substraitPlan)
	if err != nil {
		return err
	}
	prepared.SetParameters(params)

	info, err = prepared.Execute(ctx)
	if err != nil {
		return err
	}

	if err := m.validate(getQueryWithTransactionSchema(), info, client); err != nil {
		return err
	}

	schema, err = prepared.GetSchema(ctx)
	if err != nil {
		return err
	}

	if err := m.validateSchema(getQueryWithTransactionSchema(), schema); err != nil {
		return err
	}

	if err := prepared.Close(ctx); err != nil {
		return err
	}

	prepared, err = txn.Prepare(ctx, "UPDATE PREPARED STATEMENT")
	if err != nil {
		return err
	}

	updated, err = prepared.ExecuteUpdate(ctx)
	if err != nil {
		return err
	}

	if err := assertEq(updatePreparedStatementWithTransactionExpectedRows, updated); err != nil {
		return err
	}

	if err := prepared.Close(ctx); err != nil {
		return err
	}

	prepared, err = txn.PrepareSubstrait(ctx, substraitPlan)
	if err != nil {
		return err
	}

	updated, err = prepared.ExecuteUpdate(ctx)
	if err != nil {
		return err
	}

	if err := assertEq(updatePreparedStatementWithTransactionExpectedRows, updated); err != nil {
		return err
	}

	if err := prepared.Close(ctx); err != nil {
		return err
	}

	if err := txn.RollbackSavepoint(ctx, sp); err != nil {
		return err
	}

	sp2, err := txn.BeginSavepoint(ctx, savepointName)
	if err != nil {
		return err
	}

	if err := assertEq([]byte(savepointID), []byte(sp2)); err != nil {
		return err
	}

	if err := txn.ReleaseSavepoint(ctx, sp); err != nil {
		return err
	}

	if err := txn.Commit(ctx); err != nil {
		return err
	}

	txn, err = client.BeginTransaction(ctx)
	if err != nil {
		return err
	}

	if err := assertEq([]byte(transactionID), []byte(txn.ID())); err != nil {
		return err
	}

	return txn.Rollback(ctx)
}
