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
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/flight"
	"github.com/apache/arrow/go/v8/arrow/internal/arrjson"
	"github.com/apache/arrow/go/v8/arrow/internal/testing/types"
	"github.com/apache/arrow/go/v8/arrow/ipc"
	"github.com/apache/arrow/go/v8/arrow/memory"
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
			return xerrors.Errorf("got fewer batches than expected, received so far: %d, expected: %d", i, len(orig))
		}

		if !array.RecordEqual(chunk, rdr.Record()) {
			return xerrors.Errorf("batch %d doesn't match", i)
		}

		if string(rdr.LatestAppMetadata()) != strconv.Itoa(i) {
			return xerrors.Errorf("expected metadata value: %s, but got: %s", strconv.Itoa(i), string(rdr.LatestAppMetadata()))
		}
	}

	if rdr.Next() {
		return xerrors.Errorf("got more batches than the expected: %d", len(orig))
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
		return xerrors.Errorf("could not open JSON file: %q: %w", s.path, err)
	}

	rdr, err := arrjson.NewReader(r)
	if err != nil {
		return xerrors.Errorf("could not create JSON file reader from file: %q: %w", s.path, err)
	}

	dataSet := integrationDataSet{
		chunks: make([]arrow.Record, 0),
		schema: rdr.Schema(),
	}

	for {
		rec, err := rdr.Read()
		if err != nil {
			if err == io.EOF {
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
			return xerrors.Errorf("expected metadata value: %s, but got nothing.", string(metadata))
		case !bytes.Equal(metadata, acked):
			return xerrors.Errorf("expected metadata value: %s, but got: %s", string(metadata), string(acked))
		}
	}

	wr.Close()

	if err := stream.CloseSend(); err != nil {
		return err
	}

	info, err := client.GetFlightInfo(ctx, descr)
	if err != nil {
		return err
	}

	if len(info.Endpoint) == 0 {
		fmt.Fprintln(os.Stderr, "no endpoints returned from flight server.")
		return xerrors.Errorf("no endpoints returned from flight server")
	}

	for _, ep := range info.Endpoint {
		if len(ep.Location) == 0 {
			return xerrors.Errorf("no locations returned from flight server")
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
			return xerrors.Errorf("got wrong result: expected: %s, got: %s", expected, actual)
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
		return xerrors.Errorf("expected Unauthenticated, got %s", st.Code())
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
		return xerrors.Errorf("expected to receive header 'x-middleware: expected value', but instead got %s", tm.received)
	}

	fmt.Fprintln(os.Stderr, "Headers received successfully on failing call.")
	tm.received = ""
	_, err = client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("success")})
	if err != nil {
		return err
	}

	if tm.received != "expected value" {
		return xerrors.Errorf("expected to receive header 'x-middleware: expected value', but instead got %s", tm.received)
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
