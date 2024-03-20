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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	pb "github.com/apache/arrow/go/v16/arrow/flight/gen/flight"
	"github.com/apache/arrow/go/v16/arrow/flight/session"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

type testServer struct {
	flightsql.BaseServer
}

func (*testServer) GetFlightInfoStatement(ctx context.Context, q flightsql.StatementQuery, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	ticket, err := flightsql.CreateStatementQueryTicket([]byte(q.GetQuery()))
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		FlightDescriptor: fd,
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticket},
		}},
	}, nil
}

func (*testServer) PollFlightInfo(ctx context.Context, fd *flight.FlightDescriptor) (*flight.PollInfo, error) {
	return &flight.PollInfo{
		Info: &flight.FlightInfo{
			FlightDescriptor: fd,
			Endpoint: []*flight.FlightEndpoint{{
				Ticket: &flight.Ticket{Ticket: []byte{}},
			}, {
				Ticket: &flight.Ticket{Ticket: []byte{}},
			}},
		},
		FlightDescriptor: nil,
	}, nil
}

func (*testServer) PollFlightInfoStatement(ctx context.Context, q flightsql.StatementQuery, fd *flight.FlightDescriptor) (*flight.PollInfo, error) {
	ticket, err := flightsql.CreateStatementQueryTicket([]byte(q.GetQuery()))
	if err != nil {
		return nil, err
	}
	return &flight.PollInfo{
		Info: &flight.FlightInfo{
			FlightDescriptor: fd,
			Endpoint: []*flight.FlightEndpoint{{
				Ticket: &flight.Ticket{Ticket: ticket},
			}},
		},
		FlightDescriptor: &flight.FlightDescriptor{Cmd: []byte{}},
	}, nil
}

func (*testServer) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (sc *arrow.Schema, cc <-chan flight.StreamChunk, err error) {
	handle := string(ticket.GetStatementHandle())
	switch handle {
	case "1":
		b := array.NewInt16Builder(memory.DefaultAllocator)
		sc = arrow.NewSchema([]arrow.Field{{
			Name:     "t1",
			Type:     b.Type(),
			Nullable: true,
		}}, nil)
		b.AppendNull()
		c := make(chan flight.StreamChunk, 2)
		c <- flight.StreamChunk{
			Data: array.NewRecord(sc, []arrow.Array{b.NewArray()}, 1),
		}
		b.Append(1)
		c <- flight.StreamChunk{
			Data: array.NewRecord(sc, []arrow.Array{b.NewArray()}, 1),
		}
		close(c)
		cc = c
	case "2":
		b := array.NewInt16Builder(memory.DefaultAllocator)
		sc = arrow.NewSchema([]arrow.Field{{
			Name:     "t1",
			Type:     b.Type(),
			Nullable: true,
		}}, nil)
		b.Append(2)
		c := make(chan flight.StreamChunk, 2)
		c <- flight.StreamChunk{
			Data: array.NewRecord(sc, []arrow.Array{b.NewArray()}, 1),
		}
		c <- flight.StreamChunk{
			Err: status.Error(codes.Internal, "test error"),
		}
		close(c)
		cc = c
	default:
		err = fmt.Errorf("unknown statement handle: %s", handle)
	}
	return
}

func (*testServer) SetSessionOptions(ctx context.Context, req *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error) {
	session, err := session.GetSessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	errors := make(map[string]*flight.SetSessionOptionsResultError)
	for key, val := range req.GetSessionOptions() {
		if key == "lol_invalid" {
			errors[key] = &flight.SetSessionOptionsResultError{Value: flight.SetSessionOptionsResultErrorInvalidName}
			continue
		}
		if val.GetStringValue() == "lol_invalid" {
			errors[key] = &flight.SetSessionOptionsResultError{Value: flight.SetSessionOptionsResultErrorInvalidValue}
			continue
		}

		session.SetSessionOption(key, val)
	}

	return &flight.SetSessionOptionsResult{Errors: errors}, nil
}

func (*testServer) GetSessionOptions(ctx context.Context, req *flight.GetSessionOptionsRequest) (*flight.GetSessionOptionsResult, error) {
	session, err := session.GetSessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return &flight.GetSessionOptionsResult{SessionOptions: session.GetSessionOptions()}, nil
}

func (*testServer) CloseSession(ctx context.Context, req *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	session, err := session.GetSessionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if err = session.Close(); err != nil {
		return nil, err
	}

	return &flight.CloseSessionResult{Status: flight.CloseSessionResultClosed}, nil
}

type FlightSqlServerSuite struct {
	suite.Suite

	s  flight.Server
	cl *flightsql.Client
}

func (s *FlightSqlServerSuite) SetupSuite() {
	s.s = flight.NewServerWithMiddleware(nil)
	srv := flightsql.NewFlightServer(&testServer{})
	s.s.RegisterFlightService(srv)
	s.s.Init("localhost:0")

	go s.s.Serve()
}

func (s *FlightSqlServerSuite) TearDownSuite() {
	s.s.Shutdown()
}

func (s *FlightSqlServerSuite) SetupTest() {
	cl, err := flightsql.NewClient(s.s.Addr().String(), nil, nil, dialOpts...)
	s.Require().NoError(err)
	s.cl = cl
}

func (s *FlightSqlServerSuite) TearDownTest() {
	s.Require().NoError(s.cl.Close())
	s.cl = nil
}

func (s *FlightSqlServerSuite) TestExecute() {
	fi, err := s.cl.Execute(context.TODO(), "1")
	s.Require().NoError(err)
	ep := fi.GetEndpoint()
	s.Require().Len(ep, 1)
	fr, err := s.cl.DoGet(context.TODO(), ep[0].GetTicket())
	s.Require().NoError(err)
	var recs []arrow.Record
	for fr.Next() {
		rec := fr.Record()
		rec.Retain()
		defer rec.Release()
		recs = append(recs, rec)
	}
	s.Require().NoError(fr.Err())
	tbl := array.NewTableFromRecords(fr.Schema(), recs)
	defer tbl.Release()
	s.Assert().Equal(int64(2), tbl.NumRows())
	s.Assert().Equal(int64(1), tbl.NumCols())
	col := tbl.Column(0)
	s.Assert().Equal("t1", col.Name())
	s.Assert().Equal(2, col.Len())
	s.Assert().Equal(1, col.NullN())
	s.Assert().Equal(arrow.INT16, col.DataType().ID())
	var n int
	for _, arr := range col.Data().Chunks() {
		data := array.NewInt16Data(arr.Data())
		defer data.Release()
		for i := 0; i < data.Len(); i++ {
			switch n {
			case 0:
				s.Assert().Equal(true, data.IsNull(i))
			case 1:
				s.Assert().Equal(false, data.IsNull(i))
				s.Assert().Equal(int16(1), data.Value(i))
			}
			n++
		}
	}
}

func (s *FlightSqlServerSuite) TestExecuteChunkError() {
	fi, err := s.cl.Execute(context.TODO(), "2")
	s.Require().NoError(err)
	ep := fi.GetEndpoint()
	s.Require().Len(ep, 1)
	fr, err := s.cl.DoGet(context.TODO(), ep[0].GetTicket())
	s.Require().NoError(err)
	for fr.Next() {
	}
	err = fr.Err()
	if s.Assert().Error(err) {
		st := status.Convert(err)
		s.Assert().Equal(codes.Internal, st.Code())
		s.Assert().Equal("test error", st.Message())
	}
}

func (s *FlightSqlServerSuite) TestExecutePoll() {
	poll, err := s.cl.ExecutePoll(context.TODO(), "1", nil)
	s.NoError(err)
	s.NotNil(poll)
	s.NotNil(poll.GetFlightDescriptor())
	s.Len(poll.GetInfo().Endpoint, 1)

	poll, err = s.cl.ExecutePoll(context.TODO(), "1", poll.GetFlightDescriptor())
	s.NoError(err)
	s.NotNil(poll)
	s.Nil(poll.GetFlightDescriptor())
	s.Len(poll.GetInfo().Endpoint, 2)
}

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

func (s *UnimplementedFlightSqlServerSuite) TestPoll() {
	poll, err := s.cl.ExecutePoll(context.TODO(), "", nil)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("PollFlightInfoStatement not implemented", st.Message())
	s.Nil(poll)

	poll, err = s.cl.ExecuteSubstraitPoll(context.TODO(), flightsql.SubstraitPlan{}, nil)
	st, ok = status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("PollFlightInfoSubstraitPlan not implemented", st.Message())
	s.Nil(poll)
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
	prep, err := s.cl.Prepare(context.TODO(), "IRRELEVANT")
	s.Nil(prep)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("CreatePreparedStatement not implemented", st.Message())
}

func (s *UnimplementedFlightSqlServerSuite) TestCancelFlightInfo() {
	request := flight.CancelFlightInfoRequest{}
	result, err := s.cl.CancelFlightInfo(context.TODO(), &request)
	s.Nil(result)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("CancelFlightInfo not implemented", st.Message())
}

func (s *UnimplementedFlightSqlServerSuite) TestRenewFlightEndpoint() {
	endpoint := flight.FlightEndpoint{}
	request := flight.RenewFlightEndpointRequest{Endpoint: &endpoint}
	renewedEndpoint, err := s.cl.RenewFlightEndpoint(context.TODO(), &request)
	s.Nil(renewedEndpoint)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("RenewFlightEndpoint not implemented", st.Message())
}

func (s *UnimplementedFlightSqlServerSuite) TestSetSessionOptions() {
	opts, err := flight.NewSessionOptionValues(map[string]any{
		"key": "val",
	})
	s.NoError(err)
	res, err := s.cl.SetSessionOptions(context.TODO(), &flight.SetSessionOptionsRequest{SessionOptions: opts})
	s.Nil(res)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("SetSessionOptions not implemented", st.Message())
}

func (s *UnimplementedFlightSqlServerSuite) TestGetSessionOptions() {
	res, err := s.cl.GetSessionOptions(context.TODO(), &flight.GetSessionOptionsRequest{})
	s.Nil(res)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("GetSessionOptions not implemented", st.Message())
}

func (s *UnimplementedFlightSqlServerSuite) TestCloseSession() {
	res, err := s.cl.CloseSession(context.TODO(), &flight.CloseSessionRequest{})
	s.Nil(res)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("CloseSession not implemented", st.Message())
}

type FlightSqlServerSessionSuite struct {
	suite.Suite

	s  flight.Server
	cl *flightsql.Client

	sessionManager session.ServerSessionManager
}

func (s *FlightSqlServerSessionSuite) SetupSuite() {
	s.s = flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(session.NewServerSessionMiddleware(s.sessionManager)),
	})
	srv := flightsql.NewFlightServer(&testServer{})
	s.s.RegisterFlightService(srv)
	s.s.Init("localhost:0")

	go s.s.Serve()
}

func (s *FlightSqlServerSessionSuite) TearDownSuite() {
	s.s.Shutdown()
}

func (s *FlightSqlServerSessionSuite) SetupTest() {
	middleware := []flight.ClientMiddleware{
		flight.NewClientCookieMiddleware(),
	}
	cl, err := flightsql.NewClient(s.s.Addr().String(), nil, middleware, dialOpts...)
	s.Require().NoError(err)
	s.cl = cl
}

func (s *FlightSqlServerSessionSuite) TearDownTest() {
	s.Require().NoError(s.cl.Close())
	s.cl = nil
}

func (s *FlightSqlServerSessionSuite) TestSetSessionOptions() {
	opts, err := flight.NewSessionOptionValues(map[string]any{
		"foolong":                int64(123),
		"bardouble":              456.0,
		"lol_invalid":            "this won't get set",
		"key_with_invalid_value": "lol_invalid",
		"big_ol_string_list":     []string{"a", "b", "sea", "dee", " ", "  ", "geee", "(づ｡◕‿‿◕｡)づ"},
	})
	s.NoError(err)
	res, err := s.cl.SetSessionOptions(context.TODO(), &flight.SetSessionOptionsRequest{SessionOptions: opts})
	s.NoError(err)
	s.NotNil(res)

	expectedErrs := map[string]*flight.SetSessionOptionsResultError{
		"lol_invalid":            {Value: flight.SetSessionOptionsResultErrorInvalidName},
		"key_with_invalid_value": {Value: flight.SetSessionOptionsResultErrorInvalidValue},
	}

	errs := res.GetErrors()
	s.Equal(len(expectedErrs), len(errs))

	for key, val := range errs {
		s.Equal(expectedErrs[key], val)
	}
}

func (s *FlightSqlServerSessionSuite) TestGetSetGetSessionOptions() {
	ctx := context.TODO()
	getRes, err := s.cl.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{})
	s.NoError(err)
	s.NotNil(getRes)
	s.Len(getRes.SessionOptions, 0)

	expectedOpts := map[string]any{
		"foolong":            int64(123),
		"bardouble":          456.0,
		"big_ol_string_list": []string{"a", "b", "sea", "dee", " ", "  ", "geee", "(づ｡◕‿‿◕｡)づ"},
	}

	optionVals, err := flight.NewSessionOptionValues(expectedOpts)
	s.NoError(err)
	s.NotNil(optionVals)

	setRes, err := s.cl.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: optionVals})
	s.NoError(err)
	s.NotNil(setRes)
	s.Empty(setRes.Errors)

	getRes2, err := s.cl.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{})
	s.NoError(err)
	s.NotNil(getRes2)

	opts := getRes2.GetSessionOptions()
	s.Equal(3, len(opts))

	s.Equal(expectedOpts["foolong"], opts["foolong"].GetInt64Value())
	s.Equal(expectedOpts["bardouble"], opts["bardouble"].GetDoubleValue())
	s.Equal(expectedOpts["big_ol_string_list"], opts["big_ol_string_list"].GetStringListValue().GetValues())
}

func (s *FlightSqlServerSessionSuite) TestSetRemoveSessionOptions() {
	ctx := context.TODO()
	initialOpts := map[string]any{
		"foolong":            int64(123),
		"bardouble":          456.0,
		"big_ol_string_list": []string{"a", "b", "sea", "dee", " ", "  ", "geee", "(づ｡◕‿‿◕｡)づ"},
	}

	optionVals, err := flight.NewSessionOptionValues(initialOpts)
	s.NoError(err)
	s.NotNil(optionVals)

	setRes, err := s.cl.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: optionVals})
	s.NoError(err)
	s.NotNil(setRes)
	s.Empty(setRes.Errors)

	removeKeyOpts, err := flight.NewSessionOptionValues(map[string]any{
		"foolong": nil,
	})
	s.NoError(err)
	s.NotNil(removeKeyOpts)

	setRes2, err := s.cl.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: removeKeyOpts})
	s.NoError(err)
	s.NotNil(setRes2)
	s.Empty(setRes2.Errors)

	getRes, err := s.cl.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{})
	s.NoError(err)
	s.NotNil(getRes)

	opts := getRes.GetSessionOptions()
	s.Equal(2, len(opts))

	s.Equal(initialOpts["bardouble"], opts["bardouble"].GetDoubleValue())
	s.Equal(initialOpts["big_ol_string_list"], opts["big_ol_string_list"].GetStringListValue().GetValues())
}

func (s *FlightSqlServerSessionSuite) TestCloseSession() {
	ctx := context.TODO()
	initialOpts := map[string]any{
		"foolong":            int64(123),
		"bardouble":          456.0,
		"big_ol_string_list": []string{"a", "b", "sea", "dee", " ", "  ", "geee", "(づ｡◕‿‿◕｡)づ"},
	}

	optionVals, err := flight.NewSessionOptionValues(initialOpts)
	s.NoError(err)
	s.NotNil(optionVals)

	setRes, err := s.cl.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: optionVals})
	s.NoError(err)
	s.NotNil(setRes)
	s.Empty(setRes.Errors)

	closeRes, err := s.cl.CloseSession(ctx, &flight.CloseSessionRequest{})
	s.NoError(err)
	s.NotNil(closeRes)
	s.Equal(flight.CloseSessionResultClosed, closeRes.GetStatus())

	getRes, err := s.cl.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{})
	s.NoError(err)
	s.NotNil(getRes)

	opts := getRes.GetSessionOptions()
	s.Empty(opts)
}

func TestBaseServer(t *testing.T) {
	suite.Run(t, new(UnimplementedFlightSqlServerSuite))
	suite.Run(t, new(FlightSqlServerSuite))
	suite.Run(t, &FlightSqlServerSessionSuite{sessionManager: session.NewStatefulServerSessionManager()})
	suite.Run(t, &FlightSqlServerSessionSuite{sessionManager: session.NewStatelessServerSessionManager()})
}

func TestStatefulServerSessionCookies(t *testing.T) {
	// Generate session IDs deterministically
	sessionIDGenerator := func(ids []string) func() string {
		ch := make(chan string, len(ids))
		for _, id := range ids {
			ch <- id
		}
		close(ch)

		return func() string {
			return <-ch
		}
	}

	factory := session.NewSessionFactory(sessionIDGenerator([]string{"how-now-brown-cow", "unique-new-york"}))
	store := session.NewSessionStore()
	manager := session.NewStatefulServerSessionManager(session.WithFactory(factory), session.WithStore(store))
	middleware := session.NewServerSessionMiddleware(manager)

	srv := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(middleware),
	})
	srv.RegisterFlightService(flightsql.NewFlightServer(&testServer{}))
	srv.Init("localhost:0")

	go srv.Serve()
	defer srv.Shutdown()

	client, err := flightsql.NewClient(
		srv.Addr().String(),
		nil,
		[]flight.ClientMiddleware{
			flight.NewClientCookieMiddleware(),
		},
		dialOpts...,
	)
	require.NoError(t, err)
	defer client.Close()

	var (
		trailer metadata.MD
		session session.ServerSession
	)

	ctx := context.TODO()

	// Get empty session; should create new session since one doesn't exist
	_, err = client.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// Client should recieve cookie with new session ID
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, "arrow_flight_session_id=how-now-brown-cow", trailer.Get("set-cookie")[0])

	// Server should add the empty session to its internal store
	session, err = store.Get("how-now-brown-cow")
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Empty(t, session.GetSessionOptions())

	optionVals, err := flight.NewSessionOptionValues(map[string]any{"hello": "world"})
	require.NoError(t, err)
	require.NotNil(t, optionVals)

	// Add option to existing session
	_, err = client.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: optionVals}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// Server received and used session from existing client cookie, no need to set a new one
	require.Len(t, trailer.Get("set-cookie"), 0)

	// The option we set has been added to the server's state
	session, err = store.Get("how-now-brown-cow")
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Len(t, session.GetSessionOptions(), 1)
	require.Contains(t, session.GetSessionOptions(), "hello")

	// Close the existing session
	_, err = client.CloseSession(ctx, &flight.CloseSessionRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// Inform the client that the cookie should be deleted
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, "arrow_flight_session_id=how-now-brown-cow; Max-Age=0", trailer.Get("set-cookie")[0])

	// The session has been removed from the server's internal store
	session, err = store.Get("how-now-brown-cow")
	require.Error(t, err)
	require.Nil(t, session)

	// Get the session; this should create a new session because we just closed the previous one
	_, err = client.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// The client is informed to set a NEW cookie for the newly created session
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, "arrow_flight_session_id=unique-new-york", trailer.Get("set-cookie")[0])

	// The new empty session has been added to the server's internal store
	session, err = store.Get("unique-new-york")
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Empty(t, session.GetSessionOptions())

	// Close the new session
	_, err = client.CloseSession(ctx, &flight.CloseSessionRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// Inform the client that the new session's cookie should be deleted
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, "arrow_flight_session_id=unique-new-york; Max-Age=0", trailer.Get("set-cookie")[0])

	// The session has been removed from the server's internal store
	session, err = store.Get("unique-new-york")
	require.Error(t, err)
	require.Nil(t, session)
}

func TestStatelessServerSessionCookies(t *testing.T) {
	manager := session.NewStatelessServerSessionManager()
	middleware := session.NewServerSessionMiddleware(manager)

	srv := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(middleware),
	})
	srv.RegisterFlightService(flightsql.NewFlightServer(&testServer{}))
	srv.Init("localhost:0")

	go srv.Serve()
	defer srv.Shutdown()

	client, err := flightsql.NewClient(
		srv.Addr().String(),
		nil,
		[]flight.ClientMiddleware{
			flight.NewClientCookieMiddleware(),
		},
		dialOpts...,
	)
	require.NoError(t, err)
	defer client.Close()

	var trailer metadata.MD

	ctx := context.TODO()

	// Get empty session; should create new session since one doesn't exist
	_, err = client.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// Client should recieve cookie with new session token. An empty session is serialized with zero bytes.
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, "arrow_flight_session=", trailer.Get("set-cookie")[0])

	optionVals, err := flight.NewSessionOptionValues(map[string]any{"hello": "world"})
	require.NoError(t, err)
	require.NotNil(t, optionVals)

	// Add option to existing session
	_, err = client.SetSessionOptions(ctx, &flight.SetSessionOptionsRequest{SessionOptions: optionVals}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// Session state has been modified, so we send a new cookie with the updated session contents
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, `arrow_flight_session=ChAKBWhlbGxvEgcKBXdvcmxk`, trailer.Get("set-cookie")[0]) // base64 of binary '{"hello":"world"}' proto message

	// Close the existing session
	_, err = client.CloseSession(ctx, &flight.CloseSessionRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// Inform the client that the cookie should be deleted
	//
	// The cookie is in the gRPC trailer because the session may have been closed AFTER the initial headers were sent
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, "arrow_flight_session=ChAKBWhlbGxvEgcKBXdvcmxk; Max-Age=0", trailer.Get("set-cookie")[0])

	// Get the session; his should create a new session because we just closed the previous one
	// Realistically no session is "created", this just happens because the client was told to drop the cookie
	// in the last step.
	_, err = client.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// The client is informed to set a NEW cookie for the newly created empty session
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, "arrow_flight_session=", trailer.Get("set-cookie")[0])

	// Close the new session
	_, err = client.CloseSession(ctx, &flight.CloseSessionRequest{}, grpc.Trailer(&trailer))
	require.NoError(t, err)

	// Inform the client that the new session's cookie should be deleted
	require.Len(t, trailer.Get("set-cookie"), 1)
	require.Equal(t, "arrow_flight_session=; Max-Age=0", trailer.Get("set-cookie")[0])
}
