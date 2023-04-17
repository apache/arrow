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

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	pb "github.com/apache/arrow/go/v12/arrow/flight/internal/flight"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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
	prep, err := s.cl.Prepare(context.TODO(), "IRRELEVANT")
	s.Nil(prep)
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal(st.Message(), "CreatePreparedStatement not implemented")
}

func TestBaseServer(t *testing.T) {
	suite.Run(t, new(UnimplementedFlightSqlServerSuite))
	suite.Run(t, new(FlightSqlServerSuite))
}
