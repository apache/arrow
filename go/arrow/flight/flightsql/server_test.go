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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v11/arrow/flight"
	"github.com/apache/arrow/go/v11/arrow/flight/flightsql"
	pb "github.com/apache/arrow/go/v11/arrow/flight/internal/flight"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
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
