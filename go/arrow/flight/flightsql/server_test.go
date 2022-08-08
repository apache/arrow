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
	"testing"

	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

func (s *UnimplementedFlightSqlServerSuite) TestExecute() {
	info, err := s.cl.Execute(context.TODO(), "SELECT * FROM IRRELEVANT")
	st, ok := status.FromError(err)
	s.True(ok)
	s.Equal(codes.Unimplemented, st.Code())
	s.Equal("GetFlightInfoStatement not implemented", st.Message())
	s.Nil(info)
}

func TestBaseServer(t *testing.T) {
	suite.Run(t, new(UnimplementedFlightSqlServerSuite))
}
