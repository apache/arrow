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

package flight_test

import (
	"context"
	"fmt"
	"net"

	"github.com/apache/arrow/go/v14/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func ExampleRegisterFlightServiceServer() {
	s := grpc.NewServer()
	healthSrv := health.NewServer()
	healthgrpc.RegisterHealthServer(s, healthSrv)

	// add methods to this to override the desired methods
	// like DoGet, DoPut, etc.
	server := struct {
		flight.BaseFlightServer
	}{}

	flight.RegisterFlightServiceServer(s, &server)
	healthSrv.SetServingStatus("test", healthgrpc.HealthCheckResponse_SERVING)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	go s.Serve(lis)
	defer s.Stop()

	conn, err := grpc.DialContext(context.Background(), lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	hc := healthgrpc.NewHealthClient(conn)
	rsp, err := hc.Check(context.Background(), &healthgrpc.HealthCheckRequest{Service: "test"})
	if err != nil {
		panic(err)
	}

	fmt.Println(rsp.Status)
	fc := flight.NewClientFromConn(conn, nil)
	if err != nil {
		panic(err)
	}

	// we didn't implement GetFlightInfo so we should get an Unimplemented
	// error, proving it did call into the base flight server. If we didn't
	// register the service, we'd get an error that says "unknown service arrow.flight.protocol.FlightService"
	_, err = fc.GetFlightInfo(context.Background(), &flight.FlightDescriptor{})
	fmt.Println(err)

	// Output:
	// SERVING
	// rpc error: code = Unimplemented desc = method GetFlightInfo not implemented
}
