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
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/apache/arrow/go/v12/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type serverAuth struct{}

func (sa *serverAuth) Authenticate(c flight.AuthConn) error {
	in, err := c.Read()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.Unauthenticated, "no auth info provided")
	}

	if err != nil {
		return status.Error(codes.FailedPrecondition, "error reading auth handshake")
	}

	// do something with in....
	fmt.Println(string(in))

	// send auth token back
	return c.Send([]byte("foobar"))
}

func (sa *serverAuth) IsValid(token string) (interface{}, error) {
	if token == "foobar" {
		return "foo", nil
	}
	return "", status.Error(codes.PermissionDenied, "invalid auth token")
}

func Example_server() {
	server := flight.NewFlightServer()
	server.Init("localhost:0")
	svc := &flight.BaseFlightServer{}
	svc.SetAuthHandler(&serverAuth{})
	server.RegisterFlightService(svc)

	go server.Serve()
	defer server.Shutdown()

	conn, err := grpc.Dial(server.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	stream, err := client.Handshake(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// ignore error handling here for brevity
	stream.Send(&flight.HandshakeRequest{Payload: []byte("baz")})

	resp, _ := stream.Recv()
	fmt.Println(string(resp.Payload))

	// Output:
	// baz
	// foobar
}
