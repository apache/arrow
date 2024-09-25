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

package cases

import (
	"context"

	"github.com/apache/arrow/dev/flight-integration/protocol/flight"
	"github.com/apache/arrow/dev/flight-integration/scenario"
	"github.com/apache/arrow/dev/flight-integration/tester"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func init() {
	var (
		headerName    = "x-middleware"
		headerPayload = "expected value"
		successCmd    = "success"
	)

	scenario.Register(
		scenario.Scenario{
			Name: "middleware",
			Steps: []scenario.ScenarioStep{
				{
					Name: "get_flight_info_failure",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						if len(fd.GetCmd()) != 0 {
							return nil, status.Errorf(codes.Internal, "FlightDescriptor.Cmd must not be set")
						}

						propagateHeaderFromClient(ctx, headerName)
						return nil, status.Errorf(codes.Internal, "expected failure")
					}},
				},
				{
					Name: "get_flight_info_success",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						if string(fd.GetCmd()) != successCmd {
							return nil, status.Errorf(codes.Internal, "expected FlightDescriptor.Cmd to be: %s, found: %s", successCmd, string(fd.GetCmd()))
						}

						propagateHeaderFromClient(ctx, headerName)
						return nil, nil
					}},
				},
			},
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {
				{
					var header metadata.MD
					ctx := metadata.AppendToOutgoingContext(ctx, headerName, headerPayload)
					_, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD}, grpc.Header(&header))
					t.Require().Error(err)

					received := header.Get(headerName)
					t.Assert().Len(received, 1)
					t.Assert().Equal(headerPayload, received[0])
				}
				{
					var header metadata.MD
					ctx := metadata.AppendToOutgoingContext(ctx, headerName, headerPayload)
					_, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte(successCmd)}, grpc.Header(&header))
					t.Require().NoError(err)

					received := header.Get(headerName)
					t.Assert().Len(received, 1)
					t.Assert().Equal(headerPayload, received[0])
				}
			},
		},
	)
}

func propagateHeaderFromClient(ctx context.Context, name string) {
	var val string

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		received := md.Get(name)
		if len(received) > 0 {
			val = received[0]
		}
	}
	grpc.SetHeader(ctx, metadata.Pairs(name, val))
}
