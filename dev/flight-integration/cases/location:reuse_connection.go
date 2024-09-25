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
)

func init() {
	scenario.Register(
		scenario.Scenario{
			Name: "location:reuse_connection",
			Steps: []scenario.ScenarioStep{
				{
					Name: "get_info",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						return &flight.FlightInfo{
							Endpoint: []*flight.FlightEndpoint{{
								Ticket:   &flight.Ticket{Ticket: []byte("reuse")},
								Location: []*flight.Location{{Uri: "arrow-flight-reuse-connection://?"}},
							}},
						}, nil
					}},
				},
			},
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {
				info, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte("reuse")})
				t.Require().NoError(err)

				t.Assert().Len(info.Endpoint, 1, "expected 1 endpoint, got %d", len(info.Endpoint))

				endpoint := info.Endpoint[0]
				t.Assert().Len(endpoint.Location, 1, "expected 1 location, got %d", len(endpoint.Location))
				t.Assert().Equal("arrow-flight-reuse-connection://?", endpoint.Location[0].Uri)
			},
		},
	)
}
