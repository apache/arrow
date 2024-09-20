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
	"fmt"

	"github.com/apache/arrow/dev/flight-integration/flight"
	"github.com/apache/arrow/dev/flight-integration/scenario"
	"github.com/apache/arrow/dev/flight-integration/tester"
)

func init() {
	scenario.Register(
		scenario.Scenario{
			Name: "app_metadata_flight_info_endpoint",
			Steps: []scenario.ScenarioStep{
				{
					Name: "get_flight_info",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						if fd.GetType() != flight.FlightDescriptor_CMD {
							return nil, fmt.Errorf("expected FlightDescriptor.Type to be CMD, found: %s", fd.GetType())
						}

						return &flight.FlightInfo{
							FlightDescriptor: fd,
							Endpoint:         []*flight.FlightEndpoint{{AppMetadata: fd.Cmd}},
							AppMetadata:      fd.Cmd,
						}, nil
					}}},
			},
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {
				desc := &flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte("foobar")}
				info, err := client.GetFlightInfo(ctx, desc)
				t.Require().NoError(err)

				t.Assert().Equalf(desc.Cmd, info.AppMetadata, "invalid flight info app_metadata: %s, expected: %s", info.AppMetadata, desc.Cmd)
				t.Assert().Lenf(info.Endpoint, 1, "expected exactly 1 flight endpoint, got: %d", len(info.Endpoint))
				t.Assert().Equalf(desc.Cmd, info.Endpoint[0].AppMetadata, "invalid flight endpoint app_metadata: %s, expected: %s", info.Endpoint[0].AppMetadata, desc.Cmd)
			},
		},
	)
}
