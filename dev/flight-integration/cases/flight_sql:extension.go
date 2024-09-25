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
			Name: "flight_sql:extension",
			Steps: []scenario.ScenarioStep{
				{Name: "get_flight_info_sqlinfo", ServerHandler: scenario.Handler{GetFlightInfo: echoFlightInfo}},
				// {
				// 	Name: "do_get_sqlinfo",
				// 	ServerHandler: scenario.Handler{DoGet: func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
				// 		var cmd flight.CommandGetSqlInfo
				// 		if err := deserializeProtobufWrappedInAny(t.Ticket, &cmd); err != nil {
				// 			return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
				// 		}

				// 		return fs.Send(&flight.FlightData{DataHeader: buildFlatbufferSchema(fields)})
				// 	}},
				// },
			},
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {
				_, err := client.GetFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte("something")})
				t.Require().NoError(err)
			},
		},
	)
}
