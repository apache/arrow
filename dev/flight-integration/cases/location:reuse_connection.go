package cases

import (
	"context"
	"integration/scenario"
	"integration/tester"

	"github.com/apache/arrow/go/v18/arrow/flight/gen/flight"
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
