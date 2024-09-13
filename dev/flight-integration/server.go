package integration

import (
	"github.com/apache/arrow/dev/flight-integration/flight"
	"github.com/apache/arrow/dev/flight-integration/scenario"

	"google.golang.org/grpc"
)

func NewIntegrationServer(scenarios ...scenario.Scenario) *grpc.Server {
	server := scenario.NewServer(scenarios)

	srv := grpc.NewServer(grpc.StatsHandler(server))
	flight.RegisterFlightServiceServer(srv, server)

	return srv
}
