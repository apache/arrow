package integration

import (
	"integration/scenario"

	"github.com/apache/arrow/go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc"
)

func NewIntegrationServer(scenarios ...scenario.Scenario) *grpc.Server {
	server := scenario.NewServer(scenarios)

	srv := grpc.NewServer(grpc.StatsHandler(server))
	flight.RegisterFlightServiceServer(srv, server)

	return srv
}
