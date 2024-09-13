package scenario_test

import (
	"context"
	"fmt"
	"integration"
	"integration/scenario"
	"net"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/flight/gen/flight"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type ServerSuite struct {
	suite.Suite

	ctx  context.Context
	lis  *bufconn.Listener
	conn *grpc.ClientConn
}

func createConn(lis *bufconn.Listener) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		"passthrough://",
		grpc.WithContextDialer(func(ctx context.Context, str string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func (s *ServerSuite) closeConn() error { return s.conn.Close() }

func (s *ServerSuite) resetConn() error {
	var err error
	if err = s.closeConn(); err != nil {
		return err
	}

	s.conn, err = createConn(s.lis)
	return err
}

func (s *ServerSuite) SetupTest() {
	lis := bufconn.Listen(1024 * 1024)
	conn, err := createConn(lis)
	if err != nil {
		panic(err)
	}

	s.ctx = context.Background()
	s.lis = lis
	s.conn = conn
}

func (s *ServerSuite) TearDownTest() {
	s.ctx = nil
	s.lis = nil
	if err := s.closeConn(); err != nil {
		panic(err)
	}
}

func TestServer(t *testing.T) {
	suite.Run(t, &ServerSuite{})
}

func (s *ServerSuite) TestSingleScenario() {
	srv := integration.NewIntegrationServer(
		scenario.Scenario{
			Name: "mock_scenario",
			Steps: []scenario.ScenarioStep{
				{
					Name: "step_one",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						return &flight.FlightInfo{FlightDescriptor: fd}, nil
					}},
				},
			},
		},
	)

	go srv.Serve(s.lis)
	defer srv.GracefulStop()

	client := flight.NewFlightServiceClient(s.conn)

	desc := flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte("the_command")}
	info, err := client.GetFlightInfo(s.ctx, &desc)
	s.Require().NoError(err)
	s.Assert().True(proto.Equal(&desc, info.FlightDescriptor))
}

func (s *ServerSuite) TestMultipleScenariosNoDisconnect() {
	srv := integration.NewIntegrationServer(
		scenario.Scenario{
			Name: "mock_scenario1",
			Steps: []scenario.ScenarioStep{
				{
					Name: "step_one",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						return &flight.FlightInfo{FlightDescriptor: fd}, nil
					}},
				},
				{
					Name: "step_two",
					ServerHandler: scenario.Handler{GetSchema: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
						return &flight.SchemaResult{Schema: []byte("apparently a schema")}, nil
					}},
				},
			},
		},
		scenario.Scenario{
			Name: "mock_scenario2",
			Steps: []scenario.ScenarioStep{
				{
					Name: "step_one",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						return &flight.FlightInfo{FlightDescriptor: fd}, nil
					}},
				},
			},
		},
	)

	go srv.Serve(s.lis)
	defer srv.GracefulStop()

	client := flight.NewFlightServiceClient(s.conn)

	// mock_scenario1, step_one
	desc := flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte("the_command")}
	info, err := client.GetFlightInfo(s.ctx, &desc)
	s.Require().NoError(err)
	s.Assert().True(proto.Equal(&desc, info.FlightDescriptor))

	// mock_scenario1, step_two
	schema, err := client.GetSchema(s.ctx, &desc)
	s.Require().NoError(err)
	s.Assert().Equal([]byte("apparently a schema"), schema.Schema)

	// all steps in scenario1 have been completed successfully.
	//
	// the client acknowledge completion to the server by disconnecting
	// before continuing to the next scenario.

	// mock_scenario2, step_one
	// expect failure because the same client conn is in-use, signalling the client is still on the same scenario.
	_, err = client.GetFlightInfo(s.ctx, &desc)
	s.Require().ErrorContains(err, "expected previous client to disconnect before starting new scenario")
}

func (s *ServerSuite) TestMultipleScenariosWithDisconnect() {
	srv := integration.NewIntegrationServer(
		scenario.Scenario{
			Name: "mock_scenario1",
			Steps: []scenario.ScenarioStep{
				{
					Name: "step_one",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						return &flight.FlightInfo{FlightDescriptor: fd}, nil
					}},
				},
				{
					Name: "step_two",
					ServerHandler: scenario.Handler{GetSchema: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
						return &flight.SchemaResult{Schema: []byte("apparently a schema")}, nil
					}},
				},
			},
		},
		scenario.Scenario{
			Name: "mock_scenario2",
			Steps: []scenario.ScenarioStep{
				{
					Name: "step_one",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						return &flight.FlightInfo{FlightDescriptor: fd}, nil
					}},
				},
			},
		},
	)

	go srv.Serve(s.lis)
	defer srv.GracefulStop()

	client := flight.NewFlightServiceClient(s.conn)

	// mock_scenario1, step_one
	desc := flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte("the_command")}
	info, err := client.GetFlightInfo(s.ctx, &desc)
	s.Require().NoError(err)
	s.Assert().True(proto.Equal(&desc, info.FlightDescriptor))

	// mock_scenario1, step_two
	schema, err := client.GetSchema(s.ctx, &desc)
	s.Require().NoError(err)
	s.Assert().Equal([]byte("apparently a schema"), schema.Schema)

	// disconnect and reconnect to the server with a new client/conn to advance to the next scenario
	s.Require().NoError(s.resetConn())
	client = flight.NewFlightServiceClient(s.conn)

	// mock_scenario2, step_one
	_, err = client.GetFlightInfo(s.ctx, &desc)
	s.Require().NoError(err)
}

func (s *ServerSuite) TestMultipleScenariosWithError() {
	srv := integration.NewIntegrationServer(
		scenario.Scenario{
			Name: "mock_scenario1",
			Steps: []scenario.ScenarioStep{
				{
					Name: "step_one",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						return nil, fmt.Errorf("gotcha!")
					}},
				},
				{
					Name: "step_two",
					ServerHandler: scenario.Handler{GetSchema: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
						return &flight.SchemaResult{Schema: []byte("apparently a schema")}, nil
					}},
				},
			},
		},
		scenario.Scenario{
			Name: "mock_scenario2",
			Steps: []scenario.ScenarioStep{
				{
					Name: "step_one",
					ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
						return &flight.FlightInfo{FlightDescriptor: fd}, nil
					}},
				},
			},
		},
	)

	go srv.Serve(s.lis)
	defer srv.GracefulStop()

	client := flight.NewFlightServiceClient(s.conn)

	// mock_scenario1, step_one
	desc := flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte("the_command")}
	info, err := client.GetFlightInfo(s.ctx, &desc)
	s.Require().Error(err)
	s.Require().Nil(info)

	// received an error from the server. we expected it here but might not in a real test.
	//
	// if the client-side assertions fail, client should disconnect
	// and expect server to skip to next scenario on reconnect.

	// disconnect and reconnect to the server with a new client/conn to advance to the next scenario
	s.Require().NoError(s.resetConn())
	client = flight.NewFlightServiceClient(s.conn)

	// expect server to skip to mock_scenario2
	_, err = client.GetFlightInfo(s.ctx, &desc)
	s.Require().NoError(err)
}
