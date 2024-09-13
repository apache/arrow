package integration_test

import (
	"context"
	"integration"
	"integration/scenario"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	_ "integration/cases"
)

func TestIntegrationClientAndServer(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)

	scenarios, err := scenario.GetAllScenarios()
	require.NoError(t, err)

	srv := integration.NewIntegrationServer(scenarios...)
	require.NoError(t, err)

	go srv.Serve(lis)
	defer srv.GracefulStop()

	dialer := func() (conn *grpc.ClientConn, err error) {
		return grpc.NewClient(
			"passthrough://",
			grpc.WithContextDialer(func(ctx context.Context, str string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}

	runner := scenario.NewScenarioRunner(scenarios)
	require.NoError(t, runner.RunScenarios(dialer))
}
