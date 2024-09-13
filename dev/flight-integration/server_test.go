package integration_test

import (
	"context"
	"net"
	"testing"

	integration "github.com/apache/arrow/dev/flight-integration"
	"github.com/apache/arrow/dev/flight-integration/scenario"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	_ "github.com/apache/arrow/dev/flight-integration/cases"
)

func TestIntegrationClientAndServer(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)

	scenarios, err := scenario.GetScenarios()
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

	runner := scenario.NewRunner(scenarios)
	require.NoError(t, runner.RunScenarios(dialer))
}
