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

package integration_test

import (
	"context"
	"net"
	"testing"

	integration "github.com/apache/arrow/dev/flight-integration"
	"github.com/apache/arrow/dev/flight-integration/scenario"

	"github.com/stretchr/testify/assert"
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

	srv, shutdown := integration.NewIntegrationServer(scenarios...)
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
	assert.NoError(t, runner.RunScenarios(dialer))
	assert.NoError(t, shutdown())
}
