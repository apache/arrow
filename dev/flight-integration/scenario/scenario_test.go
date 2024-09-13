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

package scenario_test

import (
	"context"
	"testing"

	"github.com/apache/arrow/dev/flight-integration/flight"
	"github.com/apache/arrow/dev/flight-integration/scenario"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterScenarios(t *testing.T) {
	scenario.Register(
		scenario.Scenario{
			Name: "mock_scenario1",
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
	defer scenario.Unregister("mock_scenario1")

	scenario.Register(
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
	defer scenario.Unregister("mock_scenario2")

	scenarios, err := scenario.GetScenarios("mock_scenario1")
	require.NoError(t, err)
	assert.Len(t, scenarios, 1)

	scenarios, err = scenario.GetScenarios("mock_scenario2")
	require.NoError(t, err)
	assert.Len(t, scenarios, 1)

	scenarios, err = scenario.GetScenarios("mock_scenario1", "mock_scenario2")
	require.NoError(t, err)
	assert.Len(t, scenarios, 2)

	scenarios, err = scenario.GetScenarios()
	require.NoError(t, err)
	assert.Len(t, scenarios, 2)

	_, err = scenario.GetScenarios("fake_scenario")
	require.Error(t, err)
}
