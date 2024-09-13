package scenario_test

import (
	"context"
	"testing"

	"github.com/apache/arrow/dev/flight-integration/scenario"

	"github.com/apache/arrow/go/v18/arrow/flight/gen/flight"
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
