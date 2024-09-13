package scenario

import (
	"context"
	"errors"
	"fmt"
	"integration/tester"

	"github.com/apache/arrow/go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc"
)

func NewScenarioRunner(scenarios []Scenario) *ScenarioRunner {
	return &ScenarioRunner{scenarios: scenarios, results: make(map[string]error, len(scenarios))}
}

type ScenarioRunner struct {
	scenarios []Scenario

	results map[string]error
}

func (r *ScenarioRunner) RunScenario(scenario Scenario, newConnFn func() (conn *grpc.ClientConn, err error)) (err error) {
	conn, err := newConnFn()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t := tester.NewTester()
	defer func() {
		err = errors.Join(t.Errors()...)
		r := recover()
		if r == tester.RequirementFailedMsg {
			return
		}
		if r != nil {
			panic(r)
		}
	}()

	client := flight.NewFlightServiceClient(conn)
	scenario.RunClient(ctx, client, t)
	return
}

func (r *ScenarioRunner) RunScenarios(newConnFn func() (conn *grpc.ClientConn, err error)) error {
	var err error
	for _, scenario := range r.scenarios {
		if err := r.RunScenario(scenario, newConnFn); err != nil {
			r.results[scenario.Name] = err
		}
	}

	if len(r.results) > 0 {
		err = fmt.Errorf("client-side errors:\n%v", r.results)
	}

	return err
}
