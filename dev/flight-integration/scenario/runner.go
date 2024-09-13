package scenario

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/dev/flight-integration/flight"
	"github.com/apache/arrow/dev/flight-integration/tester"

	"google.golang.org/grpc"
)

func NewRunner(scenarios []Scenario) *ScenarioRunner {
	return &ScenarioRunner{scenarios: scenarios, results: make(map[string]error, len(scenarios))}
}

type ScenarioRunner struct {
	scenarios []Scenario

	results map[string]error
}

func (r *ScenarioRunner) runScenario(scenario Scenario, newConnFn func() (conn *grpc.ClientConn, err error)) (err error) {
	conn, err := newConnFn()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t := tester.NewTester()
	defer func() {
		err = processErrors(t.Errors())
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
		if err := r.runScenario(scenario, newConnFn); err != nil {
			r.results[scenario.Name] = err
		}
	}

	if len(r.results) > 0 {
		err = fmt.Errorf("client-side errors:\n%v", r.results)
	}

	return err
}

func processErrors(errs []error) error {
	res := make([]error, len(errs))
	for i, err := range errs {
		switch {
		case strings.Contains(err.Error(), msgExpectedClientDisconnect):
			res[i] = fmt.Errorf("client made too many RPC calls, server closed the scenario before the client was done")
		default:
			res[i] = err
		}
	}
	return errors.Join(res...)
}
