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

package scenario

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow/dev/flight-integration/protocol/flight"
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
		err = fmt.Errorf(renderResults(r.results))
	}

	return err
}

func renderResults(results map[string]error) string {
	var bldr strings.Builder

	header := "Flight Reference Implementation Client Failure"
	fmt.Fprintln(&bldr, header)
	fmt.Fprintln(&bldr, strings.Repeat("-", len(header)))

	for scenarioName, err := range results {
		fmt.Fprintf(&bldr, "Scenario Name: %s\n", scenarioName)
		fmt.Fprintf(&bldr, "Details:%s", err)
	}

	return bldr.String()
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
