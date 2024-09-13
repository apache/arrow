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

package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	_ "github.com/apache/arrow/dev/flight-integration/cases"
	"github.com/apache/arrow/dev/flight-integration/scenario"
)

var (
	host         = flag.String("host", "localhost", "Server host to connect to")
	port         = flag.Int("port", 31337, "Server port to connect to")
	scenariosStr = flag.String("scenarios", "", "Comma-delimited scenarios to run")
)

func main() {
	flag.Parse()
	addr := fmt.Sprintf("%s:%d", *host, *port)
	scenarioNames := strings.Split(*scenariosStr, ",")

	scenarios, err := scenario.GetScenarios(scenarioNames...)
	if err != nil {
		log.Fatal(err)
	}

	dialServer := func() (conn *grpc.ClientConn, err error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	runner := scenario.NewRunner(scenarios)
	if err := runner.RunScenarios(dialServer); err != nil {
		log.Fatal(err)
	}
}
