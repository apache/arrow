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
	"net"
	"strings"

	integration "github.com/apache/arrow/dev/flight-integration"

	_ "github.com/apache/arrow/dev/flight-integration/cases"
	"github.com/apache/arrow/dev/flight-integration/scenario"
)

var (
	port         = flag.Int("port", 31337, "Server port to listen on")
	scenariosStr = flag.String("scenarios", "", "Comma-delimited scenarios to run")
)

func main() {
	flag.Parse()
	scenarioNames := strings.Split(*scenariosStr, ",")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen on port %d: %v", *port, err)
	}

	scenarios, err := scenario.GetScenarios(scenarioNames...)
	if err != nil {
		log.Fatal(err)
	}

	srv := integration.NewIntegrationServer(scenarios...)

	log.Printf("server listening at %v", lis.Addr())
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
