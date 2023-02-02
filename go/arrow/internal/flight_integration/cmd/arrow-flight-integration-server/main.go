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
	"net"
	"os"
	"syscall"

	"github.com/apache/arrow/go/v12/arrow/internal/flight_integration"
)

var (
	port     = flag.Int("port", 31337, "Server port to listen on")
	scenario = flag.String("scenario", "", "Integration test scenario to run")
)

func main() {
	flag.Parse()

	s := flight_integration.GetScenario(*scenario)
	srv := s.MakeServer(*port)
	srv.SetShutdownOnSignals(syscall.SIGTERM, os.Interrupt)
	_, p, _ := net.SplitHostPort(srv.Addr().String())
	fmt.Printf("Server listening on localhost:%s\n", p)
	srv.Serve()
}
