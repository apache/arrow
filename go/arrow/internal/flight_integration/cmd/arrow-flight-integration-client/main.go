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

// Client for use with Arrow Flight Integration tests via archery
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v18/arrow/internal/flight_integration"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	host     = flag.String("host", "localhost", "Server host to connect to")
	port     = flag.Int("port", 31337, "Server port to connect to")
	path     = flag.String("path", "", "Resource path to request")
	scenario = flag.String("scenario", "", "Integration test scenario to run")
)

func main() {
	flag.Parse()

	c := flight_integration.GetScenario(*scenario, *path)
	if err := c.RunClient(fmt.Sprintf("%s:%d", *host, *port), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		log.Fatal(err)
	}

}
