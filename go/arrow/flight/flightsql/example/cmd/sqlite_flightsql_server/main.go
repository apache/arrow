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

//go:build go1.18
// +build go1.18

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql/example"
)

func main() {
	var (
		host = flag.String("host", "localhost", "hostname to bind to")
		port = flag.Int("port", 0, "port to bind to")
	)

	flag.Parse()

	db, err := example.CreateDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	srv, err := example.NewSQLiteFlightSQLServer(db)
	if err != nil {
		log.Fatal(err)
	}

	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(srv))
	server.Init(net.JoinHostPort(*host, strconv.Itoa(*port)))
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)

	fmt.Println("Starting SQLite Flight SQL Server on", server.Addr(), "...")

	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
