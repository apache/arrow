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

package session_test

import (
	"log"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/flight/session"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Example_defaultMiddleware() {
	// Setup server with default session middleware
	middleware := session.NewServerSessionMiddleware(nil)
	srv := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(middleware),
	})
	srv.RegisterFlightService(flightsql.NewFlightServer(&flightsql.BaseServer{}))
	srv.Init("localhost:0")

	go srv.Serve()
	defer srv.Shutdown()

	// Client will require cookie middleware in order to handle cookie-based server sessions
	client, err := flightsql.NewClient(
		srv.Addr().String(),
		nil,
		[]flight.ClientMiddleware{
			flight.NewClientCookieMiddleware(),
		},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

}

func Example_customStatefulMiddleware() {
	// Generate IDs for new sessions using provided function
	factory := session.NewSessionFactory(uuid.NewString)

	// Create a SessionStore to persist sessions.
	// In-memory store is default; you may provide your own implementation.
	store := session.NewSessionStore()

	// Construct the middleware with the custom manager.
	manager := session.NewStatefulServerSessionManager(session.WithFactory(factory), session.WithStore(store))
	middleware := session.NewServerSessionMiddleware(manager)
	_ = middleware // ... remaining setup is the same as DefaultMiddleware example
}

func Example_statelessMiddleware() {
	// Construct the middleware with the stateless manager.
	manager := session.NewStatelessServerSessionManager()
	middleware := session.NewServerSessionMiddleware(manager)
	_ = middleware // ... remaining setup is the same as DefaultMiddleware example
}
