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

package flight

import (
	"net"
	"os"
	"os/signal"

	"google.golang.org/grpc"
)

// Server is an interface for hiding some of the grpc specifics to make
// it slightly easier to manage a flight service, slightly modeled after
// the C++ implementation
type Server interface {
	// Init takes in the address to bind to and creates the listener
	Init(addr string) error
	// Addr will return the address that was bound to for the service to listen on
	Addr() net.Addr
	// SetShutdownOnSignals sets notifications on the given signals to call GracefulStop
	// on the grpc service if any of those signals are received
	SetShutdownOnSignals(sig ...os.Signal)
	// Serve blocks until accepting a connection fails with a fatal error. It will return
	// a non-nil error unless it stopped due to calling Shutdown or receiving one of the
	// signals set in SetShutdownOnSignals
	Serve() error
	// Shutdown will call GracefulStop on the grpc server so that it stops accepting connections
	// and will wait until current methods complete
	Shutdown()
	// RegisterFlightService sets up the handler for the Flight Endpoints as per
	// normal Grpc setups
	RegisterFlightService(*FlightServiceService)
}

type server struct {
	lis        net.Listener
	sigChannel <-chan os.Signal
	done       chan bool

	authHandler ServerAuthHandler
	server      *grpc.Server
}

// NewFlightServer takes in an auth handler for managing the handshake authentication
// and any grpc Server options desired, such as TLS certs and so on which will just
// be passed through to the underlying grpc server.
//
// Alternatively, a grpc server can be created normally without this helper as the
// grpc server generated code is still being exported. This only exists to allow
// the utility of the helpers
func NewFlightServer(auth ServerAuthHandler, opt ...grpc.ServerOption) Server {
	if auth != nil {
		opt = append([]grpc.ServerOption{
			grpc.ChainStreamInterceptor(createServerAuthStreamInterceptor(auth)),
			grpc.ChainUnaryInterceptor(createServerAuthUnaryInterceptor(auth)),
		}, opt...)
	}

	return &server{
		authHandler: auth,
		server:      grpc.NewServer(opt...),
	}
}

func (s *server) Init(addr string) (err error) {
	s.lis, err = net.Listen("tcp", addr)
	return
}

func (s *server) Addr() net.Addr {
	return s.lis.Addr()
}

func (s *server) SetShutdownOnSignals(sig ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, sig...)
	s.sigChannel = c
}

func (s *server) Serve() error {
	s.done = make(chan bool)
	go func() {
		select {
		case <-s.sigChannel:
			s.server.GracefulStop()
		case <-s.done:
		}
	}()
	err := s.server.Serve(s.lis)
	close(s.done)
	return err
}

func (s *server) RegisterFlightService(svc *FlightServiceService) {
	if svc.Handshake == nil {
		svc.Handshake = s.handshake
	}
	RegisterFlightServiceService(s.server, svc)
}

func (s *server) Shutdown() {
	s.server.GracefulStop()
}
