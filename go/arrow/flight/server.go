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
	context "context"
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

type CustomServerMiddleware interface {
	// StartCall will be called with the current context of the call, grpc.SetHeader can be used to add outgoing headers
	// if the returned context is non-nil, then it will be used as the new context being passed through the calls
	StartCall(ctx context.Context) context.Context
	// CallCompleted is a callback which is called with the return from the handler
	// it will be nil if everything was successful or will be the error about to be returned
	// to grpc
	CallCompleted(ctx context.Context, err error)
}

func CreateServerMiddleware(middleware CustomServerMiddleware) ServerMiddleware {
	return ServerMiddleware{
		Unary: func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (ret interface{}, err error) {
			nctx := middleware.StartCall(ctx)
			if nctx != nil {
				ctx = nctx
			}

			ret, err = handler(ctx, req)
			middleware.CallCompleted(ctx, err)
			return
		},
		Stream: func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx := middleware.StartCall(stream.Context())
			if ctx != nil {
				stream = &wrappedStream{ServerStream: stream, ctx: ctx}
			}

			err := handler(srv, stream)
			middleware.CallCompleted(stream.Context(), err)
			return err
		},
	}
}

type ServerMiddleware struct {
	Stream grpc.StreamServerInterceptor
	Unary  grpc.UnaryServerInterceptor
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
//
// Deprecated: prefer to use NewServerWithMiddleware
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

// NewServerWithMiddleware takes a slice of middleware which will be used
// by grpc and chained, the first middleware will be the outer most with the last
// middleware being the inner most wrapper around the actual call. It also takes
// any grpc Server options desired, such as TLS certs and so on which will just
// be passed through to the underlying grpc server.
//
// Alternatively, a grpc server can be created normally without this helper as the
// grpc server generated code is still being exported. This only exists to allow
// the utility of the helpers
func NewServerWithMiddleware(auth ServerAuthHandler, middleware []ServerMiddleware, opts ...grpc.ServerOption) Server {
	unary := make([]grpc.UnaryServerInterceptor, 0, len(middleware))
	stream := make([]grpc.StreamServerInterceptor, 0, len(middleware))
	if auth != nil {
		unary = append(unary, createServerAuthUnaryInterceptor(auth))
		stream = append(stream, createServerAuthStreamInterceptor(auth))
	}

	if len(middleware) > 0 {
		for _, m := range middleware {
			if m.Unary != nil {
				unary = append(unary, m.Unary)
			}
			if m.Stream != nil {
				stream = append(stream, m.Stream)
			}
		}
	}
	opts = append(opts, grpc.ChainUnaryInterceptor(unary...), grpc.ChainStreamInterceptor(stream...))

	return &server{server: grpc.NewServer(opts...), authHandler: auth}
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
