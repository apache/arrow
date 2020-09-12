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
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const grpcAuthHeader = "auth-token-bin"

// AuthConn wraps the stream from grpc for handshakes to simplify handling
// handshake request and response from the flight.proto forwarding just the
// payloads and errors instead of having to deal with the handshake request
// and response protos directly
type AuthConn interface {
	Read() ([]byte, error)
	Send([]byte) error
}

type serverAuthConn struct {
	stream FlightService_HandshakeServer
}

func (a *serverAuthConn) Read() ([]byte, error) {
	in, err := a.stream.Recv()
	if err != nil {
		return nil, err
	}

	return in.Payload, nil
}

func (a *serverAuthConn) Send(b []byte) error {
	return a.stream.Send(&HandshakeResponse{Payload: b})
}

// ServerAuthHandler defines an interface for the server to perform the handshake.
// The token is expected to be sent as part of the context metadata in subsequent
// requests with a key of "auth-token-bin" which will then call IsValid to validate
//
// TODO: implement returning identifying information with the token
type ServerAuthHandler interface {
	Authenticate(AuthConn) error
	IsValid(token string) error
}

func createServerAuthUnaryInterceptor(auth ServerAuthHandler) grpc.UnaryServerInterceptor {
	if auth == nil {
		return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			return handler(ctx, req)
		}
	}

	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var authTok string
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			vals := md.Get(grpcAuthHeader)
			if len(vals) > 0 {
				authTok = vals[0]
			}
		}

		if err := auth.IsValid(authTok); err != nil {
			return nil, status.Errorf(codes.PermissionDenied, "auth-error: %s", err)
		}

		return handler(ctx, req)
	}
}

func createServerAuthStreamInterceptor(auth ServerAuthHandler) grpc.StreamServerInterceptor {
	if auth == nil {
		return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, stream)
		}
	}

	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasSuffix(info.FullMethod, "/Handshake") {
			return handler(srv, stream)
		}

		var authTok string
		md, ok := metadata.FromIncomingContext(stream.Context())
		if ok {
			vals := md.Get(grpcAuthHeader)
			if len(vals) > 0 {
				authTok = vals[0]
			}
		}

		if err := auth.IsValid(authTok); err != nil {
			return status.Errorf(codes.Unauthenticated, "auth-error: %s", err)
		}

		return handler(srv, stream)
	}
}

// our implementation of handshake using the authhandler
func (s *server) handshake(stream FlightService_HandshakeServer) error {
	if s.authHandler == nil {
		return nil
	}

	return s.authHandler.Authenticate(&serverAuthConn{stream})
}
