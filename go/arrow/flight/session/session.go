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

// Package session provides server middleware and reference implementations for Flight session management.
//
// For more details on the Flight Session Specification, see:
// https://arrow.apache.org/docs/format/FlightSql.html#flight-server-session-management
//
// [NewServerSessionMiddleware] manages sessions using cookies, so any client would need its own
// middleware/support for storing and sending those cookies. The cookies may be stateful or stateless:
//
//   - [NewStatefulServerSessionManager] implements stateful cookies.
//
//   - [NewStatelessServerSessionManager] implements stateless cookies.
//
// See details of either implementation for caveats and recommended usage scenarios.
package session

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var ErrNoSession error = errors.New("flight: server session not present")

type sessionMiddlewareKey struct{}

// NewSessionContex returns a copy of the provided context containing the provided ServerSession
func NewSessionContext(ctx context.Context, session ServerSession) context.Context {
	return context.WithValue(ctx, sessionMiddlewareKey{}, session)
}

// GetSessionFromContext retrieves the ServerSession from the provided context if it exists.
// An error indicates that the session was not found in the context.
func GetSessionFromContext(ctx context.Context) (ServerSession, error) {
	session, ok := ctx.Value(sessionMiddlewareKey{}).(ServerSession)
	if !ok {
		return nil, ErrNoSession
	}
	return session, nil
}

// ServerSession is a container for named SessionOptionValues
type ServerSession interface {
	// An identifier for the session that the server can use to reconstruct
	// the session state on future requests. It is the responsibility of
	// each implementation to define the token's semantics.
	Token() string
	// Get session option value by name, or nil if it does not exist
	GetSessionOption(name string) *flight.SessionOptionValue
	// Get a copy of the session options
	GetSessionOptions() map[string]*flight.SessionOptionValue
	// Set session option by name to given value
	SetSessionOption(name string, value *flight.SessionOptionValue)
	// Idempotently remove name from this session
	EraseSessionOption(name string)
	// Close the session
	Close() error
	// Report whether the session has been closed
	Closed() bool
}

// ServerSessionManager handles session lifecycle management
type ServerSessionManager interface {
	// Create a new, empty ServerSession
	CreateSession(ctx context.Context) (ServerSession, error)
	// Get the current ServerSession, if one exists
	GetSession(ctx context.Context) (ServerSession, error)
	// Cleanup any resources associated with the current ServerSession
	CloseSession(session ServerSession) error
}

// Implementation of common session behavior. Intended to be extended
// by specific session implementations.
type serverSession struct {
	closed bool

	options map[string]*flight.SessionOptionValue
	mu      sync.RWMutex
}

func (session *serverSession) GetSessionOption(name string) *flight.SessionOptionValue {
	session.mu.RLock()
	defer session.mu.RUnlock()
	value, found := session.options[name]
	if !found {
		return nil
	}

	return value
}

func (session *serverSession) GetSessionOptions() map[string]*flight.SessionOptionValue {
	options := make(map[string]*flight.SessionOptionValue, len(session.options))

	session.mu.RLock()
	defer session.mu.RUnlock()
	for k, v := range session.options {
		options[k] = proto.Clone(v).(*flight.SessionOptionValue)
	}

	return options
}

func (session *serverSession) SetSessionOption(name string, value *flight.SessionOptionValue) {
	if value.GetOptionValue() == nil {
		session.EraseSessionOption(name)
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	session.options[name] = value
}

func (session *serverSession) EraseSessionOption(name string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	delete(session.options, name)
}

func (session *serverSession) Close() error {
	session.options = nil
	session.closed = true
	return nil
}

func (session *serverSession) Closed() bool {
	return session.closed
}

// NewServerSessionMiddleware creates new instance of CustomServerMiddleware implementing server session persistence.
//
// The provided manager can be used to customize session implementation/behavior.
// If no manager is provided, a stateful in-memory, goroutine-safe implementation is used.
func NewServerSessionMiddleware(manager ServerSessionManager) *serverSessionMiddleware {
	// Default manager
	if manager == nil {
		manager = NewStatefulServerSessionManager()
	}
	return &serverSessionMiddleware{manager: manager}
}

type serverSessionMiddleware struct {
	manager ServerSessionManager
}

// Get the existing session if one is found, otherwise create one. The resulting context will contain
// the session at a well-known key for any internal RPC methods to read/update.
func (middleware *serverSessionMiddleware) StartCall(ctx context.Context) context.Context {
	session, err := middleware.manager.GetSession(ctx)
	if err == nil {
		return NewSessionContext(ctx, session)
	}

	if err != ErrNoSession {
		panic(err)
	}

	session, err = middleware.manager.CreateSession(ctx)
	if err != nil {
		panic(err)
	}

	// TODO(joellubi): Remove this once Java clients support receiving cookies in gRPC trailer.
	// Currently, both C++ and Go client cookie middlewares merge the header and trailer when setting cookies.
	// Java middleware checks the metadata in the header, but only reads the trailer when there is an error.
	// It is far simpler to only set cookies in the trailer, especially for streaming RPC.
	sessionCookie, err := CreateCookieForSession(session)
	if err != nil {
		panic(err)
	}
	grpc.SetHeader(ctx, metadata.Pairs("Set-Cookie", sessionCookie.String()))

	return NewSessionContext(ctx, session)
}

// Determine if the session state has changed. If it has then we need to inform the client
// with a new cookie. The cookie is sent in the gRPC trailer because we would like to
// determine its contents based on the final state the session at the end of the RPC call.
func (middleware *serverSessionMiddleware) CallCompleted(ctx context.Context, _ error) {
	session, err := middleware.manager.GetSession(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get server session: %s", err))
	}

	sessionCookie, err := CreateCookieForSession(session)
	if err != nil {
		panic(err)
	}

	clientCookie, err := GetIncomingCookieByName(ctx, sessionCookie.Name)
	if err == http.ErrNoCookie {
		grpc.SetTrailer(ctx, metadata.Pairs("Set-Cookie", sessionCookie.String()))
		return
	}

	if err != nil {
		panic(err)
	}

	if session.Closed() {
		// Invalidate the client's cookie
		clientCookie.MaxAge = -1
		grpc.SetTrailer(ctx, metadata.Pairs("Set-Cookie", clientCookie.String()))

		if err = middleware.manager.CloseSession(session); err != nil {
			panic(fmt.Sprintf("failed to close server session: %s", err))
		}
		return
	}

	if sessionCookie.String() != clientCookie.String() {
		grpc.SetTrailer(ctx, metadata.Pairs("Set-Cookie", sessionCookie.String()))
	}

	// If the resulting cookie is exactly the same as the
	// client's cookie, then there's no need to send it at all.
}
