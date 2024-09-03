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

package session

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"google.golang.org/protobuf/proto"
)

const StatelessSessionCookieName string = "arrow_flight_session"

// NewStatelessServerSessionManager creates a new StatelessServerSessionManager.
//
// The tokens it produces contain the entire session state, so sessions can
// be maintained across multiple backends.
// Token contents are considered opaque but are NOT encrypted.
func NewStatelessServerSessionManager() *statelessServerSessionManager {
	return &statelessServerSessionManager{}
}

type statelessServerSessionManager struct{}

func (manager *statelessServerSessionManager) CreateSession(ctx context.Context) (ServerSession, error) {
	return NewStatelessServerSession(nil), nil
}

func (manager *statelessServerSessionManager) GetSession(ctx context.Context) (ServerSession, error) {
	session, err := GetSessionFromContext(ctx)
	if err == nil {
		return session, nil
	}

	session, err = getSessionFromIncomingCookie(ctx)
	if err == nil {
		return session, err
	}
	if err == http.ErrNoCookie {
		return nil, ErrNoSession
	}

	return nil, fmt.Errorf("failed to get current session from cookie: %w", err)
}

func (manager *statelessServerSessionManager) CloseSession(session ServerSession) error {
	return nil
}

// NewStatelessServerSession creates a new instance of a server session that can serialize its entire state.
// A map is provided containing the initial state. If it is nil, a new empty state will be created.
func NewStatelessServerSession(options map[string]*flight.SessionOptionValue) *statelessServerSession {
	if options == nil {
		options = make(map[string]*flight.SessionOptionValue)
	}

	return &statelessServerSession{
		serverSession: serverSession{options: options},
	}
}

type statelessServerSession struct {
	serverSession
}

// First encode session contents using protobuf binary marshaller.
// Then base64 encode the resulting bytes for client compatibility.
func (session *statelessServerSession) Token() string {
	session.mu.RLock()
	defer session.mu.RUnlock()

	payload := flight.GetSessionOptionsResult{SessionOptions: session.options}
	b, err := proto.Marshal(&payload)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal stateless token: %s", err))
	}

	return base64.StdEncoding.EncodeToString(b)
}

// Reconstruct the session from its fully encoded token representation
func decodeStatelessToken(token string) (*statelessServerSession, error) {
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}

	var parsed flight.GetSessionOptionsResult
	if err := proto.Unmarshal(decoded, &parsed); err != nil {
		return nil, err
	}

	return NewStatelessServerSession(parsed.SessionOptions), nil
}

// Check the provided context for a cookie in the incoming gRPC metadata containing the
// stateless session token. Decode the token payload to reconstruct the session.
func getSessionFromIncomingCookie(ctx context.Context) (*statelessServerSession, error) {
	cookie, err := GetIncomingCookieByName(ctx, StatelessSessionCookieName)
	if err != nil {
		return nil, err
	}

	return decodeStatelessToken(cookie.Value)
}
