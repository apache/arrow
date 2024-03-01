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
	"fmt"
	"net/http"
	"sync"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/google/uuid"
)

const StatefulSessionCookieName string = "arrow_flight_session_id"

// SessionStore handles persistence of ServerSession instances for
// stateful session implementations.
type SessionStore interface {
	// Get the session with the provided ID
	Get(id string) (ServerSession, error)
	// Persist the provided session
	Put(session ServerSession) error
	// Remove the session with the provided ID
	Remove(id string) error
}

// SessionFactory creates ServerSession instances
type SessionFactory interface {
	// Create a new, empty ServerSession
	CreateSession() (ServerSession, error)
}

// NewSessionStore creates a simple in-memory, goroutine-safe SessionStore
func NewSessionStore() *sessionStore {
	return &sessionStore{sessions: make(map[string]ServerSession)}
}

type sessionStore struct {
	sessions map[string]ServerSession
	mu       sync.RWMutex
}

func (store *sessionStore) Get(id string) (ServerSession, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	session, found := store.sessions[id]
	if !found {
		return nil, fmt.Errorf("session with ID %s not found", id)
	}
	return session, nil
}

func (store *sessionStore) Put(session ServerSession) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.sessions[session.Token()] = session
	return nil
}

func (store *sessionStore) Remove(id string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.sessions, id)

	return nil
}

// NewSessionFactory creates a new SessionFactory, producing in-memory, goroutine-safe ServerSessions.
// The provided function MUST produce collision-free identifiers.
func NewSessionFactory(generateID func() string) *sessionFactory {
	return &sessionFactory{generateID: generateID}
}

type sessionFactory struct {
	generateID func() string
}

func (factory *sessionFactory) CreateSession() (ServerSession, error) {
	return &statefulServerSession{
		id:            factory.generateID(),
		serverSession: serverSession{options: make(map[string]*flight.SessionOptionValue)},
	}, nil
}

type statefulServerSession struct {
	serverSession
	id string
}

func (session *statefulServerSession) Token() string {
	return session.id
}

type StatefulSessionManagerOption func(*statefulServerSessionManager)

// WithFactory specifies the SessionFactory to use for session creation
func WithFactory(factory SessionFactory) StatefulSessionManagerOption {
	return func(manager *statefulServerSessionManager) {
		manager.factory = factory
	}
}

// WithStore specifies the SessionStore to use for session persistence
func WithStore(store SessionStore) StatefulSessionManagerOption {
	return func(manager *statefulServerSessionManager) {
		manager.store = store
	}
}

// NewStatefulServerSessionManager creates a new ServerSessionManager.
//
//   - If unset via options, the default factory produces sessions with UUIDs.
//   - If unset via options, sessions are stored in-memory.
func NewStatefulServerSessionManager(opts ...StatefulSessionManagerOption) *statefulServerSessionManager {
	manager := &statefulServerSessionManager{}
	for _, opt := range opts {
		opt(manager)
	}

	// Set defaults if not specified above
	if manager.factory == nil {
		manager.factory = NewSessionFactory(uuid.NewString)
	}

	if manager.store == nil {
		manager.store = NewSessionStore()
	}

	return manager
}

type statefulServerSessionManager struct {
	factory SessionFactory
	store   SessionStore
}

func (manager *statefulServerSessionManager) CreateSession(ctx context.Context) (ServerSession, error) {
	session, err := manager.factory.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create new session: %w", err)
	}

	if err = manager.store.Put(session); err != nil {
		return nil, fmt.Errorf("failed to persist new session: %w", err)
	}

	return session, nil
}

func (manager *statefulServerSessionManager) GetSession(ctx context.Context) (ServerSession, error) {
	session, err := GetSessionFromContext(ctx)
	if err == nil {
		return session, nil
	}

	sessionID, err := getSessionIDFromIncomingCookie(ctx)
	if err == nil {
		return manager.store.Get(sessionID)
	}
	if err == http.ErrNoCookie {
		return nil, ErrNoSession
	}

	return nil, fmt.Errorf("failed to get current session from cookie: %w", err)
}

func (manager *statefulServerSessionManager) CloseSession(session ServerSession) error {
	if err := manager.store.Remove(session.Token()); err != nil {
		return fmt.Errorf("failed to remove server session from store: %w", err)
	}
	return nil
}

// Check the provided context for cookies in the incoming gRPC metadata.
func getSessionIDFromIncomingCookie(ctx context.Context) (string, error) {
	cookie, err := GetIncomingCookieByName(ctx, StatefulSessionCookieName)
	if err != nil {
		return "", err
	}

	return cookie.Value, nil
}
