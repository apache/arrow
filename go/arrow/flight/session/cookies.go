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

	"google.golang.org/grpc/metadata"
)

func GetIncomingCookieByName(ctx context.Context, name string) (http.Cookie, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return http.Cookie{}, fmt.Errorf("no metadata found for incoming context")
	}

	header := make(http.Header, md.Len())
	for k, v := range md {
		for _, val := range v {
			header.Add(k, val)
		}
	}

	cookie, err := (&http.Request{Header: header}).Cookie(name)
	if err != nil {
		return http.Cookie{}, err
	}

	if cookie == nil {
		return http.Cookie{}, fmt.Errorf("failed to get cookie with name: %s", name)
	}

	return *cookie, nil
}

func CreateCookieForSession(session ServerSession) (http.Cookie, error) {
	var key string

	if session == nil {
		return http.Cookie{}, ErrNoSession
	}

	switch s := session.(type) {
	case *statefulServerSession:
		key = StatefulSessionCookieName
	case *statelessServerSession:
		key = StatelessSessionCookieName
	default:
		return http.Cookie{}, fmt.Errorf("cannot serialize session of type %T as cookie", s)
	}

	// Reuse the std http lib functionality for constructing cookies
	cookie, err := (&http.Request{
		Header: http.Header{"Cookie": []string{fmt.Sprintf("%s=%s", key, session.Token())}},
	}).Cookie(key)
	if err != nil {
		return http.Cookie{}, err
	}
	if cookie == nil {
		return http.Cookie{}, fmt.Errorf("failed to construct cookie for session: %s", session.Token())
	}

	return *cookie, nil
}
