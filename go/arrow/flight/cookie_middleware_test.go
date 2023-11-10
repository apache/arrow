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

package flight_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// strings.Cut is go1.18+ so let's just stick a duplicate of it in here
// for now since we want to support go1.17
func cut(s, sep string) (before, after string, found bool) {
	if i := strings.Index(s, sep); i >= 0 {
		return s[:i], s[i+len(sep):], true
	}
	return s, "", false
}

type serverAddCookieMiddleware struct {
	expectedCookies map[string]string

	cookies []*http.Cookie
}

func (s *serverAddCookieMiddleware) StartCall(ctx context.Context) context.Context {
	if s.expectedCookies == nil {
		md := make(metadata.MD)
		for _, c := range s.cookies {
			md.Append("Set-Cookie", c.String())
		}
		grpc.SetHeader(ctx, md)
		return nil
	}

	cookies := metadata.ValueFromIncomingContext(ctx, "cookie")

	got := make(map[string]string)
	for _, line := range cookies {
		line = textproto.TrimString(line)

		var part string
		for len(line) > 0 {
			part, line, _ = cut(line, ";")
			part = textproto.TrimString(part)
			if part == "" {
				continue
			}

			name, val, _ := cut(part, "=")
			name = textproto.TrimString(name)
			if len(val) > 1 && val[0] == '"' && val[len(val)-1] == '"' {
				val = val[1 : len(val)-1]
			}

			got[name] = val
		}
	}

	if !reflect.DeepEqual(s.expectedCookies, got) {
		panic(fmt.Sprintf("did not get expected cookies, expected %+v, got %+v", s.expectedCookies, got))
	}

	return nil
}

func (s *serverAddCookieMiddleware) CallCompleted(ctx context.Context, err error) {}

func TestClientCookieMiddleware(t *testing.T) {
	cookieMiddleware := &serverAddCookieMiddleware{}

	s := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(cookieMiddleware),
	})
	s.Init("localhost:0")
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	credsOpt := grpc.WithTransportCredentials(insecure.NewCredentials())

	tests := []struct {
		testname string
		cookies  []*http.Cookie
		expected map[string]string
	}{
		{"single cookie", []*http.Cookie{{Name: "Cookie-1", Value: "v$1", Raw: "Cookie-1=v$1"}},
			map[string]string{"Cookie-1": "v$1"}},
		{"expired", []*http.Cookie{{
			Name: "NID", Value: "99=YsDT5", Expires: time.Date(2011, 11, 23, 1, 5, 3, 0, time.UTC),
			RawExpires: "Wed, 23-Nov-2011 01:05:03 GMT", Raw: "NID=99=YsDT5; expires=Wed, 23-Nov-11 01:05:03 GMT"}},
			map[string]string{}},
		{"multiple", []*http.Cookie{
			{Name: "negative maxage", Value: "foobar", MaxAge: -1},
			{Name: "special-1", Value: " z"},
			{Name: "cookie-2", Value: "v$2"},
		},
			map[string]string{"special-1": " z", "cookie-2": "v$2"}},
	}

	makeReq := func(c flight.Client, t *testing.T) {
		flightStream, err := c.ListFlights(context.Background(), &flight.Criteria{})
		assert.NoError(t, err)

		for {
			_, err := flightStream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				assert.NoError(t, err)
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.testname, func(t *testing.T) {
			cookieMiddleware.expectedCookies = nil

			client, err := flight.NewClientWithMiddleware(s.Addr().String(), nil,
				[]flight.ClientMiddleware{flight.NewClientCookieMiddleware()}, credsOpt)
			require.NoError(t, err)
			defer client.Close()

			cookieMiddleware.cookies = tt.cookies
			makeReq(client, t)

			cookieMiddleware.expectedCookies = tt.expected
			makeReq(client, t)
		})
	}
}

func TestCookieExpiration(t *testing.T) {
	cookieMiddleware := &serverAddCookieMiddleware{}

	s := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(cookieMiddleware),
	})
	s.Init("localhost:0")
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	makeReq := func(c flight.Client, t *testing.T) {
		flightStream, err := c.ListFlights(context.Background(), &flight.Criteria{})
		assert.NoError(t, err)

		for {
			_, err := flightStream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				assert.NoError(t, err)
			}
		}
	}

	credsOpt := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := flight.NewClientWithMiddleware(s.Addr().String(), nil,
		[]flight.ClientMiddleware{flight.NewClientCookieMiddleware()}, credsOpt)
	require.NoError(t, err)
	defer client.Close()

	// set cookies
	cookieMiddleware.cookies = []*http.Cookie{
		{Name: "foo", Value: "bar"},
		{Name: "foo2", Value: "bar2", MaxAge: 1},
	}
	makeReq(client, t)

	// validate set
	cookieMiddleware.expectedCookies = map[string]string{
		"foo": "bar", "foo2": "bar2",
	}
	makeReq(client, t)

	// wait for foo2 to expire and validate it doesn't get sent
	time.Sleep(1 * time.Second)
	cookieMiddleware.expectedCookies = map[string]string{
		"foo": "bar",
	}
	makeReq(client, t)

	// update value
	cookieMiddleware.cookies = []*http.Cookie{
		{Name: "foo", Value: "baz"},
	}
	cookieMiddleware.expectedCookies = nil
	makeReq(client, t)

	// validate updated value is sent
	cookieMiddleware.expectedCookies = map[string]string{
		"foo": "baz",
	}
	makeReq(client, t)

	// force delete cookie
	cookieMiddleware.expectedCookies = nil
	cookieMiddleware.cookies = []*http.Cookie{
		{Name: "foo", MaxAge: -1}, // delete now!
	}
	makeReq(client, t)

	// verify it's been deleted
	cookieMiddleware.expectedCookies = map[string]string{}
	makeReq(client, t)
}
