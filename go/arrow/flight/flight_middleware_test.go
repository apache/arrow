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
	"io"
	sync "sync"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/internal/arrdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type ServerMiddlewareAddHeader struct {
	ctx context.Context
}

func (s *ServerMiddlewareAddHeader) StartCall(ctx context.Context) context.Context {
	grpc.SetHeader(ctx, metadata.Pairs("foo", "bar"))
	s.ctx = ctx

	return nil
}

func (s *ServerMiddlewareAddHeader) CallCompleted(ctx context.Context, err error) {
	if s.ctx != ctx {
		panic("invalid context")
	}

	grpc.SetTrailer(ctx, metadata.Pairs("super", "duper"))

	if err != nil {
		panic("got error")
	}
}

type ServerTraceMiddleware struct{}

type tracetestKey struct{}

func (s ServerTraceMiddleware) StartCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, tracetestKey{}, "foobar")
}

func (s ServerTraceMiddleware) CallCompleted(ctx context.Context, _ error) {
	v := ctx.Value(tracetestKey{}).(string)
	if v != "foobar" {
		panic("missing value from context in middleware test")
	}
}

type ServerExpectHeaderMiddleware struct{}

func (s ServerExpectHeaderMiddleware) StartCall(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		panic("missing metadata headers")
	}

	bar := md.Get("foo")
	if len(bar) != 1 || bar[0] != "bar" {
		panic("incorrect header received: " + bar[0])
	}

	return nil
}

func (s ServerExpectHeaderMiddleware) CallCompleted(context.Context, error) {}

func TestServerStreamMiddleware(t *testing.T) {
	s := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(&ServerMiddlewareAddHeader{}),
		flight.CreateServerMiddleware(ServerTraceMiddleware{}),
	})
	s.Init("localhost:0")
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewClientWithMiddleware(s.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()

	flightStream, err := client.ListFlights(context.Background(), &flight.Criteria{})
	require.NoError(t, err)

	md, err := flightStream.Header()
	assert.NoError(t, err)
	assert.Equal(t, []string{"bar"}, md.Get("foo"))

	for {
		info, err := flightStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
		}

		fname := info.GetFlightDescriptor().GetPath()[0]
		recs, ok := arrdata.Records[fname]
		assert.True(t, ok)

		sc, err := flight.DeserializeSchema(info.GetSchema(), f.mem)
		assert.NoError(t, err)

		assert.True(t, recs[0].Schema().Equal(sc))
	}

	md = flightStream.Trailer()
	assert.Equal(t, []string{"duper"}, md.Get("super"))
}

func TestServerUnaryMiddleware(t *testing.T) {
	s := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(&ServerMiddlewareAddHeader{}),
		flight.CreateServerMiddleware(ServerTraceMiddleware{}),
	})
	s.Init("localhost:0")
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewClientWithMiddleware(s.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()

	for name, testrecs := range arrdata.Records {
		t.Run("flight get schema: "+name, func(t *testing.T) {
			var (
				hdrMD     metadata.MD
				trailerMD metadata.MD
			)
			res, err := client.GetSchema(context.Background(), &flight.FlightDescriptor{Path: []string{name}}, grpc.Header(&hdrMD), grpc.Trailer(&trailerMD))
			if err != nil {
				t.Fatal(err)
			}

			schema, err := flight.DeserializeSchema(res.GetSchema(), f.getmem())
			if err != nil {
				t.Fatal(err)
			}

			if !testrecs[0].Schema().Equal(schema) {
				t.Fatalf("schema not match: \ngot = %#v\nwant = %#v\n", schema, testrecs[0].Schema())
			}

			assert.Equal(t, []string{"bar"}, hdrMD.Get("foo"))
			assert.Equal(t, []string{"duper"}, trailerMD.Get("super"))
		})
	}
}

type ClientTestSendHeaderMiddleware struct {
	ctx context.Context
	md  metadata.MD
	mx  sync.Mutex
}

func (c *ClientTestSendHeaderMiddleware) StartCall(ctx context.Context) context.Context {
	c.ctx = context.WithValue(metadata.AppendToOutgoingContext(ctx, "foo", "bar"), tracetestKey{}, "super")
	return c.ctx
}

func (c *ClientTestSendHeaderMiddleware) CallCompleted(ctx context.Context, err error) {
	val := ctx.Value(tracetestKey{}).(string)
	if val != "super" {
		panic("invalid context client middleware")
	}
}

func (c *ClientTestSendHeaderMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
	val := ctx.Value(tracetestKey{}).(string)
	if val != "super" {
		panic("invalid context client middleware")
	}

	c.mx.Lock()
	defer c.mx.Unlock()
	c.md = md
}

func TestClientStreamMiddleware(t *testing.T) {
	s := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(&ServerExpectHeaderMiddleware{}),
		flight.CreateServerMiddleware(&ServerMiddlewareAddHeader{}),
	})
	s.Init("localhost:0")
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	middleware := &ClientTestSendHeaderMiddleware{}
	client, err := flight.NewClientWithMiddleware(s.Addr().String(), nil, []flight.ClientMiddleware{
		flight.CreateClientMiddleware(middleware),
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()

	flightStream, err := client.ListFlights(context.Background(), &flight.Criteria{})
	require.NoError(t, err)

	for {
		info, err := flightStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
		}

		fname := info.GetFlightDescriptor().GetPath()[0]
		recs, ok := arrdata.Records[fname]
		assert.True(t, ok)

		sc, err := flight.DeserializeSchema(info.GetSchema(), f.mem)
		assert.NoError(t, err)

		assert.True(t, recs[0].Schema().Equal(sc))
	}

	middleware.mx.Lock()
	defer middleware.mx.Unlock()
	assert.Equal(t, []string{"bar"}, middleware.md.Get("foo"))
	assert.Equal(t, []string{"duper"}, middleware.md.Get("super"))
}

func TestClientUnaryMiddleware(t *testing.T) {
	s := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		flight.CreateServerMiddleware(&ServerMiddlewareAddHeader{}),
		flight.CreateServerMiddleware(ServerExpectHeaderMiddleware{}),
	})
	s.Init("localhost:0")
	f := &flightServer{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	middle := &ClientTestSendHeaderMiddleware{}
	client, err := flight.NewClientWithMiddleware(s.Addr().String(), nil, []flight.ClientMiddleware{
		flight.CreateClientMiddleware(middle),
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))

	require.NoError(t, err)
	defer client.Close()

	for name, testrecs := range arrdata.Records {
		t.Run("flight get schema: "+name, func(t *testing.T) {
			res, err := client.GetSchema(context.Background(), &flight.FlightDescriptor{Path: []string{name}})
			if err != nil {
				t.Fatal(err)
			}

			schema, err := flight.DeserializeSchema(res.GetSchema(), f.getmem())
			if err != nil {
				t.Fatal(err)
			}

			if !testrecs[0].Schema().Equal(schema) {
				t.Fatalf("schema not match: \ngot = %#v\nwant = %#v\n", schema, testrecs[0].Schema())
			}

			assert.Equal(t, []string{"bar"}, middle.md.Get("foo"))
			assert.Equal(t, []string{"duper"}, middle.md.Get("super"))

			middle.md = metadata.MD{}
		})
	}
}
