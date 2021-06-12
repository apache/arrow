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

package flight_integration

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type testServerMiddleware struct{}

func (testServerMiddleware) StartCall(ctx context.Context) context.Context {
	var val string

	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		received := md.Get("x-middleware")
		if len(received) > 0 {
			val = received[0]
		}
	}

	grpc.SetHeader(ctx, metadata.Pairs("x-middleware", val))
	return nil
}

func (testServerMiddleware) CallCompleted(_ context.Context, _ error) {}

type testClientMiddleware struct {
	received string
}

func (tm *testClientMiddleware) StartCall(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-middleware", "expected value")
}

func (tm *testClientMiddleware) HeadersReceived(_ context.Context, md metadata.MD) {
	received := md.Get("x-middleware")
	if len(received) > 0 {
		tm.received = received[0]
	}
}
