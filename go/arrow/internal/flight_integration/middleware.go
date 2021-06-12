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
