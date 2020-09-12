package flight

import (
	context "context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ClientAuthHandler defines an interface for the Flight client to perform
// the authentication handshake. The token that is retrieved from GetToken
// will be sent as part of the context metadata in subsequent requests after
// authentication is performed using the key "auth-token-bin".
type ClientAuthHandler interface {
	Authenticate(AuthConn) error
	GetToken() (string, error)
}

type clientAuthConn struct {
	stream FlightService_HandshakeClient
}

func (a *clientAuthConn) Read() ([]byte, error) {
	in, err := a.stream.Recv()
	if err != nil {
		return nil, err
	}

	return in.Payload, nil
}

func (a *clientAuthConn) Send(b []byte) error {
	return a.stream.Send(&HandshakeRequest{Payload: b})
}

func createClientAuthUnaryInterceptor(auth ClientAuthHandler) grpc.UnaryClientInterceptor {
	if auth == nil {
		return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		tok, err := auth.GetToken()
		if err != nil {
			return status.Errorf(codes.Unauthenticated, "error retrieving token: %s", err)
		}

		return invoker(metadata.NewOutgoingContext(ctx, metadata.Pairs(grpcAuthHeader, tok)), method, req, reply, cc, opts...)
	}
}

func createClientAuthStreamInterceptor(auth ClientAuthHandler) grpc.StreamClientInterceptor {
	if auth == nil {
		return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(ctx, desc, cc, method, opts...)
		}
	}

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if strings.HasSuffix(method, "/Handshake") {
			return streamer(ctx, desc, cc, method, opts...)
		}

		tok, err := auth.GetToken()
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "error retrieving token: %s", err)
		}

		return streamer(metadata.NewOutgoingContext(ctx, metadata.Pairs(grpcAuthHeader, tok)), desc, cc, method, opts...)
	}
}
