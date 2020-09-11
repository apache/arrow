package flight

import (
	context "context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FlightClient interface {
	Authenticate(context.Context, ...grpc.CallOption) error
	Close() error
	FlightServiceClient
}

type client struct {
	conn        *grpc.ClientConn
	authHandler ClientAuthHandler

	FlightServiceClient
}

func NewFlightClient(addr string, auth ClientAuthHandler, opts ...grpc.DialOption) (*client, error) {
	if auth != nil {
		opts = append([]grpc.DialOption{
			grpc.WithChainStreamInterceptor(createClientAuthStreamInterceptor(auth)),
			grpc.WithChainUnaryInterceptor(createClientAuthUnaryInterceptor(auth)),
		}, opts...)
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &client{conn: conn, FlightServiceClient: NewFlightServiceClient(conn), authHandler: auth}, nil
}

func (c *client) Authenticate(ctx context.Context, opts ...grpc.CallOption) error {
	if c.authHandler == nil {
		return status.Error(codes.NotFound, "cannot authenticate without an auth-handler")
	}

	stream, err := c.FlightServiceClient.Handshake(ctx, opts...)
	if err != nil {
		return err
	}

	return c.authHandler.Authenticate(&clientAuthConn{stream})
}

func (c *client) Close() error {
	c.FlightServiceClient = nil
	return c.conn.Close()
}
