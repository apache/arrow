package flight_test

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverAuth struct{}

func (sa *serverAuth) Authenticate(c flight.AuthConn) error {
	in, err := c.Read()
	if err == io.EOF {
		return status.Error(codes.Unauthenticated, "no auth info provided")
	}

	if err != nil {
		return status.Error(codes.FailedPrecondition, "error reading auth handshake")
	}

	// do something with in....
	fmt.Println(string(in))

	// send auth token back
	return c.Send([]byte("foobar"))
}

func (sa *serverAuth) IsValid(token string) (interface{}, error) {
	if token == "foobar" {
		return "foo", nil
	}
	return "", status.Error(codes.PermissionDenied, "invalid auth token")
}

func Example_server() {
	server := flight.NewFlightServer(&serverAuth{})
	server.Init("localhost:0")
	server.RegisterFlightService(&flight.FlightServiceService{})

	go server.Serve()
	defer server.Shutdown()

	conn, err := grpc.Dial(server.Addr().String(), grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	stream, err := client.Handshake(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// ignore error handling here for brevity
	stream.Send(&flight.HandshakeRequest{Payload: []byte("baz")})

	resp, _ := stream.Recv()
	fmt.Println(string(resp.Payload))

	// Output:
	// baz
	// foobar
}
