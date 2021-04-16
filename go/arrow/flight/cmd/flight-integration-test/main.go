package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type testServerAuth struct {
	username string
	password string
}

func (ts *testServerAuth) Authenticate(c flight.AuthConn) error {
	in, err := c.Read()
	if err == io.EOF {
		return status.Error(codes.Unauthenticated, "no auth info provided")
	}

	if err != nil {
		return status.Error(codes.Unauthenticated, "error reading auth handshake")
	}

	var ba flight.BasicAuth
	if err = proto.Unmarshal(in, &ba); err != nil {
		return status.Errorf(codes.Unauthenticated, "couldn't deserialize basic auth: %s", err.Error())
	}

	if ba.GetUsername() != ts.username || ba.GetPassword() != ts.password {
		return status.Error(codes.Unauthenticated, "invalid token")
	}

	return c.Send([]byte(ba.Username))
}

func (ts *testServerAuth) IsValid(token string) (interface{}, error) {
	if token != ts.username {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	return ts.username, nil
}

type testClientAuth struct {
	username string
	password string

	token []byte
}

func (tc *testClientAuth) Authenticate(_ context.Context, c flight.AuthConn) error {
	data, err := proto.Marshal(&flight.BasicAuth{Username: tc.username, Password: tc.password})
	if err != nil {
		return err
	}

	if err = c.Send(data); err != nil {
		return err
	}

	tc.token, err = c.Read()
	return err
}

func (tc *testClientAuth) GetToken(_ context.Context) (string, error) {
	return string(tc.token), nil
}

type Scenario interface {
	MakeServer(opt ...grpc.ServerOption) flight.Server
	RunClient(context.Context, flight.Client) error
}

// the expected username and password for the basic auth integration test
const (
	authUsername = "arrow"
	authPassword = "flight"
)

func checkActionResults(ctx context.Context, c flight.Client, action *flight.Action, expected []string) error {
	stream, err := c.DoAction(ctx, action)
	if err != nil {
		return err
	}

	for _, ex := range expected {
		res, err := stream.Recv()
		if err != nil {
			return err
		}

		if ex != string(res.GetBody()) {
			return fmt.Errorf("got wrong result; expected %s, but got %s", ex, string(res.GetBody()))
		}
	}

	_, err = stream.Recv()
	if err != io.EOF {
		return fmt.Errorf("action result stream had too many entries")
	}
	return nil
}

type authBasicProto struct{}

func (authBasicProto) MakeServer(opt ...grpc.ServerOption) flight.Server {
	s := flight.NewFlightServer(&testServerAuth{username: authUsername, password: authPassword}, opt...)
	s.RegisterFlightService(&flight.FlightServiceService{
		DoAction: func(_ *flight.Action, as flight.FlightService_DoActionServer) error {
			buf := flight.AuthFromContext(as.Context()).(string)
			as.Send(&flight.Result{Body: []byte(buf)})
			return nil
		},
	})
	return s
}

func (authBasicProto) RunClient(ctx context.Context, c flight.Client) error {
	actcl, err := c.DoAction(ctx, &flight.Action{})
	if err != nil {
		return err
	}

	_, err = actcl.Recv()
	// client is unauthenticated and should fail
	st := status.Convert(err)
	if st.Code() != codes.Unauthenticated {
		return fmt.Errorf("expected Unauthenticated but got %s", st.Code().String())
	}

	if err = c.Authenticate(ctx); err != nil {
		log.Println(err)
		return err
	}
	return checkActionResults(ctx, c, &flight.Action{}, []string{authUsername})
}

func main() {
	log.SetPrefix("arrow-flight-server: ")
	log.SetFlags(0)

	var (
		port     = flag.Int("port", 31337, "port to use")
		scenario = flag.String("scenario", "", "Integration test scenario to run")
		host     = flag.String("host", "", "Set to run the client instead of server")
	)

	flag.Parse()

	var sc Scenario
	switch *scenario {
	case "auth:basic_proto":
		sc = authBasicProto{}
	case "middleware":
		panic("unimplemented scenario")
	}

	if *host != "" {
		c, err := flight.NewFlightClient(net.JoinHostPort(*host, strconv.Itoa(*port)),
			&testClientAuth{username: authUsername, password: authPassword}, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}

		if err = sc.RunClient(context.Background(), c); err != nil {
			panic(err)
		}
		return
	}

	s := sc.MakeServer()
	s.Init(net.JoinHostPort("localhost", strconv.Itoa(*port)))
	s.SetShutdownOnSignals(os.Kill, os.Interrupt)
	fmt.Println("Server listening on", s.Addr())
	if err := s.Serve(); err != nil {
		panic(err)
	}
}
