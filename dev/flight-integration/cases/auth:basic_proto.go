package cases

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/dev/flight-integration/scenario"
	"github.com/apache/arrow/dev/flight-integration/tester"

	"github.com/apache/arrow/go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func init() {
	var (
		username   = "arrow"
		password   = "flight"
		authHeader = "auth-token-bin"
		token      = username
	)

	scenario.Register(
		scenario.Scenario{
			Name: "auth:basic_proto",
			Steps: []scenario.ScenarioStep{
				{
					Name: "unauthenticated_action",
					ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
						return status.Error(codes.Unauthenticated, "no token")
					}},
				},
				{
					Name: "auth_handshake",
					ServerHandler: scenario.Handler{Handshake: func(fs flight.FlightService_HandshakeServer) error {

						in, err := fs.Recv()
						if err != nil {
							return err
						}

						var incoming flight.BasicAuth
						if err = proto.Unmarshal(in.Payload, &incoming); err != nil {
							return err
						}

						if incoming.Username != username {
							return fmt.Errorf("incorrect username for auth: expected: %s, got: %s", username, incoming.Username)
						}

						if incoming.Password != password {
							return fmt.Errorf("incorrect password for auth: expected: %s, got: %s", password, incoming.Password)
						}

						return fs.Send(&flight.HandshakeResponse{Payload: []byte(token)})
					}},
				},
				{
					Name: "authenticated_action",
					ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
						md, ok := metadata.FromIncomingContext(fs.Context())
						if !ok {
							return fmt.Errorf("auth metadata not present")
						}

						vals := md.Get(authHeader)
						if len(vals) != 1 {
							return fmt.Errorf("expected 1 token value for header \"%s\" but found %d: %s", authHeader, len(vals), vals)
						}

						if vals[0] != token {
							return fmt.Errorf("invalid token: expected %s, got %s", token, vals[0])
						}

						return fs.Send(&flight.Result{Body: []byte(token)})
					}},
				},
			},
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {
				actionStream, err := client.DoAction(ctx, &flight.Action{})
				t.Require().NoError(err)

				_, err = actionStream.Recv()
				st, ok := status.FromError(err)
				t.Require().True(ok, "failed to extract grpc status from stream error")
				t.Assert().Equalf(codes.Unauthenticated, st.Code(), "expected stream error: %s, got: %s", codes.Unauthenticated, st.Code())

				handshakeStream, err := client.Handshake(ctx)
				t.Require().NoError(err)

				b, err := proto.Marshal(&flight.BasicAuth{Username: username, Password: password})
				t.Require().NoError(err)

				t.Require().NoError(handshakeStream.Send(&flight.HandshakeRequest{Payload: b}))

				in, err := handshakeStream.Recv()
				t.Require().NoError(err)

				_, err = handshakeStream.Recv()
				t.Require().ErrorIs(err, io.EOF, "handshake result stream had too many entries")
				t.Require().NoError(handshakeStream.CloseSend())

				ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{authHeader: string(in.Payload)}))
				actionStream, err = client.DoAction(ctx, &flight.Action{})
				t.Require().NoError(err)

				res, err := actionStream.Recv()
				t.Require().NoError(err)

				t.Assert().Equal([]byte(token), res.Body)

				_, err = actionStream.Recv()
				t.Require().ErrorIs(err, io.EOF, "action result stream had too many entries")
				t.Require().NoError(actionStream.CloseSend())

			},
		},
	)
}
