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
	"testing"

	"github.com/apache/arrow/go/v14/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	status "google.golang.org/grpc/status"
)

const (
	validUsername   = "flight_username"
	validPassword   = "flight_password"
	invalidUsername = "invalid_flight_username"
	invalidPassword = "invalid_flight_password"
	validBearer     = "CAREBARESTARE"
	invalidBearer   = "PANDABEAR"
)

type HeaderAuthTestFlight struct {
	flight.BaseFlightServer
}

func (h *HeaderAuthTestFlight) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	fs.Send(&flight.FlightInfo{
		Schema: []byte("foobar"),
	})
	return nil
}

func (h *HeaderAuthTestFlight) GetSchema(ctx context.Context, in *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return &flight.SchemaResult{Schema: []byte(flight.AuthFromContext(ctx).(string))}, nil
}

type validator struct{}

func (*validator) Validate(username, password string) (string, error) {
	if username == validUsername && password == validPassword {
		return validBearer, nil
	}
	return "", status.Errorf(codes.Unauthenticated, "invalid user/password")
}

func (*validator) IsValid(bearerToken string) (interface{}, error) {
	if bearerToken == validBearer {
		return "carebears", nil
	}
	return "", status.Errorf(codes.Unauthenticated, "invalid authentication")
}

func TestErrorAuths(t *testing.T) {
	unary, stream := flight.CreateServerBearerTokenAuthInterceptors(&validator{})
	s := flight.NewFlightServer(grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream))
	s.Init("localhost:0")
	f := &HeaderAuthTestFlight{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewFlightClient(s.Addr().String(), nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("non basic auth", func(t *testing.T) {
		fc, err := client.Handshake(metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{"authorization": "Foobar ****"})))
		if err != nil {
			t.Fatal(err)
		}

		_, err = fc.Recv()
		if err == nil {
			t.Fatal("should have failed")
		}
	})

	t.Run("invalid auth", func(t *testing.T) {
		_, err := client.AuthenticateBasicToken(context.Background(), invalidUsername, invalidPassword)
		if err == nil {
			t.Fatal("should have failed")
		}
	})

	t.Run("invalid base64", func(t *testing.T) {
		fc, err := client.Handshake(metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{"authorization": "Basic ****"})))
		if err != nil {
			t.Fatal(err)
		}

		_, err = fc.Recv()
		if err == nil {
			t.Fatal("should have failed")
		}
	})

	t.Run("invalid bearer token", func(t *testing.T) {
		fs, _ := client.ListFlights(metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{"authorization": "Bearer " + invalidBearer})), &flight.Criteria{})
		_, err = fs.Recv()
		if err == nil {
			t.Fatal("should have errored with invalid bearer token")
		}
	})

	t.Run("invalid auth type", func(t *testing.T) {
		fs, _ := client.ListFlights(metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{"authorization": "FunnyStuff " + invalidBearer})), &flight.Criteria{})
		_, err = fs.Recv()
		if err == nil {
			t.Fatal("should have errored with invalid bearer token")
		}
	})

	t.Run("test no auth, unary", func(t *testing.T) {
		_, err := client.GetSchema(context.Background(), &flight.FlightDescriptor{})
		if err == nil {
			t.Fatal("should have errored")
		}
	})

	t.Run("test invalid auth, unary", func(t *testing.T) {
		_, err := client.GetSchema(metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{"authorization": "Bearer Foobarmoo"})), &flight.FlightDescriptor{})
		if err == nil {
			t.Fatal("should have errored")
		}
	})
}

func TestBasicAuthHelpers(t *testing.T) {
	s := flight.NewServerWithMiddleware([]flight.ServerMiddleware{flight.CreateServerBasicAuthMiddleware(&validator{})})
	s.Init("localhost:0")
	f := &HeaderAuthTestFlight{}
	s.RegisterFlightService(f)

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewFlightClient(s.Addr().String(), nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	fs, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = fs.Recv()
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatal("Should have failed with unauthenticated error")
	}

	ctx, err = client.AuthenticateBasicToken(ctx, validUsername, validPassword)
	if err != nil {
		t.Fatal(err)
	}

	fs, err = client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		t.Fatal(err)
	}

	info, err := fs.Recv()
	if err != nil {
		t.Fatal(err)
	}

	if string(info.Schema) != "foobar" {
		t.Fatal("should have received 'foobar'")
	}

	sc, err := client.GetSchema(ctx, &flight.FlightDescriptor{})
	if err != nil {
		t.Fatal(err)
	}

	if string(sc.Schema) != "carebears" {
		t.Fatal("should have received carebears")
	}
}
