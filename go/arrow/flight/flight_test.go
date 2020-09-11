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
	context "context"
	"errors"
	"io"
	"log"
	"testing"

	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/internal/arrdata"
	"github.com/apache/arrow/go/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type flightServer struct{}

func (f *flightServer) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	ctx := fs.Context()
	md, ok := metadata.FromIncomingContext(ctx)
	log.Println(md, ok)
	return nil
}

func (f *flightServer) DoGet(_ *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	recs := arrdata.Records["primitives"]

	w := ipc.NewFlightDataWriter(fs, ipc.WithSchema(recs[0].Schema()))
	for _, r := range recs {
		w.Write(r)
	}

	return nil
}

type servAuth struct{}

func (a *servAuth) Authenticate(c flight.AuthConn) error {
	in, err := c.Read()
	if err == io.EOF {
		return nil
	}

	if err != nil {
		return err
	}

	log.Println(string(in))
	return c.Send([]byte("baz"))
}

func (a *servAuth) IsValid(token string) error {
	if token == "baz" {
		return nil
	}
	return errors.New("novalid")
}

type clientAuth struct{}

func (a *clientAuth) Authenticate(c flight.AuthConn) error {
	if err := c.Send([]byte("foobar")); err != nil {
		return err
	}

	in, err := c.Read()
	log.Println(in)
	return err
}

func (a *clientAuth) GetToken() (string, error) {
	return "baz", nil
}

func TestServer(t *testing.T) {
	f := &flightServer{}
	service := &flight.FlightServiceService{
		ListFlights: f.ListFlights,
		DoGet:       f.DoGet,
	}

	s := flight.NewFlightServer(&servAuth{})
	s.Init("localhost:0")
	s.RegisterFlightService(service)

	log.Println(s.Addr())

	go s.Serve()
	defer s.Shutdown()

	client, err := flight.NewFlightClient(s.Addr().String(), &clientAuth{}, grpc.WithInsecure())
	if err != nil {
		t.Error(err)
	}
	defer client.Close()

	err = client.Authenticate(context.Background())
	if err != nil {
		t.Error(err)
	}

	fistream, err := client.ListFlights(context.Background(), &flight.Criteria{})
	if err != nil {
		t.Error(err)
	}

	fi, err := fistream.Recv()
	log.Println(fi, err)

	fdata, err := client.DoGet(context.Background(), &flight.Ticket{})
	if err != nil {
		t.Error(err)
	}

	r, err := ipc.NewFlightDataReader(fdata)
	if err != nil {
		t.Error(err)
	}

	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Error(err)
		}

		log.Println(rec)
	}
}
