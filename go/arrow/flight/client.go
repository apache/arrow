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

package flight

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow/go/v13/arrow/flight/internal/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type (
	FlightServiceClient             = flight.FlightServiceClient
	FlightService_HandshakeClient   = flight.FlightService_HandshakeClient
	FlightService_ListFlightsClient = flight.FlightService_ListFlightsClient
	FlightService_DoGetClient       = flight.FlightService_DoGetClient
	FlightService_DoPutClient       = flight.FlightService_DoPutClient
	FlightService_DoExchangeClient  = flight.FlightService_DoExchangeClient
	FlightService_DoActionClient    = flight.FlightService_DoActionClient
	FlightService_ListActionsClient = flight.FlightService_ListActionsClient

	DescriptorType = flight.FlightDescriptor_DescriptorType
	BasicAuth      = flight.BasicAuth
)

const (
	DescriptorUNKNOWN = flight.FlightDescriptor_UNKNOWN
	DescriptorPATH    = flight.FlightDescriptor_PATH
	DescriptorCMD     = flight.FlightDescriptor_CMD
)

var NewFlightServiceClient = flight.NewFlightServiceClient

// Client is an interface wrapped around the generated FlightServiceClient which is
// generated by grpc protobuf definitions. This interface provides a useful hiding
// of the authentication handshake via calling Authenticate and using the
// ClientAuthHandler rather than manually having to implement the grpc communication
// and sending of the auth token.
type Client interface {
	// Authenticate uses the ClientAuthHandler that was used when creating the client
	// in order to use the Handshake endpoints of the service.
	Authenticate(context.Context, ...grpc.CallOption) error
	AuthenticateBasicToken(ctx context.Context, username string, password string, opts ...grpc.CallOption) (context.Context, error)
	CancelFlightInfo(ctx context.Context, request *CancelFlightInfoRequest, opts ...grpc.CallOption) (CancelFlightInfoResult, error)
	Close() error
	RenewFlightEndpoint(ctx context.Context, request *RenewFlightEndpointRequest, opts ...grpc.CallOption) (*FlightEndpoint, error)
	// join the interface from the FlightServiceClient instead of re-defining all
	// the endpoints here.
	FlightServiceClient
}

type CustomClientMiddleware interface {
	StartCall(ctx context.Context) context.Context
}

type ClientPostCallMiddleware interface {
	CallCompleted(ctx context.Context, err error)
}

type ClientHeadersMiddleware interface {
	HeadersReceived(ctx context.Context, md metadata.MD)
}

func CreateClientMiddleware(middleware CustomClientMiddleware) ClientMiddleware {
	return ClientMiddleware{
		Unary: func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			nctx := middleware.StartCall(ctx)
			if nctx != nil {
				ctx = nctx
			}

			if hdrs, ok := middleware.(ClientHeadersMiddleware); ok {
				hdrmd := make(metadata.MD)
				trailermd := make(metadata.MD)
				opts = append(opts, grpc.Header(&hdrmd), grpc.Trailer(&trailermd))
				defer func() {
					hdrs.HeadersReceived(ctx, metadata.Join(hdrmd, trailermd))
				}()
			}

			err := invoker(ctx, method, req, reply, cc, opts...)
			if post, ok := middleware.(ClientPostCallMiddleware); ok {
				post.CallCompleted(ctx, err)
			}
			return err
		},
		Stream: func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			nctx := middleware.StartCall(ctx)
			if nctx != nil {
				ctx = nctx
			}

			cs, err := streamer(ctx, desc, cc, method, opts...)
			hdrs, isHdrs := middleware.(ClientHeadersMiddleware)
			post, isPostcall := middleware.(ClientPostCallMiddleware)
			if !isPostcall && !isHdrs {
				return cs, err
			}

			if err != nil {
				if isPostcall {
					post.CallCompleted(ctx, err)
				}
				return cs, err
			}

			// Grab the client stream context because when the finish function or the goroutine below will be
			// executed it's not guaranteed cs.Context() will be valid.
			csCtx := cs.Context()
			finishChan := make(chan struct{})
			isFinished := new(int32)
			*isFinished = 0
			finishFunc := func(err error) {

				// since there are multiple code paths that could call finishFunc
				// we need some sort of synchronization to guard against multiple
				// calls to finish
				if !atomic.CompareAndSwapInt32(isFinished, 0, 1) {
					return
				}

				close(finishChan)
				if isPostcall {
					post.CallCompleted(csCtx, err)
				}
				if isHdrs {
					hdrmd, _ := cs.Header()
					hdrs.HeadersReceived(csCtx, metadata.Join(hdrmd, cs.Trailer()))
				}
			}
			go func() {
				select {
				case <-finishChan:
					// finish is being called by something else, no action necessary
				case <-csCtx.Done():
					finishFunc(csCtx.Err())
				}
			}()

			newCS := &clientStream{
				ClientStream: cs,
				desc:         desc,
				finishFn:     finishFunc,
			}
			// The `ClientStream` interface allows one to omit calling `Recv` if it's
			// known that the result will be `io.EOF`. See
			// http://stackoverflow.com/q/42915337
			// In such cases, there's nothing that triggers the span to finish. We,
			// therefore, set a finalizer so that the span and the context goroutine will
			// at least be cleaned up when the garbage collector is run.
			runtime.SetFinalizer(newCS, func(newcs *clientStream) {
				newcs.finishFn(nil)
			})
			return newCS, nil
		},
	}
}

type clientStream struct {
	grpc.ClientStream
	desc     *grpc.StreamDesc
	finishFn func(error)
}

func (cs *clientStream) Header() (metadata.MD, error) {
	md, err := cs.ClientStream.Header()
	if err != nil {
		cs.finishFn(err)
	}
	return md, err
}

func (cs *clientStream) SendMsg(m interface{}) error {
	err := cs.ClientStream.SendMsg(m)
	if err != nil {
		cs.finishFn(err)
	}
	return err
}

func (cs *clientStream) RecvMsg(m interface{}) error {
	err := cs.ClientStream.RecvMsg(m)
	if errors.Is(err, io.EOF) {
		cs.finishFn(nil)
		return err
	} else if err != nil {
		cs.finishFn(err)
		return err
	}

	if !cs.desc.ServerStreams {
		cs.finishFn(nil)
	}
	return err
}

func (cs *clientStream) CloseSend() error {
	err := cs.ClientStream.CloseSend()
	if err != nil {
		cs.finishFn(err)
	}
	return err
}

type ClientMiddleware struct {
	Stream grpc.StreamClientInterceptor
	Unary  grpc.UnaryClientInterceptor
}

type client struct {
	conn        grpc.ClientConnInterface
	authHandler ClientAuthHandler

	FlightServiceClient
}

// NewFlightClient takes in the address of the grpc server and an auth handler for the
// application-level handshake. If using TLS or other grpc configurations they can still
// be passed via the grpc.DialOption list just as if connecting manually without this
// helper function.
//
// Alternatively, a grpc client can be constructed as normal without this helper as the
// grpc generated client code is still exported. This exists to add utility and helpers
// around the authentication and passing the token with requests.
//
// Deprecated: prefer to use NewClientWithMiddleware
func NewFlightClient(addr string, auth ClientAuthHandler, opts ...grpc.DialOption) (Client, error) {
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

	return &client{conn: conn, FlightServiceClient: flight.NewFlightServiceClient(conn), authHandler: auth}, nil
}

// NewClientWithMiddleware takes a slice of middlewares in addition to the auth and address which will be
// used by grpc and chained, the first middleware will be the outer most with the last middleware
// being the inner most wrapper around the actual call. It also passes along the dialoptions passed in such
// as TLS certs and so on.
func NewClientWithMiddleware(addr string, auth ClientAuthHandler, middleware []ClientMiddleware, opts ...grpc.DialOption) (Client, error) {
	return NewClientWithMiddlewareCtx(context.Background(), addr, auth, middleware, opts...)
}

func NewClientWithMiddlewareCtx(ctx context.Context, addr string, auth ClientAuthHandler, middleware []ClientMiddleware, opts ...grpc.DialOption) (Client, error) {
	unary := make([]grpc.UnaryClientInterceptor, 0, len(middleware))
	stream := make([]grpc.StreamClientInterceptor, 0, len(middleware))
	if auth != nil {
		unary = append(unary, createClientAuthUnaryInterceptor(auth))
		stream = append(stream, createClientAuthStreamInterceptor(auth))
	}
	if len(middleware) > 0 {
		for _, m := range middleware {
			if m.Unary != nil {
				unary = append(unary, m.Unary)
			}
			if m.Stream != nil {
				stream = append(stream, m.Stream)
			}
		}
	}
	opts = append(opts, grpc.WithChainUnaryInterceptor(unary...), grpc.WithChainStreamInterceptor(stream...))
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}

	return &client{conn: conn, FlightServiceClient: flight.NewFlightServiceClient(conn), authHandler: auth}, nil
}

func NewClientFromConn(cc grpc.ClientConnInterface, auth ClientAuthHandler) Client {
	return &client{conn: cc,
		FlightServiceClient: flight.NewFlightServiceClient(cc), authHandler: auth}
}

func (c *client) AuthenticateBasicToken(ctx context.Context, username, password string, opts ...grpc.CallOption) (context.Context, error) {
	authCtx := metadata.AppendToOutgoingContext(ctx, "Authorization", "Basic "+base64.RawStdEncoding.EncodeToString([]byte(strings.Join([]string{username, password}, ":"))))

	stream, err := c.FlightServiceClient.Handshake(authCtx, opts...)
	if err != nil {
		return ctx, err
	}

	header, err := stream.Header()
	if err != nil {
		return ctx, err
	}

	_, err = stream.Recv()
	if err != nil && err != io.EOF {
		return ctx, err
	}

	err = stream.CloseSend()
	if err != nil {
		return ctx, err
	}

	meta := stream.Trailer()
	md := metadata.Join(header, meta)
	for _, token := range md.Get("authorization") {
		if token != "" {
			return metadata.AppendToOutgoingContext(ctx, "Authorization", token), nil
		}
	}

	return ctx, fmt.Errorf("flight: no authorization header on the response")
}

func (c *client) Authenticate(ctx context.Context, opts ...grpc.CallOption) error {
	if c.authHandler == nil {
		return status.Error(codes.NotFound, "cannot authenticate without an auth-handler")
	}

	stream, err := c.FlightServiceClient.Handshake(ctx, opts...)
	if err != nil {
		return err
	}

	return c.authHandler.Authenticate(ctx, &clientAuthConn{stream})
}

// ReadUntilEOF will drain a stream until either an error is returned
// or EOF is encountered and nil is returned.
func ReadUntilEOF(stream FlightService_DoActionClient) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
}

func (c *client) CancelFlightInfo(ctx context.Context, request *CancelFlightInfoRequest, opts ...grpc.CallOption) (result CancelFlightInfoResult, err error) {
	var action flight.Action
	action.Type = CancelFlightInfoActionType
	action.Body, err = proto.Marshal(request)
	if err != nil {
		return
	}
	stream, err := c.DoAction(ctx, &action, opts...)
	if err != nil {
		return
	}
	res, err := stream.Recv()
	if err != nil {
		return
	}
	if err = proto.Unmarshal(res.Body, &result); err != nil {
		return
	}
	err = ReadUntilEOF(stream)
	return
}

func (c *client) Close() error {
	c.FlightServiceClient = nil
	if cl, ok := c.conn.(io.Closer); ok {
		return cl.Close()
	}
	return nil
}

func (c *client) RenewFlightEndpoint(ctx context.Context, request *RenewFlightEndpointRequest, opts ...grpc.CallOption) (*FlightEndpoint, error) {
	var err error
	var action flight.Action
	action.Type = RenewFlightEndpointActionType
	action.Body, err = proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	stream, err := c.DoAction(ctx, &action, opts...)
	if err != nil {
		return nil, err
	}
	res, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	var renewedEndpoint FlightEndpoint
	err = proto.Unmarshal(res.Body, &renewedEndpoint)
	if err != nil {
		return nil, err
	}
	err = ReadUntilEOF(stream)
	if err != nil {
		return nil, err
	}
	return &renewedEndpoint, nil
}
