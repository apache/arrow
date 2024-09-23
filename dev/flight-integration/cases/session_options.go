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

package cases

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/apache/arrow/dev/flight-integration/flight"
	"github.com/apache/arrow/dev/flight-integration/scenario"
	"github.com/apache/arrow/dev/flight-integration/tester"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func init() {
	var (
		getSessionOptionsActionType = "GetSessionOptions"
		setSessionOptionsActionType = "SetSessionOptions"
		closeSessionActionType      = "CloseSession"

		keyFoolong = "foolong"
		valFoolong = int64(123)

		keyBardouble = "bardouble"
		valBardouble = 456.0

		keyLolInvalid = "lol_invalid"
		valLolInvalid = "this won't get set"

		keyKeyWithInvalidValue = "key_with_invalid_value"
		valKeyWithInvalidValue = "lol_invalid"

		keyBigOlStringList = "big_ol_string_list"
		valBigOlStringList = []string{"a", "b", "sea", "dee", " ", "  ", "geee", "(づ｡◕‿‿◕｡)づ"}

		errInvalidKey   flight.SetSessionOptionsResult_ErrorValue = 1
		errInvalidValue flight.SetSessionOptionsResult_ErrorValue = 2

		sessionValues = map[string]any{
			"foolong":                int64(123),
			"bardouble":              456.0,
			"lol_invalid":            "this won't get set",
			"key_with_invalid_value": "lol_invalid",
			"big_ol_string_list":     []string{"a", "b", "sea", "dee", " ", "  ", "geee", "(づ｡◕‿‿◕｡)づ"},
		}

		sessionCookieName  = "arrow_flight_session_id"
		sessionCookieValue = "session_1"
	)

	scenario.Register(
		scenario.Scenario{
			Name: "session_options",
			Steps: []scenario.ScenarioStep{
				{
					Name: "DoAction/GetSessionOptions/Initial",
					ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
						if a.GetType() != getSessionOptionsActionType {
							return status.Errorf(codes.InvalidArgument, "expected Action.Type to be: %s, found: %s", getSessionOptionsActionType, a.GetType())
						}

						var req flight.GetSessionOptionsRequest
						if err := deserializeProtobuf(a.GetBody(), &req); err != nil {
							return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
						}

						if cookie, err := getIncomingCookieByName(fs.Context(), sessionCookieName); err == nil {
							return status.Errorf(codes.Internal, "server expected no cookie for first request, found: %s", &cookie)
						}

						// client should hold on to this value, no need to send again unless the value changes
						if err := grpc.SetTrailer(fs.Context(), metadata.Pairs("Set-Cookie", fmt.Sprintf("%s=%s", sessionCookieName, sessionCookieValue))); err != nil {
							return status.Errorf(codes.Internal, "failed to set cookie in response trailer: %s", err)
						}

						body, err := serializeProtobuf(&flight.GetSessionOptionsResult{})
						if err != nil {
							return status.Errorf(codes.Internal, "failed to serialize GetSessionOptionsResult: %s", err)
						}

						return fs.Send(&flight.Result{Body: body})
					}},
				},
				{
					Name: "DoAction/SetSessionOptions/Initial",
					ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
						if a.GetType() != setSessionOptionsActionType {
							return status.Errorf(codes.InvalidArgument, "expected Action.Type to be: %s, found: %s", setSessionOptionsActionType, a.GetType())
						}

						var req flight.SetSessionOptionsRequest
						if err := deserializeProtobuf(a.GetBody(), &req); err != nil {
							return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
						}

						opts := req.GetSessionOptions()
						if len(opts) != len(sessionValues) {
							return status.Errorf(codes.InvalidArgument, "expected client to set %d options, found: %d", len(sessionValues), len(opts))
						}

						if opts[keyFoolong].GetInt64Value() != valFoolong {
							return status.Errorf(codes.InvalidArgument, "expected option %s to be set to %d, found: %s", keyFoolong, valFoolong, opts[keyFoolong])
						}

						if opts[keyBardouble].GetDoubleValue() != valBardouble {
							return status.Errorf(codes.InvalidArgument, "expected option %s to be set to %f, found: %s", keyBardouble, valBardouble, opts[keyBardouble])
						}

						if opts[keyLolInvalid].GetStringValue() != valLolInvalid {
							return status.Errorf(codes.InvalidArgument, "expected option %s to be set to %s, found: %s", keyLolInvalid, valLolInvalid, opts[keyLolInvalid])
						}

						if opts[keyKeyWithInvalidValue].GetStringValue() != valKeyWithInvalidValue {
							return status.Errorf(codes.InvalidArgument, "expected option %s to be set to %s, found: %s", keyKeyWithInvalidValue, valKeyWithInvalidValue, opts[keyKeyWithInvalidValue])
						}

						if !slices.Equal(opts[keyBigOlStringList].GetStringListValue().GetValues(), valBigOlStringList) {
							return status.Errorf(codes.InvalidArgument, "expected option %s to be set to %s, found: %s", keyBigOlStringList, valBigOlStringList, opts[keyBigOlStringList])
						}

						cookie, err := getIncomingCookieByName(fs.Context(), sessionCookieName)
						if err != nil {
							return status.Errorf(codes.Internal, "failed to get session cookie '%s': %s", sessionCookieName, err)
						}

						if cookie.Value != sessionCookieValue {
							return status.Errorf(codes.Internal, "expected session cookie to have value: %s, found: %s", sessionCookieValue, cookie.Value)
						}

						body, err := serializeProtobuf(&flight.SetSessionOptionsResult{
							Errors: map[string]*flight.SetSessionOptionsResult_Error{
								keyLolInvalid:          {Value: errInvalidKey},
								keyKeyWithInvalidValue: {Value: errInvalidValue},
							},
						})
						if err != nil {
							return status.Errorf(codes.Internal, "failed to serialize SetSessionOptionsResult: %s", err)
						}

						return fs.Send(&flight.Result{Body: body})
					}},
				},
				{
					Name: "DoAction/GetSessionOptions/AfterSetting",
					ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
						if a.GetType() != getSessionOptionsActionType {
							return status.Errorf(codes.InvalidArgument, "expected Action.Type to be: %s, found: %s", getSessionOptionsActionType, a.GetType())
						}

						var req flight.GetSessionOptionsRequest
						if err := deserializeProtobuf(a.GetBody(), &req); err != nil {
							return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
						}

						cookie, err := getIncomingCookieByName(fs.Context(), sessionCookieName)
						if err != nil {
							return status.Errorf(codes.Internal, "failed to get session cookie '%s': %s", sessionCookieName, err)
						}

						if cookie.Value != sessionCookieValue {
							return status.Errorf(codes.Internal, "expected session cookie to have value: %s, found: %s", sessionCookieValue, cookie.Value)
						}

						body, err := serializeProtobuf(&flight.GetSessionOptionsResult{
							SessionOptions: map[string]*flight.SessionOptionValue{
								keyFoolong:   {OptionValue: &flight.SessionOptionValue_Int64Value{Int64Value: valFoolong}},
								keyBardouble: {OptionValue: &flight.SessionOptionValue_DoubleValue{DoubleValue: valBardouble}},
								keyBigOlStringList: {
									OptionValue: &flight.SessionOptionValue_StringListValue_{
										StringListValue: &flight.SessionOptionValue_StringListValue{
											Values: valBigOlStringList,
										},
									},
								},
							},
						})
						if err != nil {
							return status.Errorf(codes.Internal, "failed to serialize GetSessionOptionsResult: %s", err)
						}

						return fs.Send(&flight.Result{Body: body})
					}},
				},
				{
					Name: "DoAction/SetSessionOptions/RemoveKey",
					ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
						if a.GetType() != setSessionOptionsActionType {
							return status.Errorf(codes.InvalidArgument, "expected Action.Type to be: %s, found: %s", setSessionOptionsActionType, a.GetType())
						}

						var req flight.SetSessionOptionsRequest
						if err := deserializeProtobuf(a.GetBody(), &req); err != nil {
							return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
						}

						opts := req.GetSessionOptions()
						if len(opts) != 1 {
							return status.Errorf(codes.InvalidArgument, "expected client to set %d options, found: %d", len(sessionValues), len(opts))
						}

						if opts[keyFoolong].OptionValue != nil {
							return status.Errorf(codes.InvalidArgument, "expected option %s to be nil, found: %s", keyFoolong, opts[keyFoolong])
						}

						cookie, err := getIncomingCookieByName(fs.Context(), sessionCookieName)
						if err != nil {
							return status.Errorf(codes.Internal, "failed to get session cookie '%s': %s", sessionCookieName, err)
						}

						if cookie.Value != sessionCookieValue {
							return status.Errorf(codes.Internal, "expected session cookie to have value: %s, found: %s", sessionCookieValue, cookie.Value)
						}

						body, err := serializeProtobuf(&flight.SetSessionOptionsResult{})
						if err != nil {
							return status.Errorf(codes.Internal, "failed to serialize SetSessionOptionsResult: %s", err)
						}

						return fs.Send(&flight.Result{Body: body})
					}},
				},
				{
					Name: "DoAction/GetSessionOptions/AfterRemoval",
					ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
						if a.GetType() != getSessionOptionsActionType {
							return status.Errorf(codes.InvalidArgument, "expected Action.Type to be: %s, found: %s", getSessionOptionsActionType, a.GetType())
						}

						var req flight.GetSessionOptionsRequest
						if err := deserializeProtobuf(a.GetBody(), &req); err != nil {
							return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
						}

						cookie, err := getIncomingCookieByName(fs.Context(), sessionCookieName)
						if err != nil {
							return status.Errorf(codes.Internal, "failed to get session cookie '%s': %s", sessionCookieName, err)
						}

						if cookie.Value != sessionCookieValue {
							return status.Errorf(codes.Internal, "expected session cookie to have value: %s, found: %s", sessionCookieValue, cookie.Value)
						}

						body, err := serializeProtobuf(&flight.GetSessionOptionsResult{
							SessionOptions: map[string]*flight.SessionOptionValue{
								keyBardouble: {OptionValue: &flight.SessionOptionValue_DoubleValue{DoubleValue: valBardouble}},
								keyBigOlStringList: {
									OptionValue: &flight.SessionOptionValue_StringListValue_{
										StringListValue: &flight.SessionOptionValue_StringListValue{
											Values: valBigOlStringList,
										},
									},
								},
							},
						})
						if err != nil {
							return status.Errorf(codes.Internal, "failed to serialize GetSessionOptionsResult: %s", err)
						}

						return fs.Send(&flight.Result{Body: body})
					}},
				},
				{
					Name: "DoAction/CloseSession",
					ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
						if a.GetType() != closeSessionActionType {
							return status.Errorf(codes.InvalidArgument, "expected Action.Type to be: %s, found: %s", closeSessionActionType, a.GetType())
						}

						var req flight.CloseSessionRequest
						if err := deserializeProtobuf(a.GetBody(), &req); err != nil {
							return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
						}

						cookie, err := getIncomingCookieByName(fs.Context(), sessionCookieName)
						if err != nil {
							return status.Errorf(codes.Internal, "failed to get session cookie '%s': %s", sessionCookieName, err)
						}

						if cookie.Value != sessionCookieValue {
							return status.Errorf(codes.Internal, "expected session cookie to have value: %s, found: %s", sessionCookieValue, cookie.Value)
						}

						body, err := serializeProtobuf(&flight.CloseSessionResult{Status: flight.CloseSessionResult_CLOSED})
						if err != nil {
							return status.Errorf(codes.Internal, "failed to serialize CloseSessionResult: %s", err)
						}

						return fs.Send(&flight.Result{Body: body})
					}},
				},
			},
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {
				{
					action, err := packAction(
						getSessionOptionsActionType,
						&flight.GetSessionOptionsRequest{},
						serializeProtobuf,
					)
					t.Require().NoError(err)

					var trailer metadata.MD
					stream, err := client.DoAction(ctx, action, grpc.Trailer(&trailer))
					t.Require().NoError(err)

					result, err := stream.Recv()
					t.Require().NoError(err)
					requireDrainStream(t, stream, nil)

					cookies := trailer.Get("Set-Cookie")
					t.Require().Len(cookies, 1)

					// persist cookie in context for subsequent client calls
					ctx = metadata.AppendToOutgoingContext(ctx, "Cookie", cookies[0])

					var resultPayload flight.GetSessionOptionsResult
					t.Require().NoError(deserializeProtobuf(result.Body, &resultPayload))

					t.Assert().Empty(resultPayload.GetSessionOptions())
				}

				{
					action, err := packAction(
						setSessionOptionsActionType,
						&flight.SetSessionOptionsRequest{SessionOptions: map[string]*flight.SessionOptionValue{
							keyFoolong:             {OptionValue: &flight.SessionOptionValue_Int64Value{Int64Value: valFoolong}},
							keyBardouble:           {OptionValue: &flight.SessionOptionValue_DoubleValue{DoubleValue: valBardouble}},
							keyLolInvalid:          {OptionValue: &flight.SessionOptionValue_StringValue{StringValue: valLolInvalid}},
							keyKeyWithInvalidValue: {OptionValue: &flight.SessionOptionValue_StringValue{StringValue: valKeyWithInvalidValue}},
							keyBigOlStringList:     {OptionValue: &flight.SessionOptionValue_StringListValue_{StringListValue: &flight.SessionOptionValue_StringListValue{Values: valBigOlStringList}}},
						}},
						serializeProtobuf,
					)
					t.Require().NoError(err)

					stream, err := client.DoAction(ctx, action)
					t.Require().NoError(err)

					result, err := stream.Recv()
					t.Require().NoError(err)
					requireDrainStream(t, stream, nil)

					var resultPayload flight.SetSessionOptionsResult
					t.Require().NoError(deserializeProtobuf(result.Body, &resultPayload))

					errs := resultPayload.GetErrors()
					t.Assert().Len(errs, 2)
					t.Assert().Equal(errs[keyLolInvalid], &flight.SetSessionOptionsResult_Error{Value: errInvalidKey})
					t.Assert().Equal(errs[keyKeyWithInvalidValue], &flight.SetSessionOptionsResult_Error{Value: errInvalidValue})
				}

				{
					action, err := packAction(
						getSessionOptionsActionType,
						&flight.GetSessionOptionsRequest{},
						serializeProtobuf,
					)
					t.Require().NoError(err)

					stream, err := client.DoAction(ctx, action)
					t.Require().NoError(err)

					result, err := stream.Recv()
					t.Require().NoError(err)
					requireDrainStream(t, stream, nil)

					var resultPayload flight.GetSessionOptionsResult
					t.Require().NoError(deserializeProtobuf(result.Body, &resultPayload))

					opts := resultPayload.GetSessionOptions()
					t.Assert().Len(opts, 3)
					t.Assert().Equal(valFoolong, opts[keyFoolong].GetInt64Value())
					t.Assert().Equal(valBardouble, opts[keyBardouble].GetDoubleValue())
					t.Assert().Equal(valBigOlStringList, opts[keyBigOlStringList].GetStringListValue().GetValues())
				}

				{
					action, err := packAction(
						setSessionOptionsActionType,
						&flight.SetSessionOptionsRequest{SessionOptions: map[string]*flight.SessionOptionValue{
							keyFoolong: {},
						}},
						serializeProtobuf,
					)
					t.Require().NoError(err)

					stream, err := client.DoAction(ctx, action)
					t.Require().NoError(err)

					result, err := stream.Recv()
					t.Require().NoError(err)
					requireDrainStream(t, stream, nil)

					var resultPayload flight.SetSessionOptionsResult
					t.Require().NoError(deserializeProtobuf(result.Body, &resultPayload))

					t.Assert().Empty(resultPayload.GetErrors())
				}

				{
					action, err := packAction(
						getSessionOptionsActionType,
						&flight.GetSessionOptionsRequest{},
						serializeProtobuf,
					)
					t.Require().NoError(err)

					stream, err := client.DoAction(ctx, action)
					t.Require().NoError(err)

					result, err := stream.Recv()
					t.Require().NoError(err)
					requireDrainStream(t, stream, nil)

					var resultPayload flight.GetSessionOptionsResult
					t.Require().NoError(deserializeProtobuf(result.Body, &resultPayload))

					opts := resultPayload.GetSessionOptions()
					t.Assert().Len(opts, 2)
					t.Assert().Equal(valBardouble, opts[keyBardouble].GetDoubleValue())
					t.Assert().Equal(valBigOlStringList, opts[keyBigOlStringList].GetStringListValue().GetValues())
				}

				{
					action, err := packAction(
						closeSessionActionType,
						&flight.CloseSessionRequest{},
						serializeProtobuf,
					)
					t.Require().NoError(err)

					stream, err := client.DoAction(ctx, action)
					t.Require().NoError(err)

					result, err := stream.Recv()
					t.Require().NoError(err)
					requireDrainStream(t, stream, nil)

					var resultPayload flight.CloseSessionResult
					t.Require().NoError(deserializeProtobuf(result.Body, &resultPayload))

					t.Assert().Equal(flight.CloseSessionResult_CLOSED, resultPayload.GetStatus())
				}
			},
		},
	)
}

func getIncomingCookieByName(ctx context.Context, name string) (http.Cookie, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return http.Cookie{}, fmt.Errorf("no metadata found for incoming context")
	}

	header := make(http.Header, md.Len())
	for k, v := range md {
		for _, val := range v {
			header.Add(k, val)
		}
	}

	cookie, err := (&http.Request{Header: header}).Cookie(name)
	if err != nil {
		return http.Cookie{}, err
	}

	if cookie == nil {
		return http.Cookie{}, fmt.Errorf("failed to get cookie with name: %s", name)
	}

	return *cookie, nil
}
