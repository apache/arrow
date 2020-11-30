// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Internal;
using Apache.Arrow.Flight.Protocol;
using Grpc.Core;
using Grpc.Net.Client;

namespace Apache.Arrow.Flight.Client
{
    public class FlightClient
    {
        internal static readonly Empty EmptyInstance = new Empty();

        private readonly FlightService.FlightServiceClient _client;

        public FlightClient(GrpcChannel grpcChannel)
        {
            _client = new FlightService.FlightServiceClient(grpcChannel);
        }

        public AsyncServerStreamingCall<FlightInfo> ListFlights(FlightCriteria criteria = null, Metadata headers = null)
        {
            if(criteria == null)
            {
                criteria = FlightCriteria.Empty;
            }
            
            var response = _client.ListFlights(criteria.ToProtocol(), headers);
            var convertStream = new StreamReader<Protocol.FlightInfo, FlightInfo>(response.ResponseStream, inFlight => new FlightInfo(inFlight));

            return new AsyncServerStreamingCall<FlightInfo>(convertStream, response.ResponseHeadersAsync, response.GetStatus, response.GetTrailers, response.Dispose);
        }

        public AsyncServerStreamingCall<FlightActionType> ListActions(Metadata headers = null)
        {
            var response = _client.ListActions(EmptyInstance, headers);
            var convertStream = new StreamReader<Protocol.ActionType, FlightActionType>(response.ResponseStream, actionType => new FlightActionType(actionType));

            return new AsyncServerStreamingCall<FlightActionType>(convertStream, response.ResponseHeadersAsync, response.GetStatus, response.GetTrailers, response.Dispose);
        }

        public FlightRecordBatchStreamingCall GetStream(FlightTicket ticket, Metadata headers = null)
        {
            var stream = _client.DoGet(ticket.ToProtocol(),  headers);
            var responseStream = new FlightClientRecordBatchStreamReader(stream.ResponseStream);
            return new FlightRecordBatchStreamingCall(responseStream, stream.ResponseHeadersAsync, stream.GetStatus, stream.GetTrailers, stream.Dispose);
        }

        public AsyncUnaryCall<FlightInfo> GetInfo(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            var flightInfoResult = _client.GetFlightInfoAsync(flightDescriptor.ToProtocol(), headers);

            var flightInfo = flightInfoResult
                .ResponseAsync
                .ContinueWith(async flightInfo => new FlightInfo(await flightInfo.ConfigureAwait(false)))
                .Unwrap();

            return new AsyncUnaryCall<FlightInfo>(
                flightInfo,
                flightInfoResult.ResponseHeadersAsync,
                flightInfoResult.GetStatus,
                flightInfoResult.GetTrailers,
                flightInfoResult.Dispose);
        }

        public FlightRecordBatchDuplexStreamingCall StartPut(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            var channels = _client.DoPut(headers);
            var requestStream = new FlightClientRecordBatchStreamWriter(channels.RequestStream, flightDescriptor);
            var readStream = new StreamReader<Protocol.PutResult, FlightPutResult>(channels.ResponseStream, putResult => new FlightPutResult(putResult));
            return new FlightRecordBatchDuplexStreamingCall(
                requestStream,
                readStream,
                channels.ResponseHeadersAsync,
                channels.GetStatus,
                channels.GetTrailers,
                channels.Dispose);
        }

        public AsyncServerStreamingCall<FlightResult> DoAction(FlightAction action, Metadata headers = null)
        {
            var stream = _client.DoAction(action.ToProtocol(), headers);
            var streamReader = new StreamReader<Protocol.Result, FlightResult>(stream.ResponseStream, result => new FlightResult(result));
            return new AsyncServerStreamingCall<FlightResult>(streamReader, stream.ResponseHeadersAsync, stream.GetStatus, stream.GetTrailers, stream.Dispose);
        }

        public AsyncUnaryCall<Schema> GetSchema(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            var schemaResult = _client.GetSchemaAsync(flightDescriptor.ToProtocol(), headers);

            var schema = schemaResult
                .ResponseAsync
                .ContinueWith(async schema => FlightMessageSerializer.DecodeSchema((await schemaResult.ResponseAsync.ConfigureAwait(false)).Schema.Memory))
                .Unwrap();

            return new AsyncUnaryCall<Schema>(
                schema,
                schemaResult.ResponseHeadersAsync,
                schemaResult.GetStatus,
                schemaResult.GetTrailers,
                schemaResult.Dispose);
        }
    }
}
