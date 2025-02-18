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

using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Internal;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Flight.Server.Internal;
using Grpc.Core;

namespace Apache.Arrow.Flight.Client
{
    public class FlightClient
    {
        internal static readonly Empty EmptyInstance = new Empty();

        private readonly FlightService.FlightServiceClient _client;

        public FlightClient(ChannelBase grpcChannel)
        {
            _client = new FlightService.FlightServiceClient(grpcChannel);
        }

        public FlightClient(CallInvoker callInvoker)
        {
            _client = new FlightService.FlightServiceClient(callInvoker);
        }

        public AsyncServerStreamingCall<FlightInfo> ListFlights(FlightCriteria criteria = null, Metadata headers = null)
        {
            return ListFlights(criteria, headers, null, CancellationToken.None);
        }

        public AsyncServerStreamingCall<FlightInfo> ListFlights(FlightCriteria criteria, Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            if (criteria == null)
            {
                criteria = FlightCriteria.Empty;
            }

            var response = _client.ListFlights(criteria.ToProtocol(), headers, deadline, cancellationToken);
            var convertStream = new StreamReader<Protocol.FlightInfo, FlightInfo>(response.ResponseStream, inFlight => new FlightInfo(inFlight));

            return new AsyncServerStreamingCall<FlightInfo>(convertStream, response.ResponseHeadersAsync, response.GetStatus, response.GetTrailers, response.Dispose);
        }

        public AsyncServerStreamingCall<FlightActionType> ListActions(Metadata headers = null)
        {
            return ListActions(headers, null, CancellationToken.None);
        }

        public AsyncServerStreamingCall<FlightActionType> ListActions(Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var response = _client.ListActions(EmptyInstance, headers, deadline, cancellationToken);
            var convertStream = new StreamReader<Protocol.ActionType, FlightActionType>(response.ResponseStream, actionType => new FlightActionType(actionType));

            return new AsyncServerStreamingCall<FlightActionType>(convertStream, response.ResponseHeadersAsync, response.GetStatus, response.GetTrailers, response.Dispose);
        }

        public FlightRecordBatchStreamingCall GetStream(FlightTicket ticket, Metadata headers = null)
        {
            return GetStream(ticket, headers, null, CancellationToken.None);
        }

        public FlightRecordBatchStreamingCall GetStream(FlightTicket ticket, Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var stream = _client.DoGet(ticket.ToProtocol(), headers, deadline, cancellationToken);
            var responseStream = new FlightClientRecordBatchStreamReader(stream.ResponseStream);
            return new FlightRecordBatchStreamingCall(responseStream, stream.ResponseHeadersAsync, stream.GetStatus, stream.GetTrailers, stream.Dispose);
        }

        public AsyncUnaryCall<FlightInfo> GetInfo(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            return GetInfo(flightDescriptor, headers, null, CancellationToken.None);
        }

        public AsyncUnaryCall<FlightInfo> GetInfo(FlightDescriptor flightDescriptor, Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var flightInfoResult = _client.GetFlightInfoAsync(flightDescriptor.ToProtocol(), headers, deadline, cancellationToken);

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

        /// <summary>
        /// Start a Flight Put request.
        /// </summary>
        /// <param name="flightDescriptor">Descriptor for the data to be put</param>
        /// <param name="headers">gRPC headers to send with the request</param>
        /// <returns>A <see cref="FlightRecordBatchDuplexStreamingCall" /> object used to write data batches and receive responses</returns>
        public FlightRecordBatchDuplexStreamingCall StartPut(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            return StartPut(flightDescriptor, headers, null, CancellationToken.None);
        }

        /// <summary>
        /// Start a Flight Put request.
        /// </summary>
        /// <param name="flightDescriptor">Descriptor for the data to be put</param>
        /// <param name="schema">The schema of the data</param>
        /// <param name="headers">gRPC headers to send with the request</param>
        /// <returns>A <see cref="FlightRecordBatchDuplexStreamingCall" /> object used to write data batches and receive responses</returns>
        /// <remarks>Using this method rather than a StartPut overload that doesn't accept a schema
        /// means that the schema is sent even if no data batches are sent</remarks>
        public Task<FlightRecordBatchDuplexStreamingCall> StartPut(FlightDescriptor flightDescriptor, Schema schema, Metadata headers = null)
        {
            return StartPut(flightDescriptor, schema, headers, null, CancellationToken.None);
        }

        /// <summary>
        /// Start a Flight Put request.
        /// </summary>
        /// <param name="flightDescriptor">Descriptor for the data to be put</param>
        /// <param name="headers">gRPC headers to send with the request</param>
        /// <param name="deadline">Optional deadline. The request will be cancelled if this deadline is reached.</param>
        /// <param name="cancellationToken">Optional token for cancelling the request</param>
        /// <returns>A <see cref="FlightRecordBatchDuplexStreamingCall" /> object used to write data batches and receive responses</returns>
        public FlightRecordBatchDuplexStreamingCall StartPut(FlightDescriptor flightDescriptor, Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var channels = _client.DoPut(headers, deadline, cancellationToken);
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

        /// <summary>
        /// Start a Flight Put request.
        /// </summary>
        /// <param name="flightDescriptor">Descriptor for the data to be put</param>
        /// <param name="schema">The schema of the data</param>
        /// <param name="headers">gRPC headers to send with the request</param>
        /// <param name="deadline">Optional deadline. The request will be cancelled if this deadline is reached.</param>
        /// <param name="cancellationToken">Optional token for cancelling the request</param>
        /// <returns>A <see cref="FlightRecordBatchDuplexStreamingCall" /> object used to write data batches and receive responses</returns>
        /// <remarks>Using this method rather than a StartPut overload that doesn't accept a schema
        /// means that the schema is sent even if no data batches are sent</remarks>
        public async Task<FlightRecordBatchDuplexStreamingCall> StartPut(FlightDescriptor flightDescriptor, Schema schema, Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var channels = _client.DoPut(headers, deadline, cancellationToken);
            var requestStream = new FlightClientRecordBatchStreamWriter(channels.RequestStream, flightDescriptor);
            var readStream = new StreamReader<Protocol.PutResult, FlightPutResult>(channels.ResponseStream, putResult => new FlightPutResult(putResult));
            var streamingCall = new FlightRecordBatchDuplexStreamingCall(
                requestStream,
                readStream,
                channels.ResponseHeadersAsync,
                channels.GetStatus,
                channels.GetTrailers,
                channels.Dispose);
            await streamingCall.RequestStream.SetupStream(schema).ConfigureAwait(false);
            return streamingCall;
        }

        public AsyncDuplexStreamingCall<FlightHandshakeRequest, FlightHandshakeResponse> Handshake(Metadata headers = null)
        {
            return Handshake(headers, null, CancellationToken.None);

        }

        public AsyncDuplexStreamingCall<FlightHandshakeRequest, FlightHandshakeResponse> Handshake(Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var channel = _client.Handshake(headers, deadline, cancellationToken);
            var readStream = new StreamReader<HandshakeResponse, FlightHandshakeResponse>(channel.ResponseStream, response => new FlightHandshakeResponse(response));
            var writeStream = new FlightHandshakeStreamWriterAdapter(channel.RequestStream);
            var call = new AsyncDuplexStreamingCall<FlightHandshakeRequest, FlightHandshakeResponse>(
                writeStream,
                readStream,
                channel.ResponseHeadersAsync,
                channel.GetStatus,
                channel.GetTrailers,
                channel.Dispose);

            return call;
        }

        public FlightRecordBatchExchangeCall DoExchange(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            return DoExchange(flightDescriptor, headers, null, CancellationToken.None);
        }

        public FlightRecordBatchExchangeCall DoExchange(FlightDescriptor flightDescriptor, Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var channel = _client.DoExchange(headers, deadline, cancellationToken);
            var requestStream = new FlightClientRecordBatchStreamWriter(channel.RequestStream, flightDescriptor);
            var responseStream = new FlightClientRecordBatchStreamReader(channel.ResponseStream);
            var call = new FlightRecordBatchExchangeCall(
                requestStream,
                responseStream,
                channel.ResponseHeadersAsync,
                channel.GetStatus,
                channel.GetTrailers,
                channel.Dispose);

            return call;
        }

        public AsyncServerStreamingCall<FlightResult> DoAction(FlightAction action, Metadata headers = null)
        {
            return DoAction(action, headers, null, CancellationToken.None);
        }

        public AsyncServerStreamingCall<FlightResult> DoAction(FlightAction action, Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var stream = _client.DoAction(action.ToProtocol(), headers, deadline, cancellationToken);
            var streamReader = new StreamReader<Protocol.Result, FlightResult>(stream.ResponseStream, result => new FlightResult(result));
            return new AsyncServerStreamingCall<FlightResult>(streamReader, stream.ResponseHeadersAsync, stream.GetStatus, stream.GetTrailers, stream.Dispose);
        }

        public AsyncUnaryCall<Schema> GetSchema(FlightDescriptor flightDescriptor, Metadata headers = null)
        {
            return GetSchema(flightDescriptor, headers, null, CancellationToken.None);
        }

        public AsyncUnaryCall<Schema> GetSchema(FlightDescriptor flightDescriptor, Metadata headers, System.DateTime? deadline, CancellationToken cancellationToken = default)
        {
            var schemaResult = _client.GetSchemaAsync(flightDescriptor.ToProtocol(), headers, deadline, cancellationToken);

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
