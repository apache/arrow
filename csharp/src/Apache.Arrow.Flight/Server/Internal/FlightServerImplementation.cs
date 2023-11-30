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
using System.Threading.Tasks;
using Apache.Arrow.Flight.Internal;
using Apache.Arrow.Flight.Protocol;
using Grpc.Core;

namespace Apache.Arrow.Flight.Server.Internal
{
    /// <summary>
    /// This class has to be internal, since the generated code from proto is set as internal.
    /// </summary>
    internal class FlightServerImplementation : FlightService.FlightServiceBase
    {
        private readonly FlightServer _flightServer;
        public FlightServerImplementation(FlightServer flightServer)
        {
            _flightServer = flightServer;
        }

        public override async Task DoPut(IAsyncStreamReader<Protocol.FlightData> requestStream, IServerStreamWriter<Protocol.PutResult> responseStream, ServerCallContext context)
        {
            var readStream = new FlightServerRecordBatchStreamReader(requestStream);
            var writeStream = new StreamWriter<FlightPutResult, PutResult>(responseStream, putResult => putResult.ToProtocol());

            await _flightServer.DoPut(readStream, writeStream, context).ConfigureAwait(false);
        }

        public override Task DoGet(Protocol.Ticket request, IServerStreamWriter<Protocol.FlightData> responseStream, ServerCallContext context)
        {
            var flightTicket = new FlightTicket(request.Ticket_);
            var flightServerRecordBatchStreamWriter = new FlightServerRecordBatchStreamWriter(responseStream);

            return _flightServer.DoGet(flightTicket, flightServerRecordBatchStreamWriter, context);
        }

        public override Task ListFlights(Protocol.Criteria request, IServerStreamWriter<Protocol.FlightInfo> responseStream, ServerCallContext context)
        {
            var writeStream = new StreamWriter<FlightInfo, Protocol.FlightInfo>(responseStream, flightInfo => flightInfo.ToProtocol());

            return _flightServer.ListFlights(new FlightCriteria(request), writeStream, context);
        }

        public override Task DoAction(Protocol.Action request, IServerStreamWriter<Protocol.Result> responseStream, ServerCallContext context)
        {
            var action = new FlightAction(request);
            var writeStream = new StreamWriter<FlightResult, Protocol.Result>(responseStream, result => result.ToProtocol());

            return _flightServer.DoAction(action, writeStream, context);
        }

        public override async Task<SchemaResult> GetSchema(Protocol.FlightDescriptor request, ServerCallContext context)
        {
            var flightDescriptor = new FlightDescriptor(request);
            var schema = await _flightServer.GetSchema(flightDescriptor, context).ConfigureAwait(false);

            return new SchemaResult()
            {
                Schema = SchemaWriter.SerializeSchema(schema)
            };
        }

        public override async Task<Protocol.FlightInfo> GetFlightInfo(Protocol.FlightDescriptor request, ServerCallContext context)
        {
            var flightDescriptor = new FlightDescriptor(request);
            FlightInfo flightInfo = await _flightServer.GetFlightInfo(flightDescriptor, context).ConfigureAwait(false);

            return flightInfo.ToProtocol();
        }

        public override Task DoExchange(IAsyncStreamReader<Protocol.FlightData> requestStream, IServerStreamWriter<Protocol.FlightData> responseStream, ServerCallContext context)
        {
            //Exchange is not yet implemented
            throw new NotImplementedException();
        }

        public override Task Handshake(IAsyncStreamReader<HandshakeRequest> requestStream, IServerStreamWriter<HandshakeResponse> responseStream, ServerCallContext context)
        {
            var readStream = new StreamReader<HandshakeRequest, FlightHandshakeRequest>(requestStream, request => new FlightHandshakeRequest(request));
            var writeStream = new StreamWriter<FlightHandshakeResponse, Protocol.HandshakeResponse>(responseStream, result => result.ToProtocol());
            return _flightServer.Handshake(readStream, writeStream, context);
        }

        public override async Task ListActions(Empty request, IServerStreamWriter<Protocol.ActionType> responseStream, ServerCallContext context)
        {
            var writeStream = new StreamWriter<FlightActionType, Protocol.ActionType>(responseStream, (actionType) => actionType.ToProtocol());
            await _flightServer.ListActions(writeStream, context).ConfigureAwait(false);
        }
    }
}
