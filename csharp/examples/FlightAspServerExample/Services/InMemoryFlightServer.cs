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

using System.Collections.Concurrent;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Server;
using Grpc.Core;

namespace FlightAspServerExample.Services
{
    public class FlightData
    {
        public FlightData()
        {
            Flights = new ConcurrentDictionary<FlightTicket, FlightInfo> { };
            Tables = new ConcurrentDictionary<FlightTicket, List<RecordBatch>> { };
        }
        public IDictionary<FlightTicket, FlightInfo> Flights { get; }
        public IDictionary<FlightTicket, List<RecordBatch>> Tables { get; }
    }

    public class InMemoryFlightServer : FlightServer
    {
        private FlightData _flightData;

        public InMemoryFlightServer(FlightData flightData)
        {
            _flightData = flightData;
        }

        public override async Task DoPut(
            FlightServerRecordBatchStreamReader requestStream,
            IAsyncStreamWriter<FlightPutResult> responseStream,
            ServerCallContext context
        )
        {
            var newTable = new List<RecordBatch> { };
            long numRows = 0;

            await foreach (var batch in requestStream.ReadAllAsync(context.CancellationToken))
            {
                newTable.Add(batch);
                numRows += batch.Length;
            }

            var descriptor = await requestStream.FlightDescriptor;
            var ticket = DescriptorAsTicket(descriptor);
            var schema = await requestStream.Schema;

            _flightData.Flights.Add(ticket, new FlightInfo(
                schema,
                descriptor,
                new List<FlightEndpoint> { GetEndpoint(ticket, $"http://{context.Host}") },
                numRows,
                -1 // Unknown
            ));
            _flightData.Tables.Add(ticket, newTable);

            await responseStream.WriteAsync(new FlightPutResult("Table saved."));
        }

        public override async Task DoGet(
            FlightTicket ticket,
            FlightServerRecordBatchStreamWriter responseStream,
            ServerCallContext context
        )
        {
            if (!_flightData.Tables.ContainsKey(ticket))
            {
                throw new RpcException(new Status(StatusCode.NotFound, "Flight not found."));
            }
            var table = _flightData.Tables[ticket];

            foreach (var batch in table)
            {
                await responseStream.WriteAsync(batch);
            }
        }

        public override async Task ListFlights(
            FlightCriteria request,
            IAsyncStreamWriter<FlightInfo> responseStream,
            ServerCallContext context
        )
        {
            foreach (var flight in _flightData.Flights.Values)
            {
                await responseStream.WriteAsync(flight);
            }
        }

        public override Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context)
        {
            var key = DescriptorAsTicket(request);
            if (_flightData.Flights.ContainsKey(key))
            {
                return Task.FromResult(_flightData.Flights[key]);
            }
            else
            {
                throw new RpcException(new Status(StatusCode.NotFound, "Flight not found."));
            }
        }

        public override async Task ListActions(
            IAsyncStreamWriter<FlightActionType> responseStream, 
            ServerCallContext context
        )
        {
            await responseStream.WriteAsync(new FlightActionType("clear", "Clear the flights from the server"));
        }

        public override Task DoAction(
            FlightAction request, 
            IAsyncStreamWriter<FlightResult> responseStream, 
            ServerCallContext context
        )
        {
            if (request.Type == "clear")
            {
                _flightData.Flights.Clear();
                _flightData.Tables.Clear();
            }
            else
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Action does not exist."));
            }

            return Task.CompletedTask;
        }

        public override async Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context)
        {
            var info = await GetFlightInfo(request, context);
            return info.Schema;
        }

        private FlightTicket DescriptorAsTicket(FlightDescriptor desc)
        {
            return new FlightTicket(desc.Command);
        }

        private FlightEndpoint GetEndpoint(FlightTicket ticket, string host)
        {
            var location = new FlightLocation(host);
            return new FlightEndpoint(ticket, new List<FlightLocation> { location });
        }
    }
}