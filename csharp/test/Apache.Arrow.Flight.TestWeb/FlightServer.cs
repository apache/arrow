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
using System.Linq;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Utils;

namespace Apache.Arrow.Flight.TestWeb
{
    public class FlightServer : IFlightServer
    {
        private readonly FlightStore _flightStore;

        public FlightServer(FlightStore flightStore)
        {
            _flightStore = flightStore;
        }

        public Task DoAction(Action request, IAsyncStreamWriter<Result> responseStream, ServerCallContext context)
        {
            throw new NotImplementedException();
        }

        public async Task DoGet(Ticket ticket, IServerStreamWriter<RecordBatch> responseStream, ServerCallContext context)
        {
            var flightDescriptor = FlightDescriptor.Path(ticket.TicketString);

            if(_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
            {
                var batches = flightHolder.GetRecordBatches();

                foreach(var batch in batches)
                {
                    await responseStream.WriteAsync(batch);
                }
            }
        }

        public async Task DoPut(RecordBatchStreamReader requestStream, IAsyncStreamWriter<PutResult> responseStream, ServerCallContext context)
        {
            var flightDescriptor = await requestStream.FlightDescriptor;

            if(!_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
            {
                flightHolder = new FlightHolder(flightDescriptor, await requestStream.Schema);
                _flightStore.Flights.Add(flightDescriptor, flightHolder);
            }

            var batches = await requestStream.ToListAsync();

            foreach(var batch in batches)
            {
                flightHolder.AddBatch(batch);
                await responseStream.WriteAsync(new PutResult(new ArrowBuffer()));
            }
        }

        public Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context)
        {
            if(_flightStore.Flights.TryGetValue(request, out var flightHolder))
            {
                return Task.FromResult(flightHolder.GetFlightInfo());
            }
            throw new RpcException(new Status(StatusCode.NotFound, "Flight not found"));
        }

        public Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context)
        {
            throw new NotImplementedException();
        }

        public async Task ListActions(IAsyncStreamWriter<ActionType> responseStream, ServerCallContext context)
        {
            await responseStream.WriteAsync(new ActionType("get", "get a flight"));
            await responseStream.WriteAsync(new ActionType("put", "add a flight"));
            await responseStream.WriteAsync(new ActionType("delete", "delete a flight"));
        }

        public Task ListFlights(Criteria request, IAsyncStreamWriter<FlightInfo> responseStream, ServerCallContext context)
        {
            throw new NotImplementedException();
        }
    }
}
