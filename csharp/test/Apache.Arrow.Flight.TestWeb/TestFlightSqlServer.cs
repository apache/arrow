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
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Flight.Sql;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Apache.Arrow.Flight.TestWeb;

public class TestFlightSqlServer : FlightServer
{
    private readonly FlightStore _flightStore;

    public TestFlightSqlServer(FlightStore flightStore)
    {
        _flightStore = flightStore;
    }

    public override async Task DoAction(FlightAction request, IAsyncStreamWriter<FlightResult> responseStream,
        ServerCallContext context)
    {
        switch (request.Type)
        {
            case "test":
                await responseStream.WriteAsync(new FlightResult("test data")).ConfigureAwait(false);
                break;
            case SqlAction.GetPrimaryKeysRequest:
                await responseStream.WriteAsync(new FlightResult("test data")).ConfigureAwait(false);
                break;
            case SqlAction.CancelFlightInfoRequest:
                var cancelRequest = new FlightInfoCancelResult();
                cancelRequest.SetStatus(1);
                await responseStream.WriteAsync(new FlightResult(Any.Pack(cancelRequest).Serialize().ToByteArray()))
                    .ConfigureAwait(false);
                break;
            case SqlAction.BeginTransactionRequest:
            case SqlAction.CommitRequest:
            case SqlAction.RollbackRequest:
                await responseStream.WriteAsync(new FlightResult(ByteString.CopyFromUtf8("sample-transaction-id")))
                    .ConfigureAwait(false);
                break;
            case SqlAction.CreateRequest:
            case SqlAction.CloseRequest:
                var prepareStatementResponse = new ActionCreatePreparedStatementResult
                {
                    PreparedStatementHandle = ByteString.CopyFromUtf8("sample-testing-prepared-statement")
                };
                byte[] packedResult = Any.Pack(prepareStatementResponse).Serialize().ToByteArray();
                var flightResult = new FlightResult(packedResult);
                await responseStream.WriteAsync(flightResult).ConfigureAwait(false);
                break;
            default:
                throw new NotImplementedException();
        }
    }

    public override async Task DoGet(FlightTicket ticket, FlightServerRecordBatchStreamWriter responseStream,
        ServerCallContext context)
    {
        FlightDescriptor flightDescriptor = FlightDescriptor.CreateCommandDescriptor(ticket.Ticket.ToStringUtf8());

        if (_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
        {
            var batches = flightHolder.GetRecordBatches();

            foreach (var batch in batches)
            {
                await responseStream.WriteAsync(batch.RecordBatch, batch.Metadata);
            }
        }
    }
    
    public override async Task DoPut(FlightServerRecordBatchStreamReader requestStream,
        IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context)
    {
        var flightDescriptor = await requestStream.FlightDescriptor;

        if (!_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
        {
            flightHolder = new FlightHolder(flightDescriptor, await requestStream.Schema, $"http://{context.Host}");
            _flightStore.Flights.Add(flightDescriptor, flightHolder);
        }

        while (await requestStream.MoveNext())
        {
            flightHolder.AddBatch(new RecordBatchWithMetadata(requestStream.Current,
                requestStream.ApplicationMetadata.FirstOrDefault()));
            await responseStream.WriteAsync(FlightPutResult.Empty);
        }
    }
    
    public override Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context)
    {
        if (_flightStore.Flights.TryGetValue(request, out var flightHolder))
        {
            return Task.FromResult(flightHolder.GetFlightInfo());
        }

        if (_flightStore.Flights.Count > 0)
        {
            return Task.FromResult(_flightStore.Flights.First().Value.GetFlightInfo());
        }

        throw new RpcException(new Status(StatusCode.NotFound, "Flight not found"));
    }
    
    public override Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context)
    {
        if (_flightStore.Flights.TryGetValue(request, out var flightHolder))
        {
            return Task.FromResult(flightHolder.GetFlightInfo().Schema);
        }

        if (_flightStore.Flights.Count > 0)
        {
            return Task.FromResult(_flightStore.Flights.First().Value.GetFlightInfo().Schema);
        }

        throw new RpcException(new Status(StatusCode.NotFound, "Flight not found"));
    }
}


/*
 *
 *

   using System;
   using System.Collections.Generic;
   using System.Linq;
   using System.Threading.Tasks;
   using Apache.Arrow.Flight.Server;
   using Apache.Arrow.Flight.Sql;
   using Arrow.Flight.Protocol.Sql;
   using Google.Protobuf;
   using Google.Protobuf.WellKnownTypes;
   using Grpc.Core;
   using Grpc.Core.Utils;

   namespace Apache.Arrow.Flight.TestWeb
   {
       public class TestFlightServer : FlightServer
       {
           private readonly FlightStore _flightStore;

           public TestFlightServer(FlightStore flightStore)
           {
               _flightStore = flightStore;
           }

           public override async Task DoAction(FlightAction request, IAsyncStreamWriter<FlightResult> responseStream,
               ServerCallContext context)
           {
               switch (request.Type)
               {
                   case "test":
                       await responseStream.WriteAsync(new FlightResult("test data")).ConfigureAwait(false);
                       break;
                   case SqlAction.GetPrimaryKeysRequest:
                       await responseStream.WriteAsync(new FlightResult("test data")).ConfigureAwait(false);
                       break;
                   case SqlAction.CancelFlightInfoRequest:
                       var cancelRequest = new FlightInfoCancelResult();
                       cancelRequest.SetStatus(1);
                       await responseStream.WriteAsync(new FlightResult(Any.Pack(cancelRequest).Serialize().ToByteArray()))
                           .ConfigureAwait(false);
                       break;
                   case SqlAction.BeginTransactionRequest:
                   case SqlAction.CommitRequest:
                   case SqlAction.RollbackRequest:
                       await responseStream.WriteAsync(new FlightResult(ByteString.CopyFromUtf8("sample-transaction-id")))
                           .ConfigureAwait(false);
                       break;
                   case SqlAction.CreateRequest:
                   case SqlAction.CloseRequest:
                       var prepareStatementResponse = new ActionCreatePreparedStatementResult
                       {
                           PreparedStatementHandle = ByteString.CopyFromUtf8("sample-testing-prepared-statement")
                       };
                       byte[] packedResult = Any.Pack(prepareStatementResponse).Serialize().ToByteArray();
                       var flightResult = new FlightResult(packedResult);
                       await responseStream.WriteAsync(flightResult).ConfigureAwait(false);
                       break;
                   default:
                       throw new NotImplementedException();
               }
           }

           public override async Task DoGet(FlightTicket ticket, FlightServerRecordBatchStreamWriter responseStream,
               ServerCallContext context)
           {
               FlightDescriptor flightDescriptor = null;
               flightDescriptor = flightDescriptor is not null && flightDescriptor.Paths.Any()
                   ? FlightDescriptor.CreatePathDescriptor(ticket.Ticket.ToStringUtf8())
                   : FlightDescriptor.CreateCommandDescriptor(ticket.Ticket.ToStringUtf8());

               if (_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
               {
                   var batches = flightHolder.GetRecordBatches();


                   foreach (var batch in batches)
                   {
                       await responseStream.WriteAsync(batch.RecordBatch, batch.Metadata);
                   }
               }
           }

           public override async Task DoPut(FlightServerRecordBatchStreamReader requestStream,
               IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context)
           {
               var flightDescriptor = await requestStream.FlightDescriptor;

               if (!_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
               {
                   flightHolder = new FlightHolder(flightDescriptor, await requestStream.Schema, $"http://{context.Host}");
                   _flightStore.Flights.Add(flightDescriptor, flightHolder);
               }

               while (await requestStream.MoveNext())
               {
                   flightHolder.AddBatch(new RecordBatchWithMetadata(requestStream.Current,
                       requestStream.ApplicationMetadata.FirstOrDefault()));
                   await responseStream.WriteAsync(FlightPutResult.Empty);
               }
           }

           public override Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context)
           {
               if (_flightStore.Flights.TryGetValue(request, out var flightHolder))
               {
                   return Task.FromResult(flightHolder.GetFlightInfo());
               }

               if (_flightStore.Flights.Count > 0)
               {
                   return Task.FromResult(_flightStore.Flights.First().Value.GetFlightInfo());
               }

               throw new RpcException(new Status(StatusCode.NotFound, "Flight not found"));
           }

           public override async Task Handshake(IAsyncStreamReader<FlightHandshakeRequest> requestStream,
               IAsyncStreamWriter<FlightHandshakeResponse> responseStream, ServerCallContext context)
           {
               while (await requestStream.MoveNext().ConfigureAwait(false))
               {
                   if (requestStream.Current.Payload.ToStringUtf8() == "Hello")
                   {
                       await responseStream.WriteAsync(new(ByteString.CopyFromUtf8("Hello handshake")))
                           .ConfigureAwait(false);
                   }
                   else
                   {
                       await responseStream.WriteAsync(new(ByteString.CopyFromUtf8("Done"))).ConfigureAwait(false);
                   }
               }
           }

           public override Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context)
           {
               if (_flightStore.Flights.TryGetValue(request, out var flightHolder))
               {
                   return Task.FromResult(flightHolder.GetFlightInfo().Schema);
               }

               if (_flightStore.Flights.Count > 0)
               {
                   return Task.FromResult(_flightStore.Flights.First().Value.GetFlightInfo().Schema);
               }

               throw new RpcException(new Status(StatusCode.NotFound, "Flight not found"));
           }

           public override async Task ListActions(IAsyncStreamWriter<FlightActionType> responseStream,
               ServerCallContext context)
           {
               await responseStream.WriteAsync(new FlightActionType("get", "get a flight")).ConfigureAwait(false);
               await responseStream.WriteAsync(new FlightActionType("put", "add a flight")).ConfigureAwait(false);
               await responseStream.WriteAsync(new FlightActionType("delete", "delete a flight")).ConfigureAwait(false);
               await responseStream.WriteAsync(new FlightActionType("test", "test action")).ConfigureAwait(false);
           }

           public override async Task ListFlights(FlightCriteria request, IAsyncStreamWriter<FlightInfo> responseStream,
               ServerCallContext context)
           {
               var flightInfos = _flightStore.Flights.Select(x => x.Value.GetFlightInfo()).ToList();

               foreach (var flightInfo in flightInfos)
               {
                   await responseStream.WriteAsync(flightInfo).ConfigureAwait(false);
               }
           }

           public override async Task DoExchange(FlightServerRecordBatchStreamReader requestStream,
               FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context)
           {
               while (await requestStream.MoveNext().ConfigureAwait(false))
               {
                   await responseStream
                       .WriteAsync(requestStream.Current, requestStream.ApplicationMetadata.FirstOrDefault())
                       .ConfigureAwait(false);
               }
           }
       }
   }
 *
 */