using Apache.Arrow.Flight.Server;

namespace Apache.Arrow.Flight.Sql.TestWeb;

public class TestFlightSqlServer : FlightServer
{
}
/*
 *  public class TestFlightServer : FlightServer
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
                   await responseStream.WriteAsync(new FlightResult("test data"));
                   break;
               case "BeginTransaction":
               case "Commit":
               case "Rollback":
                   await responseStream.WriteAsync(new FlightResult(ByteString.CopyFromUtf8("sample-transaction-id")));
                   break;
               case "CreatePreparedStatement":
               case "ClosePreparedStatement":
                   var prepareStatementResponse = new ActionCreatePreparedStatementResult
                   {
                       PreparedStatementHandle = ByteString.CopyFromUtf8("sample-testing-prepared-statement")
                   };

                   var packedResult = Any.Pack(prepareStatementResponse).Serialize().ToByteArray();
                   var flightResult = new FlightResult(packedResult);
                   await responseStream.WriteAsync(flightResult);
                   break;
               default:
                   throw new NotImplementedException();
           }
       }

       public override async Task DoGet(FlightTicket ticket, FlightServerRecordBatchStreamWriter responseStream,
           ServerCallContext context)
       {
           // var flightDescriptor = FlightDescriptor.CreatePathDescriptor(ticket.Ticket.ToStringUtf8());
           var flightDescriptor = FlightDescriptor.CreateCommandDescriptor(ticket.Ticket.ToStringUtf8());

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
               // todo: should rethink of the way to implement dynamic Flights search
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

           throw new RpcException(new Status(StatusCode.NotFound, "Flight not found"));
       }

       public override async Task ListActions(IAsyncStreamWriter<FlightActionType> responseStream,
           ServerCallContext context)
       {
           await responseStream.WriteAsync(new FlightActionType("get", "get a flight"));
           await responseStream.WriteAsync(new FlightActionType("put", "add a flight"));
           await responseStream.WriteAsync(new FlightActionType("delete", "delete a flight"));
           await responseStream.WriteAsync(new FlightActionType("test", "test action"));
           await responseStream.WriteAsync(new FlightActionType("commit", "commit a transaction"));
           await responseStream.WriteAsync(new FlightActionType("rollback", "rollback a transaction"));
       }

       public override async Task ListFlights(FlightCriteria request, IAsyncStreamWriter<FlightInfo> responseStream,
           ServerCallContext context)
       {
           var flightInfos = _flightStore.Flights.Select(x => x.Value.GetFlightInfo()).ToList();

           foreach (var flightInfo in flightInfos)
           {
               await responseStream.WriteAsync(flightInfo);
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
 *
 *
 */
