using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql.TestWeb;

public class TestFlightSqlServer : FlightServer
{
    private readonly FlightSqlStore _flightStore;

    public TestFlightSqlServer(FlightSqlStore flightStore)
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
            case "GetPrimaryKeys":
                await responseStream.WriteAsync(new FlightResult("test data")).ConfigureAwait(false);
                break;
            case "CancelFlightInfo":
                var cancelRequest = new FlightInfoCancelResult();
                cancelRequest.SetStatus(1);
                await responseStream.WriteAsync(new FlightResult(Any.Pack(cancelRequest).Serialize().ToByteArray())).ConfigureAwait(false);
                break;
            case "BeginTransaction":
            case "Commit":
            case "Rollback":
                await responseStream.WriteAsync(new FlightResult(ByteString.CopyFromUtf8("sample-transaction-id"))).ConfigureAwait(false);
                break;
            case "CreatePreparedStatement":
            case "ClosePreparedStatement":
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
        var flightDescriptor = FlightDescriptor.CreateCommandDescriptor(ticket.Ticket.ToStringUtf8());

        if (_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
        {
            var batches = flightHolder.GetRecordBatches();
            foreach (var batch in batches)
            {
                await responseStream.WriteAsync(batch.RecordBatch, batch.Metadata).ConfigureAwait(false);
            }
        }
    }

    public override async Task DoPut(FlightServerRecordBatchStreamReader requestStream,
        IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context)
    {
        var flightDescriptor = await requestStream.FlightDescriptor;

        if (!_flightStore.Flights.TryGetValue(flightDescriptor, out var flightHolder))
        {
            flightHolder = new FlightSqlHolder(flightDescriptor, await requestStream.Schema, $"http://{context.Host}");
            _flightStore.Flights.Add(flightDescriptor, flightHolder);
        }

        while (await requestStream.MoveNext())
        {
            flightHolder.AddBatch(new RecordBatchWithMetadata(requestStream.Current,
                requestStream.ApplicationMetadata.FirstOrDefault()));
            await responseStream.WriteAsync(FlightPutResult.Empty).ConfigureAwait(false);
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

        if (_flightStore.Flights.Count > 0)
        {
            // todo: should rethink of the way to implement dynamic Flights search
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
        await responseStream.WriteAsync(new FlightActionType("commit", "commit a transaction")).ConfigureAwait(false);
        await responseStream.WriteAsync(new FlightActionType("rollback", "rollback a transaction")).ConfigureAwait(false);
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
