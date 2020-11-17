using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;

namespace Apache.Arrow.Flight.Client
{
    public interface IFlightServer
    {
        Task DoPut(RecordBatchStreamReader requestStream, IAsyncStreamWriter<PutResult> responseStream, ServerCallContext context);

        Task DoGet(Ticket ticket, IServerStreamWriter<RecordBatch> responseStream, ServerCallContext context);

        Task ListFlights(Criteria request, IAsyncStreamWriter<FlightInfo> responseStream, ServerCallContext context);

        Task ListActions(IAsyncStreamWriter<ActionType> responseStream, ServerCallContext context);

        Task DoAction(Action request, IAsyncStreamWriter<Result> responseStream, ServerCallContext context);

        Task<Schema> GetSchema(FlightDescriptor request, ServerCallContext context);

        Task<FlightInfo> GetFlightInfo(FlightDescriptor request, ServerCallContext context);
    }
}
