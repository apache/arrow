using Apache.Arrow.Flight.Client;
using Grpc.Core;

namespace Apache.Arrow.Flight.Sql;

public class DoPutResult
{
    public FlightClientRecordBatchStreamWriter Writer { get; }
    public IAsyncStreamReader<FlightPutResult> Reader { get; }

    public DoPutResult(FlightClientRecordBatchStreamWriter writer, IAsyncStreamReader<FlightPutResult> reader)
    {
        Writer = writer;
        Reader = reader;
    }
}
