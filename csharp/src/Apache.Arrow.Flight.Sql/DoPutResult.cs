using System.Threading.Tasks;
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
    
    /// <summary>
    /// Reads the metadata asynchronously from the reader.
    /// </summary>
    /// <returns>A ByteString containing the metadata read from the reader.</returns>
    public async Task<Google.Protobuf.ByteString> ReadMetadataAsync()
    {
        if (await Reader.MoveNext().ConfigureAwait(false))
        {
            return Reader.Current.ApplicationMetadata;
        }
        throw new RpcException(new Status(StatusCode.Internal, "No metadata available in the response stream."));
    }
    
    /// <summary>
    /// Completes the writer by signaling the end of the writing process.
    /// </summary>
    /// <returns>A Task representing the completion of the writer.</returns>
    public async Task CompleteAsync()
    {
        await Writer.CompleteAsync().ConfigureAwait(false);
    }
}
