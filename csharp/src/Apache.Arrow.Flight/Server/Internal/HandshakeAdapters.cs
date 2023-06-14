using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Protocol;
using Grpc.Core;

namespace Apache.Arrow.Flight.Server.Internal;

internal class FlightHandshakeStreamReaderAdaptor : IAsyncStreamReader<FlightHandshake>
{
    private readonly IAsyncStreamReader<HandshakeRequest> _requestStream;

    public FlightHandshakeStreamReaderAdaptor(IAsyncStreamReader<HandshakeRequest> requestStream)
    {
        _requestStream = requestStream;
    }

    public Task<bool> MoveNext(CancellationToken cancellationToken) => _requestStream.MoveNext();

    public FlightHandshake Current => new(_requestStream.Current);
}

internal class FlightHandshakeStreamWriterAdapter : IClientStreamWriter<FlightHandshake>
{
    private readonly IClientStreamWriter<HandshakeRequest> _writeStream;

    public FlightHandshakeStreamWriterAdapter(IClientStreamWriter<HandshakeRequest> writeStream)
    {
        _writeStream = writeStream;
    }

    public Task WriteAsync(FlightHandshake message) => _writeStream.WriteAsync(message.ToProtocol());

    public WriteOptions WriteOptions
    {
        get => _writeStream.WriteOptions;
        set => _writeStream.WriteOptions = value;
    }

    public Task CompleteAsync() => _writeStream.CompleteAsync();
}
