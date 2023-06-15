using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Protocol;
using Grpc.Core;

namespace Apache.Arrow.Flight.Server.Internal;

internal class FlightHandshakeStreamWriterAdapter : IClientStreamWriter<FlightHandshakeRequest>
{
    private readonly IClientStreamWriter<HandshakeRequest> _writeStream;

    public FlightHandshakeStreamWriterAdapter(IClientStreamWriter<HandshakeRequest> writeStream)
    {
        _writeStream = writeStream;
    }

    public Task WriteAsync(FlightHandshakeRequest message) => _writeStream.WriteAsync(message.ToProtocol());

    public WriteOptions WriteOptions
    {
        get => _writeStream.WriteOptions;
        set => _writeStream.WriteOptions = value;
    }

    public Task CompleteAsync() => _writeStream.CompleteAsync();
}
