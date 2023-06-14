using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Protocol;
using Google.Protobuf;
using Grpc.Core;

namespace Apache.Arrow.Flight.Server;

public class FlightHandshakeStreamReader
{
    private readonly IAsyncStreamReader<FlightHandshake> _requestStream;

    public FlightHandshakeStreamReader(IAsyncStreamReader<FlightHandshake> requestStream)
    {
        _requestStream = requestStream;
    }

    public ulong ProtocolVersion => _requestStream.Current.ProtocolVersion;
    public ByteString Payload => _requestStream.Current.Payload;

    public Task<bool> MoveNext()
    {
        return _requestStream.MoveNext();
    }
}
