using Google.Protobuf;

namespace Apache.Arrow.Flight;

public class FlightHandshakeResponse
{
    public static readonly FlightHandshakeResponse Empty = new FlightHandshakeResponse();
    private readonly Protocol.HandshakeResponse _handshakeResponse;

    public ulong ProtocolVersion
    {
        get => _handshakeResponse.ProtocolVersion;
        set => _handshakeResponse.ProtocolVersion = value;
    }

    public ByteString Payload
    {
        get => _handshakeResponse.Payload;
        set => _handshakeResponse.Payload = value;
    }

    public FlightHandshakeResponse()
    {
        _handshakeResponse = new Protocol.HandshakeResponse
        {
            ProtocolVersion = 1
        };
    }

    internal FlightHandshakeResponse(Protocol.HandshakeResponse handshakeResponse)
    {
        _handshakeResponse = handshakeResponse;
    }

    public FlightHandshakeResponse(ByteString payload, ulong protocolVersion = 1)
    {
        _handshakeResponse = new Protocol.HandshakeResponse
        {
            ProtocolVersion = protocolVersion,
            Payload = payload
        };
    }

    internal Protocol.HandshakeResponse ToProtocol()
    {
        return _handshakeResponse;
    }
}
