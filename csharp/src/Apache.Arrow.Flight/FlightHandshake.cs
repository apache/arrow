using Google.Protobuf;

namespace Apache.Arrow.Flight;

public class FlightHandshake
{
    private readonly Protocol.HandshakeRequest _result;
    public ByteString Payload => _result.Payload;
    public ulong ProtocolVersion => _result.ProtocolVersion;

    internal FlightHandshake(Protocol.HandshakeRequest result)
    {
        _result = result;
    }

    public FlightHandshake(ByteString payload, ulong protocolVersion = 1)
    {
        _result = new Protocol.HandshakeRequest
        {
            Payload = payload,
            ProtocolVersion = protocolVersion
        };
    }

    internal Protocol.HandshakeRequest ToProtocol()
    {
        return _result;
    }

    public override bool Equals(object obj)
    {
        if(obj is FlightHandshake other)
        {
            return Equals(_result, other._result);
        }
        return false;
    }

    public override int GetHashCode()
    {
        return _result.GetHashCode();
    }
}
