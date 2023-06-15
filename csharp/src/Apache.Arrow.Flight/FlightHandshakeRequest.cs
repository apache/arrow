using Google.Protobuf;

namespace Apache.Arrow.Flight;

public class FlightHandshakeRequest
{
    private readonly Protocol.HandshakeRequest _result;
    public ByteString Payload => _result.Payload;
    public ulong ProtocolVersion => _result.ProtocolVersion;

    internal FlightHandshakeRequest(Protocol.HandshakeRequest result)
    {
        _result = result;
    }

    public FlightHandshakeRequest(ByteString payload, ulong protocolVersion = 1)
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
        if(obj is FlightHandshakeRequest other)
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
