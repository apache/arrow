using Google.Protobuf;

namespace Apache.Arrow.Flight;

public class FlightData
{
    public FlightDescriptor Descriptor { get; }
    public ByteString AppMetadata { get; }
    public ByteString DataBody { get; }
    public ByteString DataHeader { get; }

    public FlightData(FlightDescriptor descriptor, ByteString dataBody = null, ByteString dataHeader = null, ByteString appMetadata = null)
    {
        Descriptor = descriptor;
        DataBody = dataBody;
        DataHeader = dataHeader;
        AppMetadata = appMetadata;
    }

    internal FlightData(Protocol.FlightData protocolFlightData)
    {
        Descriptor = protocolFlightData.FlightDescriptor == null ? null : new FlightDescriptor(protocolFlightData.FlightDescriptor);
        DataBody = protocolFlightData.DataBody;
        DataHeader = protocolFlightData.DataHeader;
        AppMetadata = protocolFlightData.AppMetadata;
    }

    internal Protocol.FlightData ToProtocol()
    {
        return new Protocol.FlightData
        {
            FlightDescriptor = Descriptor?.ToProtocol(),
            AppMetadata = AppMetadata,
            DataBody = DataBody,
            DataHeader = DataHeader
        };
    }
}
