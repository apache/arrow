using System;
using Apache.Arrow.Flight.Protocol;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Apache.Arrow.Flight;

public class FlightInfoCancelRequest : IMessage
{
    private readonly CancelFlightInfoRequest _cancelFlightInfoRequest;
    public FlightInfo FlightInfo { get; private set; }

    public FlightInfoCancelRequest(FlightInfo flightInfo)
    {
        FlightInfo = flightInfo ?? throw new ArgumentNullException(nameof(flightInfo));
        _cancelFlightInfoRequest = new CancelFlightInfoRequest();
    }

    public FlightInfoCancelRequest()
    {
        _cancelFlightInfoRequest = new CancelFlightInfoRequest();
    }

    public void MergeFrom(CodedInputStream input)
    {
        _cancelFlightInfoRequest.MergeFrom(input);
    }

    public void WriteTo(CodedOutputStream output)
    {
        _cancelFlightInfoRequest.WriteTo(output);
    }

    public int CalculateSize() => _cancelFlightInfoRequest.CalculateSize();

    public MessageDescriptor Descriptor =>
        DescriptorReflection.Descriptor.MessageTypes[0];
}
