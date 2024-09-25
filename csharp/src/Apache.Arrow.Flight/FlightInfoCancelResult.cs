using System;
using Apache.Arrow.Flight.Protocol;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Apache.Arrow.Flight;

public class FlightInfoCancelResult : IMessage
{
    private readonly CancelFlightInfoResult _flightInfoCancelResult;

    public FlightInfoCancelResult()
    {
        _flightInfoCancelResult = new CancelFlightInfoResult();
        Descriptor =
            DescriptorReflection.Descriptor.MessageTypes[0];
    }

    public void MergeFrom(CodedInputStream input) => _flightInfoCancelResult.MergeFrom(input);

    public void WriteTo(CodedOutputStream output) => _flightInfoCancelResult.WriteTo(output);

    public int CalculateSize()
    {
        return _flightInfoCancelResult.CalculateSize();
    }

    public MessageDescriptor Descriptor { get; }

    public int GetCancelStatus()
    {
        return (int)_flightInfoCancelResult.Status;
    }
}
