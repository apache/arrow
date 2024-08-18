using System;
using Apache.Arrow.Flight;
using Google.Protobuf;
using Google.Protobuf.Reflection;

public sealed class CancelFlightInfoRequest : IMessage<CancelFlightInfoRequest>
{
    public FlightInfo FlightInfo { get; set; }

    // Overloaded constructor
    public CancelFlightInfoRequest(FlightInfo flightInfo) =>
        FlightInfo = flightInfo ?? throw new ArgumentNullException(nameof(flightInfo));


    public void MergeFrom(CancelFlightInfoRequest message)
    {
        if (message != null)
        {

        }
    }

    public void MergeFrom(CodedInputStream input)
    {
        while (input.ReadTag() != 0)
        {
            // Assuming FlightInfo is serialized as a field
        }
    }


    public void WriteTo(CodedOutputStream output)
    {
        if (FlightInfo != null)
        {
            output.WriteRawMessage(this);
        }
    }

    public int CalculateSize()
    {
        int size = 0;
        if (FlightInfo != null)
        {
            size += CodedOutputStream.ComputeMessageSize(this);
        }
        return size;
    }

    public MessageDescriptor Descriptor => null!;
    public bool Equals(CancelFlightInfoRequest other) => other != null && FlightInfo.Equals(other.FlightInfo);
    public CancelFlightInfoRequest Clone() => new CancelFlightInfoRequest(FlightInfo);
}
