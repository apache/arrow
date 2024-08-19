using System;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Apache.Arrow.Flight.Sql;

public sealed class CancelFlightInfoRequest : IMessage<CancelFlightInfoRequest>
{
    public FlightInfo FlightInfo { get; set; }

    // Overloaded constructor
    public CancelFlightInfoRequest(FlightInfo flightInfo)
    {
        FlightInfo = flightInfo ?? throw new ArgumentNullException(nameof(flightInfo));
        Descriptor =
            DescriptorReflection.Descriptor.MessageTypes[0];
    }


    public void MergeFrom(CancelFlightInfoRequest message)
    {
        if (message != null)
        {
            FlightInfo = message.FlightInfo;
        }
    }

    public void MergeFrom(CodedInputStream input)
    {
        while (input.ReadTag() != 0)
        {
            switch (input.Position)
            {
                case 1:
                    input.ReadMessage(this);
                    break;
                default:
                    input.SkipLastField();
                    break;
            }
        }
    }

    public void WriteTo(CodedOutputStream output)
    {
        output.WriteTag(1, WireFormat.WireType.LengthDelimited);
        output.WriteMessage(FlightInfo.Descriptor.ParsedAndUnpackedMessage());
    }

    public int CalculateSize()
    {
        int size = 0;
        size += 1 + CodedOutputStream.ComputeMessageSize(FlightInfo.Descriptor.ParsedAndUnpackedMessage());
        return size;
    }

    public MessageDescriptor Descriptor { get; }

    public bool Equals(CancelFlightInfoRequest other) => other != null && FlightInfo.Equals(other.FlightInfo);
    public CancelFlightInfoRequest Clone() => new(FlightInfo);
}
