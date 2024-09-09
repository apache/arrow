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
    }

    public MessageDescriptor Descriptor =>
        DescriptorReflection.Descriptor.MessageTypes[0];

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
        if (FlightInfo != null)
        {
            output.WriteTag(1, WireFormat.WireType.LengthDelimited);
            output.WriteString(FlightInfo.Descriptor.Command.ToStringUtf8());
            foreach (var path in FlightInfo.Descriptor.Paths)
            {
                output.WriteString(path);
            }
            output.WriteInt64(FlightInfo.TotalRecords);
            output.WriteInt64(FlightInfo.TotalBytes);
        }
    }

    public int CalculateSize()
    {
        int size = 0;

        if (FlightInfo != null)
        {
            // Manually compute the size of FlightInfo
            size += 1 + ComputeFlightInfoSize(FlightInfo);
        }

        return size;
    }

    public bool Equals(CancelFlightInfoRequest other) => other != null && FlightInfo.Equals(other.FlightInfo);
    public CancelFlightInfoRequest Clone() => new(FlightInfo);

    private int ComputeFlightInfoSize(FlightInfo flightInfo)
    {
        int size = 0;

        if (flightInfo.Descriptor != null)
        {
            size += CodedOutputStream.ComputeStringSize(flightInfo.Descriptor.Command.ToStringUtf8());
        }

        if (flightInfo.Descriptor?.Paths != null)
        {
            foreach (string? path in flightInfo.Descriptor.Paths)
            {
                size += CodedOutputStream.ComputeStringSize(path);
            }
        }

        // Compute size for other fields within FlightInfo
        size += CodedOutputStream.ComputeInt64Size(flightInfo.TotalRecords);
        size += CodedOutputStream.ComputeInt64Size(flightInfo.TotalBytes);

        return size;
    }
}
