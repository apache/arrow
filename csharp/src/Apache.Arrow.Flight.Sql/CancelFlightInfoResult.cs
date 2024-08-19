using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Apache.Arrow.Flight.Sql;

public enum CancelStatus
{
    Unspecified = 0,
    Cancelled = 1,
    Cancelling = 2,
    NotCancellable = 3,
    Unrecognized = -1
}

public sealed class CancelFlightInfoResult : IMessage<CancelFlightInfoResult>
{
    public CancelStatus CancelStatus { get; private set; }

    // Public parameterless constructor
    public CancelFlightInfoResult()
    {
        CancelStatus = CancelStatus.Unspecified;
        Descriptor =
            DescriptorReflection.Descriptor.MessageTypes[0];
    }

    public void MergeFrom(CancelFlightInfoResult message)
    {
        if (message != null)
        {
            CancelStatus = message.CancelStatus;
        }
    }

    public void MergeFrom(CodedInputStream input)
    {
        while (input.ReadTag() != 0)
        {
            switch (input.Position)
            {
                case 1:
                    CancelStatus = (CancelStatus)input.ReadEnum();
                    break;
                default:
                    input.SkipLastField();
                    break;
            }
        }
    }

    public void WriteTo(CodedOutputStream output)
    {
        if (CancelStatus != CancelStatus.Unspecified)
        {
            output.WriteRawTag(8); // Field number 1, wire type 0 (varint)
            output.WriteEnum((int)CancelStatus);
        }
    }

    public int CalculateSize()
    {
        int size = 0;
        if (CancelStatus != CancelStatus.Unspecified)
        {
            size += 1 + CodedOutputStream.ComputeEnumSize((int)CancelStatus);
        }

        return size;
    }

    public MessageDescriptor? Descriptor { get; }


    public CancelFlightInfoResult Clone() => new() { CancelStatus = CancelStatus };

    public bool Equals(CancelFlightInfoResult other)
    {
        if (other == null)
        {
            return false;
        }

        return CancelStatus == other.CancelStatus;
    }

    public override int GetHashCode()
    {
        return CancelStatus.GetHashCode();
    }

    public override string ToString()
    {
        return $"CancelFlightInfoResult {{ CancelStatus = {CancelStatus} }}";
    }
}
