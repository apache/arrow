using System;
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
    public CancelStatus CancelStatus { get; }

    public void MergeFrom(CancelFlightInfoResult message) => throw new NotImplementedException();

    public void MergeFrom(CodedInputStream input) => throw new NotImplementedException();

    public void WriteTo(CodedOutputStream output) => throw new NotImplementedException();

    public int CalculateSize() => throw new NotImplementedException();

    public MessageDescriptor Descriptor { get; }
    public bool Equals(CancelFlightInfoResult other) => throw new NotImplementedException();

    public CancelFlightInfoResult Clone() => throw new NotImplementedException();
}
