using System.Buffers;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Apache.Arrow.Flight.Sql.Tests;

public static class FlightSqlTestExtensions
{
    public static ByteString PackAndSerialize(this IMessage command)
    {
        return Any.Pack(command).Serialize();
    }
}
