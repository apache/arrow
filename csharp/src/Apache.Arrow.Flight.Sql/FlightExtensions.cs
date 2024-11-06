using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Apache.Arrow.Flight.Sql;

internal static class FlightExtensions
{
    public static byte[] PackAndSerialize(this IMessage command) => Any.Pack(command).ToByteArray();
    public static T ParseAndUnpack<T>(this ByteString source) where T : IMessage<T>, new() => Any.Parser.ParseFrom(source).Unpack<T>();
}