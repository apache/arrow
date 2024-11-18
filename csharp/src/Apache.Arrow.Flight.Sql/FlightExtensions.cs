using System;
using System.Collections.Generic;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Apache.Arrow.Flight.Sql;

internal static class FlightExtensions
{
    public static byte[] PackAndSerialize(this IMessage command) => Any.Pack(command).ToByteArray();
    public static T ParseAndUnpack<T>(this ByteString source) where T : IMessage<T>, new() => Any.Parser.ParseFrom(source).Unpack<T>();

    public static IEnumerable<long> ExtractRowCount(this RecordBatch batch)
    {
        foreach (var array in batch.Arrays)
        {
            var values = ExtractValues(array);
            foreach (var value in values)
            {
                yield return value as long? ?? 0;
            }
        }
    }
    
    private static IEnumerable<object?> ExtractValues(IArrowArray array)
    {
        return array switch
        {
            Int32Array int32Array => ExtractPrimitiveValues(int32Array),
            Int64Array int64Array => ExtractPrimitiveValues(int64Array),
            FloatArray floatArray => ExtractPrimitiveValues(floatArray),
            BooleanArray booleanArray => ExtractBooleanValues(booleanArray),
            StringArray stringArray => ExtractStringValues(stringArray),
            _ => throw new NotSupportedException($"Array type {array.GetType().Name} is not supported.")
        };
    }
    
    private static IEnumerable<object?> ExtractPrimitiveValues<T>(PrimitiveArray<T> array) where T : struct, IEquatable<T>
    {
        for (int i = 0; i < array.Length; i++)
        {
            yield return array.IsNull(i) ? null : array.Values[i];
        }
    }
    
    private static IEnumerable<object?> ExtractBooleanValues(BooleanArray array)
    {
        for (int i = 0; i < array.Length; i++)
        {
            yield return array.IsNull(i) ? null : array.Values[i];
        }
    }
    
    private static IEnumerable<string?> ExtractStringValues(StringArray stringArray)
    {
        for (int i = 0; i < stringArray.Length; i++)
        {
            yield return stringArray.IsNull(i) ? null : stringArray.GetString(i);
        }
    }
}