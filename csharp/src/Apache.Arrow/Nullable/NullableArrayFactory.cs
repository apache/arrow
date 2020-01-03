namespace Apache.Arrow.Nullable
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using global::Apache.Arrow;

    public static class NullableArrayFactory
    {
        public static Int8Array CreateInt8(IEnumerable<sbyte?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new Int8Array(
             valueBuffer: new ArrowBuffer.Builder<sbyte>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }

        public static Int16Array CreateInt16(IEnumerable<short?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new Int16Array(
             valueBuffer: new ArrowBuffer.Builder<short>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }


        public static Int32Array CreateInt32(IEnumerable<int?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new Int32Array(
             valueBuffer: new ArrowBuffer.Builder<int>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }

        public static Int64Array CreateInt64(IEnumerable<long?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new Int64Array(
             valueBuffer: new ArrowBuffer.Builder<long>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }

        public static UInt8Array CreateIntU8(IEnumerable<byte?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new UInt8Array(
             valueBuffer: new ArrowBuffer.Builder<byte>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }

        public static UInt16Array CreateIntU16(IEnumerable<ushort?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new UInt16Array(
             valueBuffer: new ArrowBuffer.Builder<ushort>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }

        public static UInt32Array CreateUInt32(IEnumerable<uint?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new UInt32Array(
             valueBuffer: new ArrowBuffer.Builder<uint>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }

        public static UInt64Array CreateUInt64(IEnumerable<ulong?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new UInt64Array(
             valueBuffer: new ArrowBuffer.Builder<ulong>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }

        public static DoubleArray CreateDouble(IEnumerable<double?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new DoubleArray(
                valueBuffer: new ArrowBuffer.Builder<double>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
                nullBitmapBuffer: temp.Nulls.Build(),
                length: temp.Count,
                nullCount: temp.Nulls.NullCount,
                offset: 0);
        }

        public static FloatArray CreateFloat(IEnumerable<float?> values)
        {
            var temp = CreateNullMapForStruct(values);

            return new FloatArray(
                valueBuffer: new ArrowBuffer.Builder<float>().Reserve(temp.Values.Count).AppendRange(temp.Values).Build(),
                nullBitmapBuffer: temp.Nulls.Build(),
                length: temp.Count,
                nullCount: temp.Nulls.NullCount,
                offset: 0);
        }

        public static StringArray CreateString(IEnumerable<string> values)
        {
            var temp = CreateNullMapForClass(values, string.Empty);

            var conversionBuilder = new StringArray.Builder();

            var conversionData = conversionBuilder
                .Reserve(temp.Values.Count)
                .AppendRange(temp.Values)
                .Build();

            return new StringArray(
             valueOffsetsBuffer: conversionData.Data.Buffers[1],
             dataBuffer: conversionData.Data.Buffers[2],
             nullBitmapBuffer: temp.Nulls.Build(),
             length: temp.Count,
             nullCount: temp.Nulls.NullCount,
             offset: 0);
        }

        public static Date32Array CreateDate32(IEnumerable<DateTime?> values)
        {
            var temp = CreateNullMapForStruct(values);

            var conversionBuilder = new Date32Array.Builder();

            var conversionData = conversionBuilder
                .Reserve(temp.Values.Count)
                .AppendRange(temp.Values.Select(x => new DateTimeOffset(x)))
                .Build();

            var encodedValueBuffer = conversionData.Data.Buffers[1];

            return new Date32Array(
                    valueBuffer: encodedValueBuffer,
                    nullBitmapBuffer: temp.Nulls.Build(),
                    length: temp.Count,
                    nullCount: temp.Nulls.NullCount,
                    offset: 0
                );
        }

        public static Date64Array CreateDate64(IEnumerable<DateTimeOffset?> values)
        {
            var temp = CreateNullMapForStruct(values);

            var conversionBuilder = new Date64Array.Builder();

            var conversionData = conversionBuilder
                .Reserve(temp.Values.Count)
                .AppendRange(temp.Values)
                .Build();

            var encodedValueBuffer = conversionData.Data.Buffers[1];

            return new Date64Array(
                    valueBuffer: encodedValueBuffer,
                    nullBitmapBuffer: temp.Nulls.Build(),
                    length: temp.Count,
                    nullCount: temp.Nulls.NullCount,
                    offset: 0
                );
        }


        private static NullableCollection<T> CreateNullMapForStruct<T>(IEnumerable<T?> values)
            where T : struct
        {
            var temp = new NullableCollection<T>(values.Count());

            var index = 0;
            foreach (var v in values)
            {
                if (!v.HasValue)
                {
                    temp.Values.Add(default);
                }
                else
                {
                    temp.Nulls.SetValid(index);
                    temp.Values.Add(v.Value);
                }

                ++index;
            }

            return temp;
        }

        private static NullableCollection<T> CreateNullMapForClass<T>(IEnumerable<T> values, T defaultValue)
            where T : class
        {
            var temp = new NullableCollection<T>(values.Count());

            var index = 0;
            foreach (var v in values)
            {
                if (v == null)
                {
                    temp.Values.Add(defaultValue);
                }
                else
                {
                    temp.Nulls.SetValid(index);
                    temp.Values.Add(v);
                }

                ++index;
            }

            return temp;
        }
    }
}