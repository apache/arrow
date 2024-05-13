// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowArrayTests
    {

        [Fact]
        public void ThrowsWhenGetValueIndexOutOfBounds()
        {
            var array = new Int64Array.Builder().Append(1).Append(2).Build();
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetValue(-1));
            Assert.Equal(1, array.GetValue(0));
            Assert.Equal(2, array.GetValue(1));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetValue(2));
        }

        [Fact]
        public void ThrowsWhenGetValueAndOffsetIndexOutOfBounds()
        {
            var array = new BinaryArray.Builder().Append(1).Append(2).Build();
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetValueLength(-1));
            Assert.Equal(1, array.GetValueLength(0));
            Assert.Equal(1, array.GetValueLength(1));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetValueLength(2));

#pragma warning disable 618
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetValueOffset(-1));
            Assert.Equal(0, array.GetValueOffset(0));
            Assert.Equal(1, array.GetValueOffset(1));
            Assert.Equal(2, array.GetValueOffset(2));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetValueOffset(3));
#pragma warning restore 618

            Assert.Throws<IndexOutOfRangeException>(() => array.ValueOffsets[-1]);
            Assert.Equal(0, array.ValueOffsets[0]);
            Assert.Equal(1, array.ValueOffsets[1]);
            Assert.Equal(2, array.ValueOffsets[2]);
            Assert.Throws<IndexOutOfRangeException>(() => array.ValueOffsets[3]);

        }

        [Fact]
        public void IsValidValue()
        {
            const int totalValueCount = 8;
            const byte nullBitmap = 0b_11110011;

            var nullBitmapBuffer = new ArrowBuffer.Builder<byte>().Append(nullBitmap).Build();
            var valueBuffer = new ArrowBuffer.Builder<long>().Append(0).Append(1).Append(4).Append(5).Append(6).Append(7).Append(8).Build();

            //Check all offset and length
            for (var offset = 0; offset < totalValueCount; offset++)
            {
                var nullCount = totalValueCount - offset - BitUtility.CountBits(nullBitmapBuffer.Span, offset);
                for (var length = 1; length + offset < totalValueCount; length++)
                {
                    TestIsValid(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
                }
            }

            void TestIsValid(ArrowBuffer valueBuf, ArrowBuffer nullBitmapBuf, int length, int nullCount, int offset)
            {
                var array = new Int64Array(valueBuf, nullBitmapBuf, length, nullCount, offset);
                for (var i = 0; i < length; i++)
                {
                    if (BitUtility.GetBit(nullBitmap, i + offset))
                    {
                        Assert.True(array.IsValid(i));
                    }
                    else
                    {
                        Assert.False(array.IsValid(i));
                    }
                }
            }
        }

        [Fact]
        public void EnumerateArray()
        {
            var array = new Int64Array.Builder().Append(1).Append(2).Build();

            foreach(long? foo in array)
            {
                Assert.InRange(foo!.Value, 1, 2);
            }

            foreach (object foo in (IEnumerable)array)
            {
                Assert.InRange((long)foo, 1, 2);
            }
        }

        [Fact]
        public void ArrayAsReadOnlyList()
        {
            TestArrayAsReadOnlyList<long, Int64Array, Int64Array.Builder>([1, 2]);
            TestArrayAsReadOnlyList<byte, UInt8Array, UInt8Array.Builder>([1, 2]);
            TestArrayAsReadOnlyList<bool, BooleanArray, BooleanArray.Builder>([true, false]);
            TestArrayAsReadOnlyList<DateTime, Date32Array, Date32Array.Builder>([DateTime.MinValue.Date, DateTime.MaxValue.Date]);
            TestArrayAsReadOnlyList<DateTime, Date64Array, Date64Array.Builder>([DateTime.MinValue.Date, DateTime.MaxValue.Date]);
            TestArrayAsReadOnlyList<DateTimeOffset, TimestampArray, TimestampArray.Builder>([DateTimeOffset.MinValue, DateTimeOffset.MinValue.AddYears(100)]);

#if NET5_0_OR_GREATER
            TestArrayAsReadOnlyList<DateOnly, Date32Array, Date32Array.Builder>([DateOnly.MinValue, DateOnly.MaxValue]);
            TestArrayAsReadOnlyList<DateOnly, Date64Array, Date64Array.Builder>([DateOnly.MinValue, DateOnly.MaxValue]);
            TestArrayAsReadOnlyList<TimeOnly, Time32Array, Time32Array.Builder>([TimeOnly.MinValue, TimeOnly.MinValue.AddHours(23)]);
            TestArrayAsReadOnlyList<TimeOnly, Time64Array, Time64Array.Builder>([TimeOnly.MinValue, TimeOnly.MaxValue]);
            TestArrayAsReadOnlyList<Half, HalfFloatArray, HalfFloatArray.Builder>([(Half)1.1, (Half)2.2f]);
#endif
        }

        // Parameter 'values' must contain two distinct values
        private static void TestArrayAsReadOnlyList<T, TArray, TArrayBuilder>(IReadOnlyList<T> values)
            where T : struct
            where TArray : IArrowArray
            where TArrayBuilder : IArrowArrayBuilder<T, TArray, TArrayBuilder>, new()
        {
            Assert.Equal(2, values.Count);
            TArray array = new TArrayBuilder().Append(values[0]).AppendNull().Append(values[1]).Build(default);
            Assert.NotNull(array);
            var readOnlyList = (IReadOnlyList<T?>)array;

            Assert.Equal(array.Length, readOnlyList.Count);
            Assert.Equal(3, readOnlyList.Count);
            Assert.Equal(values[0], readOnlyList[0]);
            Assert.Null(readOnlyList[1]);
            Assert.Equal(values[1], readOnlyList[2]);
        }

        [Fact]
        public void ArrayAsCollection()
        {
            TestPrimitiveArrayAsCollection<long, Int64Array, Int64Array.Builder>([1, 2, 3, 4]);
            TestPrimitiveArrayAsCollection<byte, UInt8Array, UInt8Array.Builder>([1, 2, 3, 4]);
            TestPrimitiveArrayAsCollection<bool, BooleanArray, BooleanArray.Builder>([true, true, true, false]);
            TestPrimitiveArrayAsCollection<DateTime, Date32Array, Date32Array.Builder>([DateTime.MinValue.Date, DateTime.MaxValue.Date, DateTime.Today, DateTime.Today]);
            TestPrimitiveArrayAsCollection<DateTime, Date64Array, Date64Array.Builder>([DateTime.MinValue.Date, DateTime.MaxValue.Date, DateTime.Today, DateTime.Today]);
            TestPrimitiveArrayAsCollection<DateTimeOffset, TimestampArray, TimestampArray.Builder>([DateTimeOffset.MinValue, DateTimeOffset.MinValue.AddYears(100), DateTimeOffset.Now, DateTimeOffset.UtcNow]);

#if NET5_0_OR_GREATER
            TestPrimitiveArrayAsCollection<DateOnly, Date32Array, Date32Array.Builder>([DateOnly.MinValue, DateOnly.MaxValue, DateOnly.FromDayNumber(1), DateOnly.FromDayNumber(2)]);
            TestPrimitiveArrayAsCollection<DateOnly, Date64Array, Date64Array.Builder>([DateOnly.MinValue, DateOnly.MaxValue, DateOnly.FromDayNumber(1), DateOnly.FromDayNumber(2)]);
            TestPrimitiveArrayAsCollection<TimeOnly, Time32Array, Time32Array.Builder>([TimeOnly.MinValue, TimeOnly.MinValue.AddHours(23), TimeOnly.MinValue.AddHours(1), TimeOnly.MinValue.AddHours(2)]);
            TestPrimitiveArrayAsCollection<TimeOnly, Time64Array, Time64Array.Builder>([TimeOnly.MinValue, TimeOnly.MaxValue, TimeOnly.MinValue.AddHours(1), TimeOnly.MinValue.AddHours(2)]);
            TestPrimitiveArrayAsCollection<Half, HalfFloatArray, HalfFloatArray.Builder>([(Half)1.1, (Half)2.2f, (Half)3.3f, (Half)4.4f]);
#endif

            byte[][] byteArrs = [new byte[1], [], [255], new byte[2]];
            TestObjectArrayAsCollection(new BinaryArray.Builder().Append(byteArrs[0].AsEnumerable()).AppendNull().Append(byteArrs[1].AsEnumerable()).Append(byteArrs[0].AsEnumerable()).Build(), System.Array.Empty<byte>(), byteArrs);

            string[] strings = ["abc", "abd", "acd", "adc"];
            TestObjectArrayAsCollection(new StringArray.Builder().Append(strings[0]).AppendNull().Append(strings[1]).Append(strings[0]).Build(), null, strings);
        }

        // Parameter 'values' must contain four values. The last value must be distinct from the rest.
        private static void TestPrimitiveArrayAsCollection<T, TArray, TArrayBuilder>(IReadOnlyList<T> values)
            where T : struct
            where TArray : IArrowArray, ICollection<T?>
            where TArrayBuilder : IArrowArrayBuilder<T, TArray, TArrayBuilder>, new()
        {
            Assert.Equal(4, values.Count);
            TArray array = new TArrayBuilder().Append(values[0]).AppendNull().Append(values[1]).Append(values[0]).Build(default);
            Assert.NotNull(array);
            var collection = (ICollection<T?>)array;

            Assert.Equal(array.Length, collection.Count);
            Assert.Equal(4, collection.Count);
            Assert.True(collection.IsReadOnly);

            Assert.Equal("Collection is read-only.", Assert.Throws<NotSupportedException>(() => collection.Add(values[3])).Message);
            Assert.Equal("Collection is read-only.", Assert.Throws<NotSupportedException>(() => collection.Remove(values[3])).Message);
            Assert.Equal("Collection is read-only.", Assert.Throws<NotSupportedException>(collection.Clear).Message);

            Assert.True(collection.Contains(values[0]));
            Assert.True(collection.Contains(values[1]));
            Assert.True(collection.Contains(default));
            Assert.False(collection.Contains(values[3]));

            T sentinel = values[2];
            T?[] destArr = { sentinel, sentinel, sentinel, sentinel, sentinel, sentinel };
            collection.CopyTo(destArr, 1);
            Assert.Equal(sentinel, destArr[0]);
            Assert.Equal(values[0], destArr[1]);
            Assert.Null(destArr[2]);
            Assert.Equal(values[1], destArr[3]);
            Assert.Equal(values[0], destArr[4]);
            Assert.Equal(sentinel, destArr[0]);
        }

        // Parameter 'values' must contain four values. The last value must be distinct from the rest.
        private static void TestObjectArrayAsCollection<T, TArray>(TArray array, T nullValue, IReadOnlyList<T> values)
            where T : class
            where TArray : IArrowArray, ICollection<T?>
        {
            Assert.NotNull(array);
            Assert.Equal(4, values.Count);
            var collection = (ICollection<T?>)array;

            Assert.Equal(array.Length, collection.Count);
            Assert.Equal(4, collection.Count);
            Assert.True(collection.IsReadOnly);

            Assert.Equal("Collection is read-only.", Assert.Throws<NotSupportedException>(() => collection.Add(values[3])).Message);
            Assert.Equal("Collection is read-only.", Assert.Throws<NotSupportedException>(() => collection.Remove(values[3])).Message);
            Assert.Equal("Collection is read-only.", Assert.Throws<NotSupportedException>(collection.Clear).Message);

            Assert.True(collection.Contains(values[0]));
            Assert.True(collection.Contains(values[1]));
            Assert.True(collection.Contains(default));
            Assert.False(collection.Contains(values[3]));

            T sentinel = values[2];
            T?[] destArr = { sentinel, sentinel, sentinel, sentinel, sentinel, sentinel };
            collection.CopyTo(destArr, 1);
            Assert.Equal(sentinel, destArr[0]);
            Assert.Equal(values[0], destArr[1]);
            Assert.Equal(nullValue, destArr[2]);
            Assert.Equal(values[1], destArr[3]);
            Assert.Equal(values[0], destArr[4]);
            Assert.Equal(sentinel, destArr[0]);
        }

        [Fact]
        public void ContainsDoesNotMatchDefaultValueInArrayWithNullValue()
        {
            Int64Array array = new Int64Array.Builder().Append(1).Append(2).AppendNull().Build();
            Assert.NotNull(array);
            var collection = (ICollection<long?>)array;

            Assert.True(collection.Contains(1));
            Assert.True(collection.Contains(2));
            Assert.True(collection.Contains(default));
            // A null value is stored as a null bit in the null bitmap, and a default value in the value buffer. Check that we do not match the default value.
            Assert.False(collection.Contains(0));
        }

        [Fact]
        public void RecursiveArraySlice()
        {
            var initialValues = Enumerable.Range(0, 100).ToArray();
            var array = new Int32Array.Builder().AppendRange(initialValues).Build();

            var sliced = (Int32Array) array.Slice(20, 30);
            var slicedAgain = (Int32Array) sliced.Slice(5, 10);

            Assert.Equal(25, slicedAgain.Offset);
            Assert.Equal(10, slicedAgain.Length);
            Assert.Equal(
                initialValues.Skip(25).Take(10).Select(val => (int?) val).ToArray(),
                (IReadOnlyList<int?>) slicedAgain);
        }

#if NET5_0_OR_GREATER
        [Fact]
        public void SliceArray()
        {
            TestNumberSlice<int, Int32Array, Int32Array.Builder>();
            TestNumberSlice<sbyte, Int8Array, Int8Array.Builder>();
            TestNumberSlice<short, Int16Array, Int16Array.Builder>();
            TestNumberSlice<long, Int64Array, Int64Array.Builder>();
            TestNumberSlice<byte, UInt8Array, UInt8Array.Builder>();
            TestNumberSlice<ushort, UInt16Array, UInt16Array.Builder>();
            TestNumberSlice<uint, UInt32Array, UInt32Array.Builder>();
            TestNumberSlice<ulong, UInt64Array, UInt64Array.Builder>();
            TestNumberSlice<Half, HalfFloatArray, HalfFloatArray.Builder>();
            TestNumberSlice<float, FloatArray, FloatArray.Builder>();
            TestNumberSlice<double, DoubleArray, DoubleArray.Builder>();
            TestSlice<Date32Array, Date32Array.Builder>(x => x.Append(new DateTime(2019, 1, 1)).Append(new DateTime(2019, 1, 2)).Append(new DateTime(2019, 1, 3)));
            TestSlice<Date64Array, Date64Array.Builder>(x => x.Append(new DateTime(2019, 1, 1)).Append(new DateTime(2019, 1, 2)).Append(new DateTime(2019, 1, 3)));
            TestSlice<Time32Array, Time32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<Time64Array, Time64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<StringArray, StringArray.Builder>(x => x.Append("10").Append("20").Append("30"));

            static void TestNumberSlice<T, TArray, TBuilder>()
                where T : struct, INumber<T>
                where TArray : PrimitiveArray<T>
                where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>, new() =>
                TestSlice<TArray, TBuilder>(x => x.Append(T.CreateChecked(10)).Append(T.CreateChecked(20)).Append(T.CreateChecked(30)));
        }

        [Fact]
        public void SlicePrimitiveArrayWithNulls()
        {
            TestNumberSlice<int, Int32Array, Int32Array.Builder>();
            TestNumberSlice<sbyte, Int8Array, Int8Array.Builder>();
            TestNumberSlice<short, Int16Array, Int16Array.Builder>();
            TestNumberSlice<long, Int64Array, Int64Array.Builder>();
            TestNumberSlice<byte, UInt8Array, UInt8Array.Builder>();
            TestNumberSlice<ushort, UInt16Array, UInt16Array.Builder>();
            TestNumberSlice<uint, UInt32Array, UInt32Array.Builder>();
            TestNumberSlice<ulong, UInt64Array, UInt64Array.Builder>();
            TestNumberSlice<Half, HalfFloatArray, HalfFloatArray.Builder>();
            TestNumberSlice<float, FloatArray, FloatArray.Builder>();
            TestNumberSlice<double, DoubleArray, DoubleArray.Builder>();
            TestSlice<Date32Array, Date32Array.Builder>(x => x.Append(new DateTime(2019, 1, 1)).Append(new DateTime(2019, 1, 2)).AppendNull().Append(new DateTime(2019, 1, 3)));
            TestSlice<Date64Array, Date64Array.Builder>(x => x.Append(new DateTime(2019, 1, 1)).Append(new DateTime(2019, 1, 2)).AppendNull().Append(new DateTime(2019, 1, 3)));
            TestSlice<Time32Array, Time32Array.Builder>(x => x.Append(10).Append(20).AppendNull().Append(30));
            TestSlice<Time64Array, Time64Array.Builder>(x => x.Append(10).Append(20).AppendNull().Append(30));
            TestSlice<Int32Array, Int32Array.Builder>(x => x.AppendNull().AppendNull().AppendNull());  // All nulls

            static void TestNumberSlice<T, TArray, TBuilder>()
                where T : struct, INumber<T>
                where TArray : PrimitiveArray<T>
                where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>, new() =>
                TestSlice<TArray, TBuilder>(x => x.AppendNull().Append(T.CreateChecked(10)).Append(T.CreateChecked(20)).AppendNull().Append(T.CreateChecked(30)));
        }
#endif

        [Fact]
        public void SliceBooleanArray()
        {
            TestSlice<BooleanArray, BooleanArray.Builder>(x => x.Append(true).Append(false).Append(true));
            TestSlice<BooleanArray, BooleanArray.Builder>(x => x.Append(true).Append(false).AppendNull().Append(true));
        }

        [Fact]
        public void SliceStringArrayWithNullsAndEmptyStrings()
        {
            TestSlice<StringArray, StringArray.Builder>(x => x.Append("10").AppendNull().Append("30"));
            TestSlice<StringArray, StringArray.Builder>(x => x.Append("10").Append(string.Empty).Append("30"));
            TestSlice<StringArray, StringArray.Builder>(x => x.Append("10").Append(string.Empty).AppendNull().Append("30"));
            TestSlice<StringArray, StringArray.Builder>(x => x.Append("10").AppendNull().Append(string.Empty).Append("30"));
            TestSlice<StringArray, StringArray.Builder>(x => x.Append("10").AppendNull().Append(string.Empty).AppendNull().Append("30"));
        }

        private static void TestSlice<TArray, TArrayBuilder>(Action<TArrayBuilder> action)
            where TArray : IArrowArray
            where TArrayBuilder : IArrowArrayBuilder<TArray>, new()
        {
            var builder = new TArrayBuilder();
            action(builder);
            var baseArray = builder.Build(default) as Array;
            Assert.NotNull(baseArray);
            var totalLength = baseArray.Length;
            var validator = new ArraySliceValidator(baseArray);

            //Check all offset and length
            for (var offset = 0; offset < totalLength; offset++)
            {
                for (var length = 1; length + offset <= totalLength; length++)
                {
                    var targetArray = baseArray.Slice(offset, length);
                    targetArray.Accept(validator);
                }
            }
        }

        private class ArraySliceValidator :
            IArrowArrayVisitor<Int8Array>,
            IArrowArrayVisitor<Int16Array>,
            IArrowArrayVisitor<Int32Array>,
            IArrowArrayVisitor<Int64Array>,
            IArrowArrayVisitor<UInt8Array>,
            IArrowArrayVisitor<UInt16Array>,
            IArrowArrayVisitor<UInt32Array>,
            IArrowArrayVisitor<UInt64Array>,
            IArrowArrayVisitor<Date32Array>,
            IArrowArrayVisitor<Date64Array>,
            IArrowArrayVisitor<Time32Array>,
            IArrowArrayVisitor<Time64Array>,
            IArrowArrayVisitor<DurationArray>,
#if NET5_0_OR_GREATER
            IArrowArrayVisitor<HalfFloatArray>,
#endif
            IArrowArrayVisitor<FloatArray>,
            IArrowArrayVisitor<DoubleArray>,
            IArrowArrayVisitor<BooleanArray>,
            IArrowArrayVisitor<StringArray>
        {
            private readonly IArrowArray _baseArray;

            public ArraySliceValidator(IArrowArray baseArray)
            {
                _baseArray = baseArray;
            }

            public void Visit(Int8Array array) => ValidateArrays(array);
            public void Visit(Int16Array array) => ValidateArrays(array);
            public void Visit(Int32Array array) => ValidateArrays(array);
            public void Visit(Int64Array array) => ValidateArrays(array);
            public void Visit(UInt8Array array) => ValidateArrays(array);
            public void Visit(UInt16Array array) => ValidateArrays(array);
            public void Visit(UInt32Array array) => ValidateArrays(array);
            public void Visit(UInt64Array array) => ValidateArrays(array);

            public void Visit(Date32Array array)
            {
                ValidateArrays(array);
                Assert.IsAssignableFrom<Date32Array>(_baseArray);
                var baseArray = (Date32Array)_baseArray;

                Assert.Equal(baseArray.GetDateTimeOffset(array.Offset), array.GetDateTimeOffset(0));
            }

            public void Visit(Date64Array array)
            {
                ValidateArrays(array);
                Assert.IsAssignableFrom<Date64Array>(_baseArray);
                var baseArray = (Date64Array)_baseArray;

                Assert.Equal(baseArray.GetDateTimeOffset(array.Offset), array.GetDateTimeOffset(0));
            }
            public void Visit(Time32Array array) => ValidateArrays(array);
            public void Visit(Time64Array array) => ValidateArrays(array);
            public void Visit(DurationArray array) => ValidateArrays(array);

#if NET5_0_OR_GREATER
            public void Visit(HalfFloatArray array) => ValidateArrays(array);
#endif
            public void Visit(FloatArray array) => ValidateArrays(array);
            public void Visit(DoubleArray array) => ValidateArrays(array);
            public void Visit(StringArray array) => ValidateArrays(array);
            public void Visit(BooleanArray array) => ValidateArrays(array);

            public void Visit(IArrowArray array) => throw new NotImplementedException();

            private void ValidateArrays<T>(PrimitiveArray<T> slicedArray)
                where T : struct, IEquatable<T>
            {
                Assert.IsAssignableFrom<PrimitiveArray<T>>(_baseArray);
                var baseArray = (PrimitiveArray<T>)_baseArray;

                Assert.True(baseArray.NullBitmapBuffer.Span.SequenceEqual(slicedArray.NullBitmapBuffer.Span));
                Assert.True(
                    baseArray.ValueBuffer.Span.CastTo<T>().Slice(slicedArray.Offset, slicedArray.Length)
                        .SequenceEqual(slicedArray.Values));

                Assert.Equal(baseArray.GetValue(slicedArray.Offset), slicedArray.GetValue(0));

                ValidateNullCount(slicedArray);
            }

            private void ValidateArrays(BooleanArray slicedArray)
            {
                Assert.IsAssignableFrom<BooleanArray>(_baseArray);
                var baseArray = (BooleanArray)_baseArray;

                Assert.True(baseArray.NullBitmapBuffer.Span.SequenceEqual(slicedArray.NullBitmapBuffer.Span));
                Assert.True(baseArray.Values.SequenceEqual(slicedArray.Values));

                Assert.True(
                    baseArray.ValueBuffer.Span.Slice(0, (int) Math.Ceiling(slicedArray.Length / 8.0))
                        .SequenceEqual(slicedArray.Values));

                Assert.Equal(baseArray.GetValue(slicedArray.Offset), slicedArray.GetValue(0));

#pragma warning disable CS0618
                Assert.Equal(baseArray.GetBoolean(slicedArray.Offset), slicedArray.GetBoolean(0));
#pragma warning restore CS0618

                ValidateNullCount(slicedArray);
            }

            private void ValidateArrays(BinaryArray slicedArray)
            {
                Assert.IsAssignableFrom<BinaryArray>(_baseArray);
                var baseArray = (BinaryArray)_baseArray;

                Assert.True(baseArray.Values.SequenceEqual(slicedArray.Values));
                Assert.True(baseArray.NullBitmapBuffer.Span.SequenceEqual(slicedArray.NullBitmapBuffer.Span));
                Assert.True(
                    baseArray.ValueOffsetsBuffer.Span.CastTo<int>().Slice(slicedArray.Offset, slicedArray.Length + 1)
                        .SequenceEqual(slicedArray.ValueOffsets));

                Assert.True(baseArray.GetBytes(slicedArray.Offset).SequenceEqual(slicedArray.GetBytes(0)));

                ValidateNullCount(slicedArray);
            }

            private static void ValidateNullCount(IArrowArray slicedArray)
            {
                var expectedNullCount = Enumerable.Range(0, slicedArray.Length)
                    .Select(i => slicedArray.IsNull(i) ? 1 : 0)
                    .Sum();
                Assert.Equal(expectedNullCount, slicedArray.NullCount);
            }
        }
    }
}
