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
            TestNumberSlice<int, Time32Array, Time32Array.Builder>();
            TestNumberSlice<long, Time64Array, Time64Array.Builder>();
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
            TestNumberSlice<int, Time32Array, Time32Array.Builder>();
            TestNumberSlice<long, Time64Array, Time64Array.Builder>();

            static void TestNumberSlice<T, TArray, TBuilder>()
                where T : struct, INumber<T>
                where TArray : PrimitiveArray<T>
                where TBuilder : PrimitiveArrayBuilder<T, TArray, TBuilder>, new() =>
                TestSlice<TArray, TBuilder>(x => x.AppendNull().Append(T.CreateChecked(10)).Append(T.CreateChecked(20)).AppendNull().Append(T.CreateChecked(30)));
        }

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
            IArrowArrayVisitor<HalfFloatArray>,
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

            public void Visit(HalfFloatArray array) => ValidateArrays(array);
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
            }
        }
    }
}
