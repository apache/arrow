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

using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using System;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Xunit;
using System.Diagnostics;

namespace Apache.Arrow.Tests
{
    public static class ArrowReaderVerifier
    {
        public static void VerifyReader(ArrowStreamReader reader, RecordBatch originalBatch)
        {
            RecordBatch readBatch = reader.ReadNextRecordBatch();
            CompareBatches(originalBatch, readBatch);

            // There should only be one batch - calling ReadNextRecordBatch again should return null.
            Assert.Null(reader.ReadNextRecordBatch());
            Assert.Null(reader.ReadNextRecordBatch());
        }

        public static async Task VerifyReaderAsync(ArrowStreamReader reader, RecordBatch originalBatch)
        {
            Schema schema = await reader.GetSchema();
            Assert.NotNull(schema);

            RecordBatch readBatch = await reader.ReadNextRecordBatchAsync();
            CompareBatches(originalBatch, readBatch);

            // There should only be one batch - calling ReadNextRecordBatchAsync again should return null.
            Assert.Null(await reader.ReadNextRecordBatchAsync());
            Assert.Null(await reader.ReadNextRecordBatchAsync());
        }

        public static void CompareBatches(RecordBatch expectedBatch, RecordBatch actualBatch, bool strictCompare = true)
        {
            SchemaComparer.Compare(expectedBatch.Schema, actualBatch.Schema);
            Assert.Equal(expectedBatch.Length, actualBatch.Length);
            Assert.Equal(expectedBatch.ColumnCount, actualBatch.ColumnCount);

            for (int i = 0; i < expectedBatch.ColumnCount; i++)
            {
                IArrowArray expectedArray = expectedBatch.Arrays.ElementAt(i);
                IArrowArray actualArray = actualBatch.Arrays.ElementAt(i);

                CompareArrays(expectedArray, actualArray, strictCompare);
            }
        }

        public static void CompareArrays(IArrowArray expectedArray, IArrowArray actualArray, bool strictCompare = true)
        {
            actualArray.Accept(new ArrayComparer(expectedArray, strictCompare));
        }

        private class ArrayComparer :
            IArrowArrayVisitor<Int8Array>,
            IArrowArrayVisitor<Int16Array>,
            IArrowArrayVisitor<Int32Array>,
            IArrowArrayVisitor<Int64Array>,
            IArrowArrayVisitor<UInt8Array>,
            IArrowArrayVisitor<UInt16Array>,
            IArrowArrayVisitor<UInt32Array>,
            IArrowArrayVisitor<UInt64Array>,
#if NET5_0_OR_GREATER
            IArrowArrayVisitor<HalfFloatArray>,
#endif
            IArrowArrayVisitor<FloatArray>,
            IArrowArrayVisitor<DoubleArray>,
            IArrowArrayVisitor<BooleanArray>,
            IArrowArrayVisitor<TimestampArray>,
            IArrowArrayVisitor<Date32Array>,
            IArrowArrayVisitor<Date64Array>,
            IArrowArrayVisitor<Time32Array>,
            IArrowArrayVisitor<Time64Array>,
            IArrowArrayVisitor<DurationArray>,
            IArrowArrayVisitor<YearMonthIntervalArray>,
            IArrowArrayVisitor<DayTimeIntervalArray>,
            IArrowArrayVisitor<MonthDayNanosecondIntervalArray>,
            IArrowArrayVisitor<ListArray>,
            IArrowArrayVisitor<ListViewArray>,
            IArrowArrayVisitor<LargeListArray>,
            IArrowArrayVisitor<FixedSizeListArray>,
            IArrowArrayVisitor<StringArray>,
            IArrowArrayVisitor<StringViewArray>,
            IArrowArrayVisitor<LargeStringArray>,
            IArrowArrayVisitor<FixedSizeBinaryArray>,
            IArrowArrayVisitor<BinaryArray>,
            IArrowArrayVisitor<BinaryViewArray>,
            IArrowArrayVisitor<LargeBinaryArray>,
            IArrowArrayVisitor<StructArray>,
            IArrowArrayVisitor<UnionArray>,
            IArrowArrayVisitor<Decimal32Array>,
            IArrowArrayVisitor<Decimal64Array>,
            IArrowArrayVisitor<Decimal128Array>,
            IArrowArrayVisitor<Decimal256Array>,
            IArrowArrayVisitor<DictionaryArray>,
            IArrowArrayVisitor<NullArray>
        {
            private readonly IArrowArray _expectedArray;
            private readonly ArrayTypeComparer _arrayTypeComparer;
            private readonly bool _strictCompare;

            public ArrayComparer(IArrowArray expectedArray, bool strictCompare)
            {
                _expectedArray = expectedArray;
                _arrayTypeComparer = new ArrayTypeComparer(expectedArray.Data.DataType);
                _strictCompare = strictCompare;
            }

            public void Visit(Int8Array array) => CompareArrays(array);
            public void Visit(Int16Array array) => CompareArrays(array);
            public void Visit(Int32Array array) => CompareArrays(array);
            public void Visit(Int64Array array) => CompareArrays(array);
            public void Visit(UInt8Array array) => CompareArrays(array);
            public void Visit(UInt16Array array) => CompareArrays(array);
            public void Visit(UInt32Array array) => CompareArrays(array);
            public void Visit(UInt64Array array) => CompareArrays(array);
#if NET5_0_OR_GREATER
            public void Visit(HalfFloatArray array) => CompareArrays(array);
#endif
            public void Visit(FloatArray array) => CompareArrays(array);
            public void Visit(DoubleArray array) => CompareArrays(array);
            public void Visit(BooleanArray array) => CompareArrays(array);
            public void Visit(TimestampArray array) => CompareArrays(array);
            public void Visit(Date32Array array) => CompareArrays(array);
            public void Visit(Date64Array array) => CompareArrays(array);
            public void Visit(Time32Array array) => CompareArrays(array);
            public void Visit(Time64Array array) => CompareArrays(array);
            public void Visit(DurationArray array) => CompareArrays(array);
            public void Visit(YearMonthIntervalArray array) => CompareArrays(array);
            public void Visit(DayTimeIntervalArray array) => CompareArrays(array);
            public void Visit(MonthDayNanosecondIntervalArray array) => CompareArrays(array);
            public void Visit(ListArray array) => CompareArrays(array);
            public void Visit(ListViewArray array) => CompareArrays(array);
            public void Visit(LargeListArray array) => CompareArrays(array);
            public void Visit(FixedSizeListArray array) => CompareArrays(array);
            public void Visit(FixedSizeBinaryArray array) => CompareArrays(array);
            public void Visit(Decimal32Array array) => CompareArrays(array);
            public void Visit(Decimal64Array array) => CompareArrays(array);
            public void Visit(Decimal128Array array) => CompareArrays(array);
            public void Visit(Decimal256Array array) => CompareArrays(array);
            public void Visit(StringArray array) => CompareBinaryArrays<StringArray>(array);
            public void Visit(StringViewArray array) => CompareVariadicArrays<StringViewArray>(array);
            public void Visit(LargeStringArray array) => CompareLargeBinaryArrays<LargeStringArray>(array);
            public void Visit(BinaryArray array) => CompareBinaryArrays<BinaryArray>(array);
            public void Visit(BinaryViewArray array) => CompareVariadicArrays<BinaryViewArray>(array);
            public void Visit(LargeBinaryArray array) => CompareLargeBinaryArrays<LargeBinaryArray>(array);

            public void Visit(StructArray array)
            {
                Assert.IsAssignableFrom<StructArray>(_expectedArray);
                StructArray expectedArray = (StructArray)_expectedArray;

                Assert.Equal(expectedArray.Length, array.Length);
                Assert.Equal(expectedArray.NullCount, array.NullCount);
                Assert.Equal(expectedArray.Data.Children.Length, array.Data.Children.Length);
                Assert.Equal(expectedArray.Fields.Count, array.Fields.Count);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, array.Offset);
                }

                for (int i = 0; i < array.Fields.Count; i++)
                {
                    array.Fields[i].Accept(new ArrayComparer(expectedArray.Fields[i], _strictCompare));
                }
            }

            public void Visit(UnionArray array)
            {
                Assert.IsAssignableFrom<UnionArray>(_expectedArray);
                UnionArray expectedArray = (UnionArray)_expectedArray;

                Assert.Equal(expectedArray.Mode, array.Mode);
                Assert.Equal(expectedArray.Length, array.Length);
                Assert.Equal(expectedArray.NullCount, array.NullCount);
                Assert.Equal(expectedArray.Data.Children.Length, array.Data.Children.Length);
                Assert.Equal(expectedArray.Fields.Count, array.Fields.Count);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, array.Offset);
                    Assert.True(expectedArray.TypeBuffer.Span.SequenceEqual(array.TypeBuffer.Span));
                }
                else
                {
                    for (int i = 0; i < expectedArray.Length; i++)
                    {
                        Assert.Equal(expectedArray.TypeIds[i], array.TypeIds[i]);
                    }
                }

                if (_expectedArray is DenseUnionArray expectedDenseArray)
                {
                    Assert.IsAssignableFrom<DenseUnionArray>(array);
                    var denseArray = array as DenseUnionArray;
                    Assert.NotNull(denseArray);

                    if (_strictCompare)
                    {
                        Assert.True(expectedDenseArray.ValueOffsetBuffer.Span.SequenceEqual(denseArray.ValueOffsetBuffer.Span));
                    }
                    else
                    {
                        for (int i = 0; i < expectedDenseArray.Length; i++)
                        {
                            Assert.Equal(
                                expectedDenseArray.ValueOffsets[i], denseArray.ValueOffsets[i]);
                        }
                    }
                }

                for (int i = 0; i < array.Fields.Count; i++)
                {
                    array.Fields[i].Accept(new ArrayComparer(expectedArray.Fields[i], _strictCompare));
                }
            }

            public void Visit(DictionaryArray array)
            {
                Assert.IsAssignableFrom<DictionaryArray>(_expectedArray);
                DictionaryArray expectedArray = (DictionaryArray)_expectedArray;
                var indicesComparer = new ArrayComparer(expectedArray.Indices, _strictCompare);
                var dictionaryComparer = new ArrayComparer(expectedArray.Dictionary, _strictCompare);
                array.Indices.Accept(indicesComparer);
                array.Dictionary.Accept(dictionaryComparer);
            }

            public void Visit(NullArray array)
            {
                Assert.IsAssignableFrom<NullArray>(_expectedArray);
                Assert.Equal(_expectedArray.Length, array.Length);
                Assert.Equal(_expectedArray.NullCount, array.NullCount);
                Assert.Equal(_expectedArray.Offset, array.Offset);
            }

            public void Visit(IArrowArray array) => throw new NotImplementedException();

            private void CompareBinaryArrays<T>(BinaryArray actualArray)
                where T : IArrowArray
            {
                Assert.IsAssignableFrom<T>(_expectedArray);
                Assert.IsAssignableFrom<T>(actualArray);

                var expectedArray = (BinaryArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                CompareValidityBuffer(expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer, expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                    Assert.True(expectedArray.ValueOffsetsBuffer.Span.SequenceEqual(actualArray.ValueOffsetsBuffer.Span));
                    Assert.True(expectedArray.Values.Slice(0, expectedArray.Length).SequenceEqual(actualArray.Values.Slice(0, actualArray.Length)));
                }
                else
                {
                    for (int i = 0; i < expectedArray.Length; i++)
                    {
                        Assert.True(
                            expectedArray.GetBytes(i).SequenceEqual(actualArray.GetBytes(i)),
                            $"BinaryArray values do not match at index {i}.");
                    }
                }
            }

            private void CompareLargeBinaryArrays<T>(LargeBinaryArray actualArray)
                where T : IArrowArray
            {
                Assert.IsAssignableFrom<T>(_expectedArray);
                Assert.IsAssignableFrom<T>(actualArray);

                var expectedArray = (LargeBinaryArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                CompareValidityBuffer(
                    expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer,
                    expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                    Assert.True(expectedArray.ValueOffsetsBuffer.Span.SequenceEqual(actualArray.ValueOffsetsBuffer.Span));
                    Assert.True(expectedArray.ValueBuffer.Span.Slice(0, expectedArray.Length).SequenceEqual(actualArray.ValueBuffer.Span.Slice(0, actualArray.Length)));
                }
                else
                {
                    for (int i = 0; i < expectedArray.Length; i++)
                    {
                        Assert.True(
                            expectedArray.GetBytes(i).SequenceEqual(actualArray.GetBytes(i)),
                            $"LargeBinaryArray values do not match at index {i}.");
                    }
                }
            }

            private void CompareVariadicArrays<T>(BinaryViewArray actualArray)
                where T : IArrowArray
            {
                Assert.IsAssignableFrom<T>(_expectedArray);
                Assert.IsAssignableFrom<T>(actualArray);

                var expectedArray = (BinaryViewArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                }

                CompareValidityBuffer(expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer, expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                Assert.True(expectedArray.Views.SequenceEqual(actualArray.Views));

                for (int i = 0; i < expectedArray.Length; i++)
                {
                    Assert.True(
                        expectedArray.GetBytes(i).SequenceEqual(actualArray.GetBytes(i)),
                        $"BinaryArray values do not match at index {i}.");
                }
            }

            private void CompareArrays(FixedSizeBinaryArray actualArray)
            {
                Assert.IsAssignableFrom<FixedSizeBinaryArray>(_expectedArray);
                Assert.IsAssignableFrom<FixedSizeBinaryArray>(actualArray);

                var expectedArray = (FixedSizeBinaryArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                CompareValidityBuffer(expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer, expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                    Assert.True(expectedArray.ValueBuffer.Span.Slice(0, expectedArray.Length).SequenceEqual(actualArray.ValueBuffer.Span.Slice(0, actualArray.Length)));
                }
                else
                {
                    for (int i = 0; i < expectedArray.Length; i++)
                    {
                        Assert.True(
                            expectedArray.GetBytes(i).SequenceEqual(actualArray.GetBytes(i)),
                            $"FixedSizeBinaryArray values do not match at index {i}.");
                    }
                }
            }

            private void CompareArrays<T>(PrimitiveArray<T> actualArray)
                where T : struct, IEquatable<T>
            {
                Assert.IsAssignableFrom<PrimitiveArray<T>>(_expectedArray);
                PrimitiveArray<T> expectedArray = (PrimitiveArray<T>)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                CompareValidityBuffer(expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer, expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                    Assert.True(expectedArray.Values.Slice(0, expectedArray.Length).SequenceEqual(actualArray.Values.Slice(0, actualArray.Length)));
                }
                else
                {
                    for (int i = 0; i < expectedArray.Length; i++)
                    {
                        T? expected = expectedArray.GetValue(i);
                        T? actual = actualArray.GetValue(i);
                        Assert.Equal(expected.HasValue, actual.HasValue);
                        if (expected.HasValue)
                        {
                            Assert.Equal(expected.Value, actual.Value);
                        }
                    }
                }
            }

            private void CompareArrays(BooleanArray actualArray)
            {
                Assert.IsAssignableFrom<BooleanArray>(_expectedArray);
                BooleanArray expectedArray = (BooleanArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                CompareValidityBuffer(expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer, expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                    int booleanByteCount = BitUtility.ByteCount(expectedArray.Length);
                    Assert.True(expectedArray.Values.Slice(0, booleanByteCount).SequenceEqual(actualArray.Values.Slice(0, booleanByteCount)));
                }
                else
                {
                    for (int i = 0; i < expectedArray.Length; i++)
                    {
                        Assert.Equal(expectedArray.GetValue(i), actualArray.GetValue(i));
                    }
                }
            }

            private void CompareArrays(ListArray actualArray)
            {
                Assert.IsAssignableFrom<ListArray>(_expectedArray);
                ListArray expectedArray = (ListArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                CompareValidityBuffer(expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer, expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                    Assert.True(expectedArray.ValueOffsetsBuffer.Span.SequenceEqual(actualArray.ValueOffsetsBuffer.Span));
                    actualArray.Values.Accept(new ArrayComparer(expectedArray.Values, _strictCompare));
                }
                else
                {
                    for (int i = 0; i < actualArray.Length; ++i)
                    {
                        if (expectedArray.IsNull(i))
                        {
                            Assert.True(actualArray.IsNull(i));
                        }
                        else
                        {
                            var expectedList = expectedArray.GetSlicedValues(i);
                            var actualList = actualArray.GetSlicedValues(i);
                            actualList.Accept(new ArrayComparer(expectedList, _strictCompare));
                        }
                    }
                }
            }

            private void CompareArrays(ListViewArray actualArray)
            {
                Assert.IsAssignableFrom<ListViewArray>(_expectedArray);
                ListViewArray expectedArray = (ListViewArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                CompareValidityBuffer(expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer, expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                    Assert.True(expectedArray.ValueOffsetsBuffer.Span.SequenceEqual(actualArray.ValueOffsetsBuffer.Span));
                    Assert.True(expectedArray.SizesBuffer.Span.SequenceEqual(actualArray.SizesBuffer.Span));
                    actualArray.Values.Accept(new ArrayComparer(expectedArray.Values, _strictCompare));
                }
                else
                {
                    for (int i = 0; i < actualArray.Length; ++i)
                    {
                        if (expectedArray.IsNull(i))
                        {
                            Assert.True(actualArray.IsNull(i));
                        }
                        else
                        {
                            var expectedList = expectedArray.GetSlicedValues(i);
                            var actualList = actualArray.GetSlicedValues(i);
                            actualList.Accept(new ArrayComparer(expectedList, _strictCompare));
                        }
                    }
                }
            }

            private void CompareArrays(LargeListArray actualArray)
            {
                Assert.IsAssignableFrom<LargeListArray>(_expectedArray);
                LargeListArray expectedArray = (LargeListArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);

                CompareValidityBuffer(
                    expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer,
                    expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                    Assert.True(expectedArray.ValueOffsetsBuffer.Span.SequenceEqual(actualArray.ValueOffsetsBuffer.Span));
                    actualArray.Values.Accept(new ArrayComparer(expectedArray.Values, _strictCompare));
                }
                else
                {
                    for (int i = 0; i < actualArray.Length; ++i)
                    {
                        if (expectedArray.IsNull(i))
                        {
                            Assert.True(actualArray.IsNull(i));
                        }
                        else
                        {
                            var expectedList = expectedArray.GetSlicedValues(i);
                            var actualList = actualArray.GetSlicedValues(i);
                            actualList.Accept(new ArrayComparer(expectedList, _strictCompare));
                        }
                    }
                }
            }

            private void CompareArrays(FixedSizeListArray actualArray)
            {
                Assert.IsAssignableFrom<FixedSizeListArray>(_expectedArray);
                FixedSizeListArray expectedArray = (FixedSizeListArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);
                if (_strictCompare)
                {
                    Assert.Equal(expectedArray.Offset, actualArray.Offset);
                }

                CompareValidityBuffer(expectedArray.NullCount, _expectedArray.Length, expectedArray.NullBitmapBuffer, expectedArray.Offset, actualArray.NullBitmapBuffer, actualArray.Offset);

                var listSize = ((FixedSizeListType)expectedArray.Data.DataType).ListSize;
                var expectedValuesSlice = ArrowArrayFactory.Slice(
                    expectedArray.Values, expectedArray.Offset * listSize, expectedArray.Length * listSize);
                var actualValuesSlice = ArrowArrayFactory.Slice(
                    actualArray.Values, actualArray.Offset * listSize, actualArray.Length * listSize);
                actualValuesSlice.Accept(new ArrayComparer(expectedValuesSlice, _strictCompare));
            }

            private void CompareValidityBuffer(int nullCount, int arrayLength, ArrowBuffer expectedValidityBuffer, int expectedBufferOffset, ArrowBuffer actualValidityBuffer, int actualBufferOffset)
            {
                if (_strictCompare)
                {
                    Assert.True(expectedValidityBuffer.Span.SequenceEqual(actualValidityBuffer.Span));
                }
                else if (actualValidityBuffer.IsEmpty || expectedValidityBuffer.IsEmpty || arrayLength == 0)
                {
                    Assert.True(nullCount == 0 || arrayLength == 0);
                }
                else
                {
                    // Compare all values bitwise
                    var expectedSpan = expectedValidityBuffer.Span;
                    var actualSpan = actualValidityBuffer.Span;
                    for (int i = 0; i < arrayLength; i++)
                    {
                        Assert.True(
                            BitUtility.GetBit(expectedSpan, expectedBufferOffset + i) == BitUtility.GetBit(actualSpan, actualBufferOffset + i),
                            string.Format("Bit at index {0}/{1} is not equal", i, arrayLength));
                    }
                }
            }
        }
    }
}
