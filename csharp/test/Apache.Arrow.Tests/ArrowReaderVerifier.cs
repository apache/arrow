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
            RecordBatch readBatch = await reader.ReadNextRecordBatchAsync();
            CompareBatches(originalBatch, readBatch);

            // There should only be one batch - calling ReadNextRecordBatchAsync again should return null.
            Assert.Null(await reader.ReadNextRecordBatchAsync());
            Assert.Null(await reader.ReadNextRecordBatchAsync());
        }

        public static void CompareBatches(RecordBatch expectedBatch, RecordBatch actualBatch)
        {
            SchemaComparer.Compare(expectedBatch.Schema, actualBatch.Schema);
            Assert.Equal(expectedBatch.Length, actualBatch.Length);
            Assert.Equal(expectedBatch.ColumnCount, actualBatch.ColumnCount);

            for (int i = 0; i < expectedBatch.ColumnCount; i++)
            {
                IArrowArray expectedArray = expectedBatch.Arrays.ElementAt(i);
                IArrowArray actualArray = actualBatch.Arrays.ElementAt(i);

                actualArray.Accept(new ArrayComparer(expectedArray));
            }
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
            IArrowArrayVisitor<FloatArray>,
            IArrowArrayVisitor<DoubleArray>,
            IArrowArrayVisitor<BooleanArray>,
            IArrowArrayVisitor<TimestampArray>,
            IArrowArrayVisitor<Date32Array>,
            IArrowArrayVisitor<Date64Array>,
            IArrowArrayVisitor<ListArray>,
            IArrowArrayVisitor<StringArray>,
            IArrowArrayVisitor<BinaryArray>,
            IArrowArrayVisitor<StructArray>,
            IArrowArrayVisitor<Decimal128Array>,
            IArrowArrayVisitor<Decimal256Array>,
            IArrowArrayVisitor<DictionaryArray>
        {
            private readonly IArrowArray _expectedArray;
            private readonly ArrayTypeComparer _arrayTypeComparer;

            public ArrayComparer(IArrowArray expectedArray)
            {
                _expectedArray = expectedArray;
                _arrayTypeComparer = new ArrayTypeComparer(expectedArray.Data.DataType);
            }

            public void Visit(Int8Array array) => CompareArrays(array);
            public void Visit(Int16Array array) => CompareArrays(array);
            public void Visit(Int32Array array) => CompareArrays(array);
            public void Visit(Int64Array array) => CompareArrays(array);
            public void Visit(UInt8Array array) => CompareArrays(array);
            public void Visit(UInt16Array array) => CompareArrays(array);
            public void Visit(UInt32Array array) => CompareArrays(array);
            public void Visit(UInt64Array array) => CompareArrays(array);
            public void Visit(FloatArray array) => CompareArrays(array);
            public void Visit(DoubleArray array) => CompareArrays(array);
            public void Visit(BooleanArray array) => CompareArrays(array);
            public void Visit(TimestampArray array) => CompareArrays(array);
            public void Visit(Date32Array array) => CompareArrays(array);
            public void Visit(Date64Array array) => CompareArrays(array);
            public void Visit(ListArray array) => CompareArrays(array);
            public void Visit(Decimal128Array array) => CompareArrays(array);
            public void Visit(Decimal256Array array) => CompareArrays(array);
            public void Visit(StringArray array) => CompareBinaryArrays<StringArray>(array);

            public void Visit(BinaryArray array) => CompareBinaryArrays<BinaryArray>(array);

            public void Visit(StructArray array)
            {
                Assert.IsAssignableFrom<StructArray>(_expectedArray);
                StructArray expectedArray = (StructArray)_expectedArray;

                Assert.Equal(expectedArray.Length, array.Length);
                Assert.Equal(expectedArray.NullCount, array.NullCount);
                Assert.Equal(expectedArray.Offset, array.Offset);
                Assert.Equal(expectedArray.Data.Children.Length, array.Data.Children.Length);
                Assert.Equal(expectedArray.Fields.Count, array.Fields.Count);

                for (int i = 0; i < array.Fields.Count; i++)
                {
                    array.Fields[i].Accept(new ArrayComparer(expectedArray.Fields[i]));
                }
            }

            public void Visit(DictionaryArray array)
            {
                Assert.IsAssignableFrom<DictionaryArray>(_expectedArray);
                DictionaryArray expectedArray = (DictionaryArray)_expectedArray;
                var indicesComparer = new ArrayComparer(expectedArray.Indices);
                var dictionaryComparer = new ArrayComparer(expectedArray.Dictionary);
                array.Indices.Accept(indicesComparer);
                array.Dictionary.Accept(dictionaryComparer);
            }

            public void Visit(FixedSizeBinaryType array) => throw new NotImplementedException();
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
                Assert.Equal(expectedArray.Offset, actualArray.Offset);

                Assert.True(expectedArray.NullBitmapBuffer.Span.SequenceEqual(actualArray.NullBitmapBuffer.Span));
                Assert.True(expectedArray.ValueOffsetsBuffer.Span.SequenceEqual(actualArray.ValueOffsetsBuffer.Span));
                Assert.True(expectedArray.Values.Slice(0, expectedArray.Length).SequenceEqual(actualArray.Values.Slice(0, actualArray.Length)));
            }

            private void CompareArrays(FixedSizeBinaryArray actualArray)
            {
                Assert.IsAssignableFrom<FixedSizeBinaryArray>(_expectedArray);
                Assert.IsAssignableFrom<FixedSizeBinaryArray>(actualArray);

                var expectedArray = (FixedSizeBinaryArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);
                Assert.Equal(expectedArray.Offset, actualArray.Offset);

                Assert.True(expectedArray.NullBitmapBuffer.Span.SequenceEqual(actualArray.NullBitmapBuffer.Span));
                Assert.True(expectedArray.ValueBuffer.Span.Slice(0, expectedArray.Length).SequenceEqual(actualArray.ValueBuffer.Span.Slice(0, actualArray.Length)));
            }

            private void CompareArrays<T>(PrimitiveArray<T> actualArray)
                where T : struct, IEquatable<T>
            {
                Assert.IsAssignableFrom<PrimitiveArray<T>>(_expectedArray);
                PrimitiveArray<T> expectedArray = (PrimitiveArray<T>)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);
                Assert.Equal(expectedArray.Offset, actualArray.Offset);

                Assert.True(expectedArray.NullBitmapBuffer.Span.SequenceEqual(actualArray.NullBitmapBuffer.Span));
                Assert.True(expectedArray.Values.Slice(0, expectedArray.Length).SequenceEqual(actualArray.Values.Slice(0, actualArray.Length)));
            }

            private void CompareArrays(BooleanArray actualArray)
            {
                Assert.IsAssignableFrom<BooleanArray>(_expectedArray);
                BooleanArray expectedArray = (BooleanArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);
                Assert.Equal(expectedArray.Offset, actualArray.Offset);

                Assert.True(expectedArray.NullBitmapBuffer.Span.SequenceEqual(actualArray.NullBitmapBuffer.Span));

                int booleanByteCount = BitUtility.ByteCount(expectedArray.Length);
                Assert.True(expectedArray.Values.Slice(0, booleanByteCount).SequenceEqual(actualArray.Values.Slice(0, booleanByteCount)));
            }

            private void CompareArrays(ListArray actualArray)
            {
                Assert.IsAssignableFrom<ListArray>(_expectedArray);
                ListArray expectedArray = (ListArray)_expectedArray;

                actualArray.Data.DataType.Accept(_arrayTypeComparer);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);
                Assert.Equal(expectedArray.Offset, actualArray.Offset);

                Assert.True(expectedArray.NullBitmapBuffer.Span.SequenceEqual(actualArray.NullBitmapBuffer.Span));
                Assert.True(expectedArray.ValueOffsetsBuffer.Span.SequenceEqual(actualArray.ValueOffsetsBuffer.Span));

                actualArray.Values.Accept(new ArrayComparer(expectedArray.Values));
            }
        }
    }
}
