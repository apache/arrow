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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
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
            Assert.True(SchemaComparer.Equals(expectedBatch.Schema, actualBatch.Schema));
            Assert.Equal(expectedBatch.Length, actualBatch.Length);
            Assert.Equal(expectedBatch.ColumnCount, actualBatch.ColumnCount);

            for (int i = 0; i < expectedBatch.ColumnCount; i++)
            {
                IArrowArray expectedArray = expectedBatch.Arrays.ElementAt(i);
                IArrowArray actualArray = actualBatch.Arrays.ElementAt(i);

                actualArray.Accept(new ArrayComparer(expectedArray));
            }
        }

        private class ArrayTypeComparer :
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<Date32Type>,
            IArrowTypeVisitor<Date64Type>,
            IArrowTypeVisitor<Time32Type>,
            IArrowTypeVisitor<Time64Type>,
            IArrowTypeVisitor<FixedSizeBinaryType>
        {
            private readonly IArrowType _expectedType;

            public ArrayTypeComparer(IArrowType expectedType)
            {
                Debug.Assert(expectedType != null);
                _expectedType = expectedType;
            }

            public void Visit(TimestampType actualType)
            {
                Assert.IsAssignableFrom<TimestampType>(_expectedType);

                var expectedType = (TimestampType) _expectedType;
                
                Assert.Equal(expectedType.Timezone, actualType.Timezone);
                Assert.Equal(expectedType.Unit, actualType.Unit);
            }

            public void Visit(Date32Type actualType)
            {
                Assert.IsAssignableFrom<Date32Type>(_expectedType);
                var expectedType = (Date32Type)_expectedType;

                Assert.Equal(expectedType.Unit, actualType.Unit);
            }

            public void Visit(Date64Type actualType)
            {
                Assert.IsAssignableFrom<Date64Type>(_expectedType);
                var expectedType = (Date64Type)_expectedType;

                Assert.Equal(expectedType.Unit, actualType.Unit);
            }

            public void Visit(Time32Type actualType)
            {
                Assert.IsAssignableFrom<Time32Type>(_expectedType);
                var expectedType = (Time32Type)_expectedType;

                Assert.Equal(expectedType.Unit, actualType.Unit);
            }

            public void Visit(Time64Type actualType)
            {
                Assert.IsAssignableFrom<Time64Type>(_expectedType);
                var expectedType = (Time64Type)_expectedType;

                Assert.Equal(expectedType.Unit, actualType.Unit);
            }

            public void Visit(FixedSizeBinaryType actualType)
            {
                Assert.IsAssignableFrom<FixedSizeBinaryType>(_expectedType);
                var expectedType = (FixedSizeBinaryType)_expectedType;

                Assert.Equal(expectedType.ByteWidth, actualType.ByteWidth);
            }

            public void Visit(IArrowType actualType)
            {
                Assert.IsAssignableFrom(actualType.GetType(), _expectedType);
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
            IArrowArrayVisitor<BinaryArray>
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
            public void Visit(ListArray array) => throw new NotImplementedException();

            public void Visit(StringArray array) => CompareBinaryArrays<StringArray>(array);

            public void Visit(BinaryArray array) => CompareBinaryArrays<BinaryArray>(array);
            public void Visit(FixedSizeBinaryType array) => throw new NotImplementedException();
            public void Visit(IArrowArray array) => throw new NotImplementedException();

            private void CompareBinaryArrays<T>(BinaryArray actualArray)
                where T: IArrowArray
            {
                Assert.IsAssignableFrom<T>(_expectedArray);
                Assert.IsAssignableFrom<T>(actualArray);

                var expectedArray = (BinaryArray)_expectedArray;

                _arrayTypeComparer.Visit(actualArray.Data.DataType);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);
                Assert.Equal(expectedArray.Offset, actualArray.Offset);

                Assert.True(expectedArray.NullBitmapBuffer.Span.SequenceEqual(actualArray.NullBitmapBuffer.Span));
                Assert.True(expectedArray.ValueOffsetsBuffer.Span.SequenceEqual(actualArray.ValueOffsetsBuffer.Span));
                Assert.True(expectedArray.Values.Slice(0, expectedArray.Length).SequenceEqual(actualArray.Values.Slice(0, actualArray.Length)));
            }

            private void CompareArrays<T>(PrimitiveArray<T> actualArray)
                where T : struct, IEquatable<T>
            {
                Assert.IsAssignableFrom<PrimitiveArray<T>>(_expectedArray);
                PrimitiveArray<T> expectedArray = (PrimitiveArray<T>)_expectedArray;

                _arrayTypeComparer.Visit(actualArray.Data.DataType);

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

                _arrayTypeComparer.Visit(actualArray.Data.DataType);

                Assert.Equal(expectedArray.Length, actualArray.Length);
                Assert.Equal(expectedArray.NullCount, actualArray.NullCount);
                Assert.Equal(expectedArray.Offset, actualArray.Offset);

                Assert.True(expectedArray.NullBitmapBuffer.Span.SequenceEqual(actualArray.NullBitmapBuffer.Span));

                int booleanByteCount = BitUtility.ByteCount(expectedArray.Length);
                Assert.True(expectedArray.Values.Slice(0, booleanByteCount).SequenceEqual(actualArray.Values.Slice(0, booleanByteCount)));
            }
        }
    }
}
