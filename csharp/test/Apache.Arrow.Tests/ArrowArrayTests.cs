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
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowArrayTests
    {
        [Fact]
        public void SliceArray()
        {
            TestSlice<Int32Array, Int32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<Int8Array, Int8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<Int16Array, Int16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<Int64Array, Int64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<UInt8Array, UInt8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<UInt16Array, UInt16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<UInt32Array, UInt32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<UInt64Array, UInt64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<FloatArray, FloatArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<DoubleArray, DoubleArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestSlice<StringArray, StringArray.Builder>(x => x.Append("10").Append("20").Append("30"));
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
            IArrowArrayVisitor<FloatArray>,
            IArrowArrayVisitor<DoubleArray>,
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
            public void Visit(FloatArray array) => ValidateArrays(array);
            public void Visit(DoubleArray array) => ValidateArrays(array);
            public void Visit(StringArray array) => ValidateArrays(array);

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
            }

            private void ValidateArrays(BinaryArray slicedArray)
            {
                Assert.IsAssignableFrom<BinaryArray>(_baseArray);
                var baseArray = (BinaryArray)_baseArray;

                Assert.True(baseArray.NullBitmapBuffer.Span.SequenceEqual(slicedArray.NullBitmapBuffer.Span));
                Assert.True(
                    baseArray.ValueOffsetsBuffer.Span.CastTo<int>().Slice(slicedArray.Offset, slicedArray.Length + 1)
                        .SequenceEqual(slicedArray.ValueOffsets));
            }
        }
    }
}
