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
using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests;

public class UnionArrayTests
{
    [Theory]
    [InlineData(UnionMode.Sparse)]
    [InlineData(UnionMode.Dense)]
    public void UnionArrayIsNull(UnionMode mode)
    {
        var (array, expectedNull) = BuildUnionArray(mode, 100);

        for (var i = 0; i < array.Length; ++i)
        {
            Assert.Equal(expectedNull[i], array.IsNull(i));
            Assert.Equal(!expectedNull[i], array.IsValid(i));
        }
    }

    [Theory]
    [InlineData(UnionMode.Sparse)]
    [InlineData(UnionMode.Dense)]
    public void UnionArraySlice(UnionMode mode)
    {
        var (array, expectedNull) = BuildUnionArray(mode, 10);

        for (var offset = 0; offset < array.Length; ++offset)
        {
            for (var length = 0; length < array.Length - offset; ++length)
            {
                var slicedArray = (UnionArray)ArrowArrayFactory.Slice(array, offset, length);

                var nullCount = 0;
                for (var i = 0; i < slicedArray.Length; ++i)
                {
                    Assert.Equal(expectedNull[offset + i], slicedArray.IsNull(i));
                    Assert.Equal(!expectedNull[offset + i], slicedArray.IsValid(i));
                    nullCount += expectedNull[offset + i] ? 1 : 0;

                    CompareValue(array, offset + i, slicedArray, i);
                }

                Assert.Equal(nullCount, slicedArray.NullCount);
            }
        }
    }

    [Theory]
    [InlineData(UnionMode.Sparse)]
    [InlineData(UnionMode.Dense)]
    public void UnionArrayConstructedWithOffset(UnionMode mode)
    {
        const int length = 10;
        var (array, expectedNull) = BuildUnionArray(mode, length);

        for (var offset = 0; offset < array.Length; ++offset)
        {
            var (slicedArray, _) = BuildUnionArray(mode, length, offset);

            var nullCount = 0;
            for (var i = 0; i < slicedArray.Length; ++i)
            {
                Assert.Equal(expectedNull[offset + i], slicedArray.IsNull(i));
                Assert.Equal(!expectedNull[offset + i], slicedArray.IsValid(i));
                nullCount += expectedNull[offset + i] ? 1 : 0;

                CompareValue(array, offset + i, slicedArray, i);
            }

            Assert.Equal(nullCount, slicedArray.NullCount);
        }
    }

    private static void CompareValue(UnionArray originalArray, int originalIndex, UnionArray slicedArray, int sliceIndex)
    {
        var typeId = originalArray.TypeIds[originalIndex];
        var sliceTypeId = slicedArray.TypeIds[sliceIndex];
        Assert.Equal(typeId, sliceTypeId);

        switch (typeId)
        {
            case 0:
                CompareFieldValue<int, Int32Array>(typeId, originalArray, originalIndex, slicedArray, sliceIndex);
                break;
            case 1:
                CompareFieldValue<float, FloatArray>(typeId, originalArray, originalIndex, slicedArray, sliceIndex);
                break;
            default:
                throw new Exception($"Unexpected type id {typeId}");
        }
    }

    private static void CompareFieldValue<T, TArray>(byte typeId, UnionArray originalArray, int originalIndex, UnionArray slicedArray, int sliceIndex)
        where T : struct, IEquatable<T>
        where TArray : PrimitiveArray<T>
    {
        if (originalArray is DenseUnionArray denseOriginalArray)
        {
            Assert.IsType<DenseUnionArray>(slicedArray);

            originalIndex = denseOriginalArray.ValueOffsets[originalIndex];
            sliceIndex = ((DenseUnionArray)slicedArray).ValueOffsets[sliceIndex];
        }
        var originalValue = ((TArray)originalArray.Fields[typeId]).GetValue(originalIndex);
        var sliceValue = ((TArray)slicedArray.Fields[typeId]).GetValue(sliceIndex);
        Assert.Equal(originalValue, sliceValue);
    }

    private static (UnionArray array, bool[] isNull) BuildUnionArray(UnionMode mode, int length, int offset=0)
    {
        var fields = new Field[]
        {
            new Field("field0", new Int32Type(), true),
            new Field("field1", new FloatType(), true),
        };
        var typeIds = new[] { 0, 1 };
        var type = new UnionType(fields, typeIds, mode);

        var nullCount = 0;
        var field0Builder = new Int32Array.Builder();
        var field1Builder = new FloatArray.Builder();
        var typeIdsBuilder = new ArrowBuffer.Builder<byte>();
        var valuesOffsetBuilder = new ArrowBuffer.Builder<int>();
        var expectedNull = new bool[length];

        for (var i = 0; i < length; ++i)
        {
            var isNull = i % 3 == 0;
            expectedNull[i] = isNull;
            nullCount += (isNull && i >= offset) ? 1 : 0;

            if (i % 2 == 0)
            {
                typeIdsBuilder.Append(0);
                if (mode == UnionMode.Sparse)
                {
                    field1Builder.Append(0.0f);
                }
                else
                {
                    valuesOffsetBuilder.Append(field0Builder.Length);
                }

                if (isNull)
                {
                    field0Builder.AppendNull();
                }
                else
                {
                    field0Builder.Append(i);
                }
            }
            else
            {
                typeIdsBuilder.Append(1);
                if (mode == UnionMode.Sparse)
                {
                    field0Builder.Append(0);
                }
                else
                {
                    valuesOffsetBuilder.Append(field1Builder.Length);
                }

                if (isNull)
                {
                    field1Builder.AppendNull();
                }
                else
                {
                    field1Builder.Append(i * 0.1f);
                }
            }
        }

        var typeIdsBuffer = typeIdsBuilder.Build();
        var valuesOffsetBuffer = valuesOffsetBuilder.Build();
        var children = new IArrowArray[]
        {
            field0Builder.Build(),
            field1Builder.Build()
        };

        UnionArray array = mode == UnionMode.Dense
            ? new DenseUnionArray(type, length - offset, children, typeIdsBuffer, valuesOffsetBuffer, nullCount, offset)
            : new SparseUnionArray(type, length - offset, children, typeIdsBuffer, nullCount, offset);

        return (array, expectedNull);
    }
}
