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

using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using System;
using System.Collections.Generic;

namespace Apache.Arrow
{
    static class ArrayDataConcatenator
    {
        internal static ArrayData Concatenate(IReadOnlyList<ArrayData> arrayDataList, MemoryAllocator allocator = default)
        {
            if (arrayDataList == null || arrayDataList.Count == 0)
            {
                return null;
            }

            if (arrayDataList.Count == 1)
            {
                return arrayDataList[0];
            }

            var arrowArrayConcatenationVisitor = new ArrayDataConcatenationVisitor(arrayDataList, allocator);

            IArrowType type = arrayDataList[0].DataType;
            type.Accept(arrowArrayConcatenationVisitor);

            return arrowArrayConcatenationVisitor.Result;
        }

        private class ArrayDataConcatenationVisitor :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<FixedWidthType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<StructType>
        {
            public ArrayData Result { get; private set; }
            private readonly IReadOnlyList<ArrayData> _arrayDataList;
            private readonly int _totalLength;
            private readonly int _totalNullCount;
            private readonly MemoryAllocator _allocator;

            public ArrayDataConcatenationVisitor(IReadOnlyList<ArrayData> arrayDataList, MemoryAllocator allocator = default)
            {
                _arrayDataList = arrayDataList;
                _allocator = allocator;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    _totalLength += arrayData.Length;
                    _totalNullCount += arrayData.NullCount;
                }
            }

            public void Visit(BooleanType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer valueBuffer = ConcatenateBitmapBuffer(1);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, valueBuffer });
            }

            public void Visit(FixedWidthType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer valueBuffer = ConcatenateFixedWidthTypeValueBuffer(type);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, valueBuffer });
            }

            public void Visit(BinaryType type) => ConcatenateVariableBinaryArrayData(type);

            public void Visit(StringType type) => ConcatenateVariableBinaryArrayData(type);

            public void Visit(ListType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateOffsetBuffer();
                ArrayData child = Concatenate(SelectChildren(0), _allocator);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer }, new[] { child });
            }

            public void Visit(StructType type)
            {
                CheckData(type, 1);
                List<ArrayData> children = new List<ArrayData>(type.Fields.Count);

                for (int i = 0; i < type.Fields.Count; i++)
                {
                    children.Add(Concatenate(SelectChildren(i), _allocator));
                }

                Result = new ArrayData(type, _arrayDataList[0].Length, _arrayDataList[0].NullCount, 0, _arrayDataList[0].Buffers, children);
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"Concatenation for {type.Name} is not supported yet.");
            }

            private void CheckData(IArrowType type, int expectedBufferCount)
            {
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    arrayData.EnsureDataType(type.TypeId);
                    arrayData.EnsureBufferCount(expectedBufferCount);
                }
            }

            private void ConcatenateVariableBinaryArrayData(IArrowType type)
            {
                CheckData(type, 3);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateOffsetBuffer();
                ArrowBuffer valueBuffer = ConcatenateVariableBinaryValueBuffer();

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer, valueBuffer });
            }

            private ArrowBuffer ConcatenateValidityBuffer()
            {
                if (_totalNullCount == 0)
                {
                    return ArrowBuffer.Empty;
                }

                return ConcatenateBitmapBuffer(0);
            }

            private ArrowBuffer ConcatenateBitmapBuffer(int bufferIndex)
            {
                var builder = new ArrowBuffer.BitmapBuilder(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    ReadOnlySpan<byte> span = arrayData.Buffers[bufferIndex].Span;

                    builder.Append(span, length);
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateFixedWidthTypeValueBuffer(FixedWidthType type)
            {
                int typeByteWidth = type.BitWidth / 8;
                var builder = new ArrowBuffer.Builder<byte>(_totalLength * typeByteWidth);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    int byteLength = length * typeByteWidth;

                    builder.Append(arrayData.Buffers[1].Span.Slice(0, byteLength));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateVariableBinaryValueBuffer()
            {
                var builder = new ArrowBuffer.Builder<byte>();

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int lastOffset = arrayData.Buffers[1].Span.CastTo<int>()[arrayData.Length];
                    builder.Append(arrayData.Buffers[2].Span.Slice(0, lastOffset));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<int>(_totalLength + 1);
                int baseOffset = 0;

                builder.Append(0);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    // The first offset is always 0.
                    // It should be skipped because it duplicate to the last offset of builder.
                    ReadOnlySpan<int> span = arrayData.Buffers[1].Span.CastTo<int>().Slice(1, arrayData.Length);

                    foreach (int offset in span)
                    {
                        builder.Append(baseOffset + offset);
                    }

                    // The next offset must start from the current last offset.
                    baseOffset += span[arrayData.Length - 1];
                }

                return builder.Build(_allocator);
            }

            private List<ArrayData> SelectChildren(int index)
            {
                var children = new List<ArrayData>(_arrayDataList.Count);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    children.Add(arrayData.Children[index]);
                }

                return children;
            }
        }
    }
}
