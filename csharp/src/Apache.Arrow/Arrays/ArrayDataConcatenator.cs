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
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using System;
using System.Collections.Generic;

namespace Apache.Arrow
{
    public class ArrayDataConcatenator
    {
        public static ArrayData Concatenate(IReadOnlyList<ArrayData> arrayDataList, MemoryAllocator allocator = default)
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
            IArrowTypeVisitor<BinaryViewType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<StringViewType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<ListViewType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<UnionType>,
            IArrowTypeVisitor<MapType>
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
                ArrowBuffer valueBuffer = ConcatenateFixedWidthTypeValueBuffer(1, type);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, valueBuffer });
            }

            public void Visit(BinaryType type) => ConcatenateVariableBinaryArrayData(type);

            public void Visit(BinaryViewType type) => ConcatenateBinaryViewArrayData(type);

            public void Visit(StringType type) => ConcatenateVariableBinaryArrayData(type);

            public void Visit(StringViewType type) => ConcatenateBinaryViewArrayData(type);

            public void Visit(ListType type) => ConcatenateLists(type);

            public void Visit(ListViewType type)
            {
                CheckData(type, 3);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();

                var offsetsBuilder = new ArrowBuffer.Builder<int>(_totalLength);
                int baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length > 0)
                    {
                        ReadOnlySpan<int> span = arrayData.Buffers[1].Span.CastTo<int>().Slice(0, arrayData.Length);
                        foreach (int offset in span)
                        {
                            offsetsBuilder.Append(baseOffset + offset);
                        }
                    }

                    baseOffset += arrayData.Children[0].Length;
                }

                ArrowBuffer offsetBuffer = offsetsBuilder.Build(_allocator);
                ArrowBuffer sizesBuffer = ConcatenateFixedWidthTypeValueBuffer(2, Int32Type.Default);
                ArrayData child = Concatenate(SelectChildren(0), _allocator);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer, sizesBuffer }, new[] { child });
            }

            public void Visit(FixedSizeListType type)
            {
                CheckData(type, 1);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrayData child = Concatenate(SelectChildren(0), _allocator);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer }, new[] { child });
            }

            public void Visit(StructType type)
            {
                CheckData(type, 1);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                List<ArrayData> children = new List<ArrayData>(type.Fields.Count);

                for (int i = 0; i < type.Fields.Count; i++)
                {
                    children.Add(Concatenate(SelectChildren(i), _allocator));
                }

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer }, children);
            }

            public void Visit(UnionType type)
            {
                int bufferCount = type.Mode switch
                {
                    UnionMode.Sparse => 1,
                    UnionMode.Dense => 2,
                    _ => throw new InvalidOperationException("TODO"),
                };

                CheckData(type, bufferCount);
                List<ArrayData> children = new List<ArrayData>(type.Fields.Count);

                for (int i = 0; i < type.Fields.Count; i++)
                {
                    children.Add(Concatenate(SelectChildren(i), _allocator));
                }

                ArrowBuffer[] buffers = new ArrowBuffer[bufferCount];
                buffers[0] = ConcatenateUnionTypeBuffer();
                if (bufferCount > 1)
                {
                    buffers[1] = ConcatenateUnionOffsetBuffer();
                }

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, buffers, children);
            }

            public void Visit(MapType type) => ConcatenateLists(type.UnsortedKey()); /* Can't tell if the output is still sorted */

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

            private void CheckDataVariadicCount(IArrowType type, int expectedBufferCount)
            {
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    arrayData.EnsureDataType(type.TypeId);
                    arrayData.EnsureVariadicBufferCount(expectedBufferCount);
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

            private void ConcatenateBinaryViewArrayData(IArrowType type)
            {
                CheckDataVariadicCount(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer viewBuffer = ConcatenateViewBuffer(out int variadicBufferCount);
                ArrowBuffer[] buffers = new ArrowBuffer[2 + variadicBufferCount];
                buffers[0] = validityBuffer;
                buffers[1] = viewBuffer;
                int index = 2;
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    for (int i = 2; i < arrayData.Buffers.Length; i++)
                    {
                        buffers[index++] = arrayData.Buffers[i];
                    }
                }

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, buffers);
            }

            private void ConcatenateLists(NestedType type)
            {
                CheckData(type, 2);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                ArrowBuffer offsetBuffer = ConcatenateOffsetBuffer();
                ArrayData child = Concatenate(SelectChildren(0), _allocator);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer }, new[] { child });
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

            private ArrowBuffer ConcatenateFixedWidthTypeValueBuffer(int bufferIndex, FixedWidthType type)
            {
                int typeByteWidth = type.BitWidth / 8;
                var builder = new ArrowBuffer.Builder<byte>(_totalLength * typeByteWidth);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    int byteLength = length * typeByteWidth;

                    builder.Append(arrayData.Buffers[bufferIndex].Span.Slice(0, byteLength));
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

            private ArrowBuffer ConcatenateViewBuffer(out int variadicBufferCount)
            {
                var builder = new ArrowBuffer.Builder<BinaryView>(_totalLength);
                variadicBufferCount = 0;
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    ReadOnlySpan<BinaryView> span = arrayData.Buffers[1].Span.CastTo<BinaryView>().Slice(0, arrayData.Length);
                    foreach (BinaryView view in span)
                    {
                        if (view.Length > BinaryView.MaxInlineLength)
                        {
                            builder.Append(view.AdjustBufferIndex(variadicBufferCount));
                        }
                        else
                        {
                            builder.Append(view);
                        }
                    }

                    variadicBufferCount += (arrayData.Buffers.Length - 2);
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateUnionTypeBuffer()
            {
                var builder = new ArrowBuffer.Builder<byte>(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    builder.Append(arrayData.Buffers[0]);
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateUnionOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<int>(_totalLength);
                int baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    ReadOnlySpan<int> span = arrayData.Buffers[1].Span.CastTo<int>();
                    foreach (int offset in span)
                    {
                        builder.Append(baseOffset + offset);
                    }

                    // The next offset must start from the current last offset.
                    baseOffset += span[arrayData.Length];
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
