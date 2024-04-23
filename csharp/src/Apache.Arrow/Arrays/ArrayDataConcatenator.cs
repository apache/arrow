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
                    _totalNullCount += arrayData.GetNullCount();
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
                ArrowBuffer sizesBuffer = ConcatenateFixedWidthTypeValueBuffer(2, Int32Type.Default);

                var children = new List<ArrayData>(_arrayDataList.Count);
                var offsetsBuilder = new ArrowBuffer.Builder<int>(_totalLength);
                int baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    var child = arrayData.Children[0];
                    ReadOnlySpan<int> offsets = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length);
                    ReadOnlySpan<int> sizes = arrayData.Buffers[2].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length);
                    var minOffset = offsets[0];
                    var maxEnd = 0;

                    for (int i = 0; i < arrayData.Length; ++i)
                    {
                        minOffset = Math.Min(minOffset, offsets[i]);
                        maxEnd = Math.Max(maxEnd, offsets[i] + sizes[i]);
                    }

                    foreach (int offset in offsets)
                    {
                        offsetsBuilder.Append(baseOffset + offset - minOffset);
                    }

                    var childLength = maxEnd - minOffset;
                    if (minOffset != 0 || childLength != child.Length)
                    {
                        child = child.Slice(minOffset, childLength);
                    }

                    baseOffset += childLength;
                    children.Add(child);
                }

                ArrowBuffer offsetBuffer = offsetsBuilder.Build(_allocator);
                ArrayData combinedChild = Concatenate(children, _allocator);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer, sizesBuffer }, new[] { combinedChild });
            }

            public void Visit(FixedSizeListType type)
            {
                CheckData(type, 1);
                var listSize = type.ListSize;
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();

                var children = new List<ArrayData>(_arrayDataList.Count);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    var offset = arrayData.Offset;
                    var length = arrayData.Length;
                    var child = arrayData.Children[0];
                    if (offset != 0 || child.Length != length * listSize)
                    {
                        child = child.Slice(offset * listSize, length * listSize);
                    }

                    children.Add(child);
                }

                ArrayData combinedChild = Concatenate(children, _allocator);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer }, new[] { combinedChild });
            }

            public void Visit(StructType type)
            {
                CheckData(type, 1);
                ArrowBuffer validityBuffer = ConcatenateValidityBuffer();
                List<ArrayData> children = new List<ArrayData>(type.Fields.Count);

                for (int i = 0; i < type.Fields.Count; i++)
                {
                    children.Add(Concatenate(SelectSlicedChildren(i), _allocator));
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
                    // For dense mode, the offsets aren't adjusted so are into the non-sliced child arrays
                    var fieldChildren = type.Mode == UnionMode.Sparse
                        ? SelectSlicedChildren(i)
                        : SelectChildren(i);
                    children.Add(Concatenate(fieldChildren, _allocator));
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

                var children = new List<ArrayData>(_arrayDataList.Count);
                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    var child = arrayData.Children[0];
                    ReadOnlySpan<int> offsets = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length + 1);
                    var firstOffset = offsets[0];
                    var lastOffset = offsets[arrayData.Length];
                    if (firstOffset != 0 || lastOffset != child.Length)
                    {
                        child = child.Slice(firstOffset, lastOffset - firstOffset);
                    }

                    children.Add(child);
                }

                ArrayData combinedChild = Concatenate(children, _allocator);

                Result = new ArrayData(type, _totalLength, _totalNullCount, 0, new ArrowBuffer[] { validityBuffer, offsetBuffer }, new[] { combinedChild });
            }

            private ArrowBuffer ConcatenateValidityBuffer()
            {
                if (_totalNullCount == 0)
                {
                    return ArrowBuffer.Empty;
                }

                var builder = new ArrowBuffer.BitmapBuilder(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    int offset = arrayData.Offset;
                    ReadOnlySpan<byte> span = arrayData.Buffers[0].Span;

                    if (length > 0 && span.Length == 0)
                    {
                        if (arrayData.NullCount == 0)
                        {
                            builder.AppendRange(true , length);
                        }
                        else if (arrayData.NullCount == length)
                        {
                            builder.AppendRange(false , length);
                        }
                        else
                        {
                            throw new Exception("Array has no validity buffer and null count != 0 or length");
                        }
                    }
                    else if (offset == 0)
                    {
                        builder.Append(span, length);
                    }
                    else
                    {
                        for (int i = 0; i < length; ++i)
                        {
                            builder.Append(BitUtility.GetBit(span, offset + i));
                        }
                    }
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateBitmapBuffer(int bufferIndex)
            {
                var builder = new ArrowBuffer.BitmapBuilder(_totalLength);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int length = arrayData.Length;
                    int offset = arrayData.Offset;
                    ReadOnlySpan<byte> span = arrayData.Buffers[bufferIndex].Span;

                    if (offset == 0)
                    {
                        builder.Append(span, length);
                    }
                    else
                    {
                        for (int i = 0; i < length; ++i)
                        {
                            builder.Append(BitUtility.GetBit(span, offset + i));
                        }
                    }
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateFixedWidthTypeValueBuffer(int bufferIndex, FixedWidthType type)
            {
                int typeByteWidth = type.BitWidth / 8;
                var builder = new ArrowBuffer.Builder<byte>(_totalLength * typeByteWidth);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    int byteLength = arrayData.Length * typeByteWidth;
                    int byteOffset = arrayData.Offset * typeByteWidth;

                    builder.Append(arrayData.Buffers[bufferIndex].Span.Slice(byteOffset, byteLength));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateVariableBinaryValueBuffer()
            {
                var builder = new ArrowBuffer.Builder<byte>();

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    var offsets = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length + 1);
                    var firstOffset = offsets[0];
                    var lastOffset = offsets[arrayData.Length];
                    builder.Append(arrayData.Buffers[2].Span.Slice(firstOffset, lastOffset - firstOffset));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<int>(_totalLength + 1);
                int baseOffset = 0;

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    if (arrayData.Length == 0)
                    {
                        continue;
                    }

                    ReadOnlySpan<int> span = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length + 1);
                    // First offset may be non-zero for sliced arrays
                    var firstOffset = span[0];

                    foreach (int offset in span.Slice(0, arrayData.Length))
                    {
                        builder.Append(baseOffset + offset - firstOffset);
                    }

                    baseOffset += span[arrayData.Length] - firstOffset;
                }

                builder.Append(baseOffset);

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

                    ReadOnlySpan<BinaryView> span = arrayData.Buffers[1].Span.CastTo<BinaryView>().Slice(arrayData.Offset, arrayData.Length);
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
                    builder.Append(arrayData.Buffers[0].Span.Slice(arrayData.Offset, arrayData.Length));
                }

                return builder.Build(_allocator);
            }

            private ArrowBuffer ConcatenateUnionOffsetBuffer()
            {
                var builder = new ArrowBuffer.Builder<int>(_totalLength);
                var typeCount = _arrayDataList.Count > 0 ? _arrayDataList[0].Children.Length : 0;
                var baseOffsets = new int[typeCount];

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    ReadOnlySpan<byte> typeSpan = arrayData.Buffers[0].Span.Slice(arrayData.Offset, arrayData.Length);
                    ReadOnlySpan<int> offsetSpan = arrayData.Buffers[1].Span.CastTo<int>().Slice(arrayData.Offset, arrayData.Length);
                    for (int i = 0; i < arrayData.Length; ++i)
                    {
                        var typeId = typeSpan[i];
                        builder.Append(checked(baseOffsets[typeId] + offsetSpan[i]));
                    }

                    for (int i = 0; i < typeCount; ++i)
                    {
                        checked
                        {
                            baseOffsets[i] += arrayData.Children[i].Length;
                        }
                    }
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

            private List<ArrayData> SelectSlicedChildren(int index)
            {
                var children = new List<ArrayData>(_arrayDataList.Count);

                foreach (ArrayData arrayData in _arrayDataList)
                {
                    var offset = arrayData.Offset;
                    var length = arrayData.Length;
                    var child = arrayData.Children[index];
                    if (offset != 0 || child.Length != length)
                    {
                        child = child.Slice(offset, length);
                    }

                    children.Add(child);
                }

                return children;
            }
        }
    }
}
