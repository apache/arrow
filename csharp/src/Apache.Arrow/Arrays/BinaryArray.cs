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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    public class BinaryArray: Array
    {
        public class Builder : BuilderBase<BinaryArray, Builder>
        {
            public Builder() : base(BinaryType.Default) { }
            public Builder(IArrowType dataType) : base(dataType) { }

            protected override BinaryArray Build(ArrayData data)
            {
                return new BinaryArray(data);
            }
        }

        public BinaryArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Binary);
            data.EnsureBufferCount(3);
        }

        public BinaryArray(ArrowTypeId typeId, ArrayData data)
            : base (data)
        {
            data.EnsureDataType(typeId);
            data.EnsureBufferCount(3);
        }

        public abstract class BuilderBase<TArray, TBuilder>: IArrowArrayBuilder<byte, TArray, TBuilder>
            where TArray: IArrowArray
            where TBuilder: class, IArrowArrayBuilder<byte, TArray, TBuilder>
        {
            protected IArrowType DataType { get; }
            protected TBuilder Instance => this as TBuilder;
            protected ArrowBuffer.Builder<int> ValueOffsets { get; }
            protected ArrowBuffer.Builder<byte> ValueBuffer { get; }
            protected int Offset { get; set; }

            protected BuilderBase(IArrowType dataType)
            {
                DataType = dataType;
                ValueOffsets = new ArrowBuffer.Builder<int>();
                ValueBuffer = new ArrowBuffer.Builder<byte>();
            }

            protected abstract TArray Build(ArrayData data);

            public TArray Build(MemoryAllocator allocator = default)
            {
                ValueOffsets.Append(Offset);

                var data = new ArrayData(DataType, ValueOffsets.Length - 1, 0, 0, 
                    new [] { ArrowBuffer.Empty, ValueOffsets.Build(allocator), ValueBuffer.Build(allocator) });

                return Build(data);
            }

            public TBuilder Append(byte value)
            {
                ValueOffsets.Append(Offset);
                ValueBuffer.Append(value);
                Offset++;
                return Instance;
            }

            public TBuilder Append(ReadOnlySpan<byte> span)
            {
                ValueOffsets.Append(Offset);
                ValueBuffer.Append(span);
                Offset += span.Length;
                return Instance;
            }

            public TBuilder AppendRange(IEnumerable<byte[]> values)
            {
                foreach (var arr in values)
                {
                    var len = ValueBuffer.Length;
                    ValueOffsets.Append(Offset);
                    ValueBuffer.Append(arr);
                    Offset += ValueBuffer.Length - len;
                }

                return Instance;
            }

            public TBuilder AppendRange(IEnumerable<byte> values)
            {
                var len = ValueBuffer.Length;
                ValueOffsets.Append(Offset);
                ValueBuffer.AppendRange(values);
                Offset += ValueBuffer.Length - len;
                return Instance;
            }

            public TBuilder Reserve(int capacity)
            {
                ValueOffsets.Reserve(capacity + 1);
                ValueBuffer.Reserve(capacity);
                return Instance;
            }

            public TBuilder Resize(int length)
            {
                ValueOffsets.Resize(length + 1);
                ValueBuffer.Resize(length);
                return Instance;
            }

            public TBuilder Swap(int i, int j)
            {
                // TODO: Implement
                throw new NotImplementedException();
            }

            public TBuilder Set(int index, byte value)
            {
                // TODO: Implement
                throw new NotImplementedException();
            }

            public TBuilder Clear()
            {
                ValueOffsets.Clear();
                ValueBuffer.Clear();
                return Instance;
            }
        }

        public BinaryArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer,
            ArrowBuffer dataBuffer,
            ArrowBuffer nullBitmapBuffer,
            int nullCount = 0, int offset = 0)
        : this(new ArrayData(dataType, length, nullCount, offset, 
            new [] { nullBitmapBuffer, valueOffsetsBuffer, dataBuffer }))
        { }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

        public ArrowBuffer ValueBuffer => Data.Buffers[2];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<int>().Slice(0, Length + 1);

        public ReadOnlySpan<byte> Values => ValueBuffer.Span.CastTo<byte>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetValueOffset(int index)
        {
            return ValueOffsets[Offset + index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetValueLength(int index)
        {
            var offsets = ValueOffsets;
            var offset = Offset + index;

            return offsets[offset + 1] - offsets[offset];
        }

        public ReadOnlySpan<byte> GetBytes(int index)
        {
            var offset = GetValueOffset(index);
            var length = GetValueLength(index);
            
            return ValueBuffer.Span.Slice(offset, length);
        }

    }
}
