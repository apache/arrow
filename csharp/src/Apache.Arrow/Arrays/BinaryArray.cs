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
    public class BinaryArray : Array
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
            : base(data)
        {
            data.EnsureDataType(typeId);
            data.EnsureBufferCount(3);
        }

        public abstract class BuilderBase<TArray, TBuilder> : IArrowArrayBuilder<byte, TArray, TBuilder>
            where TArray : IArrowArray
            where TBuilder : class, IArrowArrayBuilder<byte, TArray, TBuilder>
        {
            protected IArrowType DataType { get; }
            protected TBuilder Instance => this as TBuilder;
            protected ArrowBuffer.Builder<int> ValueOffsets { get; }
            protected ArrowBuffer.Builder<byte> ValueBuffer { get; }
            protected BooleanArray.Builder ValidityBuffer { get; }
            protected int Offset { get; set; }
            protected int NullCount { get; private set; }

            protected BuilderBase(IArrowType dataType)
            {
                DataType = dataType;
                ValueOffsets = new ArrowBuffer.Builder<int>();
                ValueBuffer = new ArrowBuffer.Builder<byte>();
                ValidityBuffer = new BooleanArray.Builder();
            }

            protected abstract TArray Build(ArrayData data);

            public int Length => ValueOffsets.Length;

            public TArray Build(MemoryAllocator allocator = default)
            {
                ValueOffsets.Append(Offset);

                ArrowBuffer validityBuffer = NullCount > 0
                                        ? ValidityBuffer.Build(allocator).ValueBuffer
                                        : ArrowBuffer.Empty;

                var data = new ArrayData(DataType, ValueOffsets.Length - 1, NullCount, 0,
                    new[] { validityBuffer, ValueOffsets.Build(allocator), ValueBuffer.Build(allocator) });

                return Build(data);
            }

            public TBuilder AppendNull()
            {
                ValueOffsets.Append(Offset);
                ValidityBuffer.Append(false);
                NullCount++;
                return Instance;
            }

            public TBuilder Append(byte value)
            {
                ValueOffsets.Append(Offset);
                ValueBuffer.Append(value);
                Offset++;
                ValidityBuffer.Append(true);
                return Instance;
            }

            public TBuilder Append(ReadOnlySpan<byte> span)
            {
                ValueOffsets.Append(Offset);
                ValueBuffer.Append(span);
                ValidityBuffer.Append(true);
                Offset += span.Length;
                return Instance;
            }

            public TBuilder AppendRange(IEnumerable<byte[]> values)
            {
                foreach (byte[] arr in values)
                {
                    if (arr == null)
                    {
                        AppendNull();
                        continue;
                    }
                    int len = ValueBuffer.Length;
                    ValueOffsets.Append(Offset);
                    ValueBuffer.Append(arr);
                    ValidityBuffer.Append(true);
                    Offset += ValueBuffer.Length - len;
                }

                return Instance;
            }

            public TBuilder AppendRange(IEnumerable<byte> values)
            {
                if (values == null)
                {
                    return AppendNull();
                }
                int len = ValueBuffer.Length;
                ValueBuffer.AppendRange(values);
                int valOffset = ValueBuffer.Length - len;
                ValueOffsets.Append(Offset);
                Offset += valOffset;
                ValidityBuffer.Append(true);
                return Instance;
            }

            public TBuilder Reserve(int capacity)
            {
                ValueOffsets.Reserve(capacity + 1);
                ValueBuffer.Reserve(capacity);
                ValidityBuffer.Reserve(capacity + 1);
                return Instance;
            }

            public TBuilder Resize(int length)
            {
                ValueOffsets.Resize(length + 1);
                ValueBuffer.Resize(length);
                ValidityBuffer.Resize(length + 1);
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
                ValidityBuffer.Clear();
                return Instance;
            }
        }

        public BinaryArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer,
            ArrowBuffer dataBuffer,
            ArrowBuffer nullBitmapBuffer,
            int nullCount = 0, int offset = 0)
        : this(new ArrayData(dataType, length, nullCount, offset,
            new[] { nullBitmapBuffer, valueOffsetsBuffer, dataBuffer }))
        { }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

        public ArrowBuffer ValueBuffer => Data.Buffers[2];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<int>().Slice(Offset, Length + 1);

        public ReadOnlySpan<byte> Values => ValueBuffer.Span.CastTo<byte>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [Obsolete("This method has been deprecated. Please use ValueOffsets[index] instead.")]
        public int GetValueOffset(int index)
        {
            if (index < 0 || index > Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            return ValueOffsets[index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetValueLength(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            if (!IsValid(index))
            {
                return 0;
            }

            ReadOnlySpan<int> offsets = ValueOffsets;
            return offsets[index + 1] - offsets[index];
        }

        public ReadOnlySpan<byte> GetBytes(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            if (IsNull(index))
            {
                return null;
            }

            return ValueBuffer.Span.Slice(ValueOffsets[index], GetValueLength(index));
        }

    }
}
