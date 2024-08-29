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
using System.Collections;
using System.Collections.Generic;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Arrays
{
    public class FixedSizeBinaryArray : Array, IReadOnlyList<byte[]>
    {
        public FixedSizeBinaryArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.FixedSizedBinary);
            data.EnsureBufferCount(2);
        }

        public FixedSizeBinaryArray(ArrowTypeId typeId, ArrayData data)
            : base(data)
        {
            data.EnsureDataType(typeId);
            data.EnsureBufferCount(2);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public ArrowBuffer ValueBuffer => Data.Buffers[1];

        /// <summary>
        /// Get the collection of bytes, as a read-only span, at a given index in the array.
        /// </summary>
        /// <remarks>
        /// Note that this method cannot reliably identify null values, which are indistinguishable from empty byte
        /// collection values when seen in the context of this method's return type of <see cref="ReadOnlySpan{Byte}"/>.
        /// Use the <see cref="Array.IsNull"/> method instead to reliably determine null values.
        /// </remarks>
        /// <param name="index">Index at which to get bytes.</param>
        /// <returns>Returns a <see cref="ReadOnlySpan{Byte}"/> object.</returns>
        /// <exception cref="ArgumentOutOfRangeException">If the index is negative or beyond the length of the array.
        /// </exception>
        public ReadOnlySpan<byte> GetBytes(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            if (IsNull(index))
            {
                // Note that `return null;` is valid syntax, but would be misleading as `null` in the context of a span
                // is actually returned as an empty span.
                return ReadOnlySpan<byte>.Empty;
            }

            int size = ((FixedSizeBinaryType)Data.DataType).ByteWidth;
            return ValueBuffer.Span.Slice(index * size, size);
        }

        int IReadOnlyCollection<byte[]>.Count => Length;
        byte[] IReadOnlyList<byte[]>.this[int index] => GetBytes(index).ToArray();

        IEnumerator<byte[]> IEnumerable<byte[]>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetBytes(index).ToArray();
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<byte[]>)this).GetEnumerator();

        public abstract class BuilderBase<TArray, TBuilder> : IArrowArrayBuilder<byte[], TArray, TBuilder>
            where TArray : IArrowArray
            where TBuilder : class, IArrowArrayBuilder<byte[], TArray, TBuilder>
        {
            protected IArrowType DataType { get; }
            protected TBuilder Instance => this as TBuilder;
            protected int ByteWidth { get; }
            protected ArrowBuffer.Builder<byte> ValueBuffer { get; }
            protected ArrowBuffer.BitmapBuilder ValidityBuffer { get; }
            public int Length => ValueBuffer.Length / ByteWidth;
            protected int NullCount => this.ValidityBuffer.UnsetBitCount;
            protected abstract TArray Build(ArrayData data);

            protected BuilderBase(IArrowType dataType, int byteWidth)
            {
                DataType = dataType;
                ByteWidth = byteWidth;
                ValueBuffer = new ArrowBuffer.Builder<byte>();
                ValidityBuffer = new ArrowBuffer.BitmapBuilder();
            }

            public TArray Build(MemoryAllocator allocator = default)
            {
                var bufs = new[]
                {
                    NullCount > 0 ? ValidityBuffer.Build(allocator) : ArrowBuffer.Empty,
                    ValueBuffer.Build(ByteWidth, allocator),
                };
                var data = new ArrayData(
                    DataType,
                    Length,
                    NullCount,
                    0,
                    bufs);

                return Build(data);
            }

            public TBuilder Reserve(int capacity)
            {
                ValueBuffer.Reserve(capacity * ByteWidth);
                ValidityBuffer.Reserve(capacity);
                return Instance;
            }

            public TBuilder Resize(int length)
            {
                ValueBuffer.Resize(length * ByteWidth);
                ValidityBuffer.Resize(length);
                return Instance;
            }

            public TBuilder Clear() {

                ValueBuffer.Clear();
                ValidityBuffer.Clear();

                return Instance;
            }

            public TBuilder Append(byte[] value)
            {
                if(value.Length % ByteWidth != 0)
                    throw new ArgumentOutOfRangeException("Bytes of length: " + value.Length + " do not conform to the fixed size: " + ByteWidth);
                return Append(value.AsSpan());
            }
            public TBuilder Append(ReadOnlySpan<byte[]> span)
            {
                foreach (var b in span)
                {
                    Append(b);
                }

                return Instance;
            }

            public TBuilder AppendRange(IEnumerable<byte[]> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (byte[] b in values)
                {
                    Append(b);
                }

                return Instance;
            }

            public TBuilder Append(ReadOnlySpan<byte> span)
            {
                ValueBuffer.Append(span);
                ValidityBuffer.Append(true);
                return Instance;
            }

            public TBuilder AppendNull()
            {
                ValueBuffer.Append(new byte[ByteWidth]);
                ValidityBuffer.Append(false);
                return Instance;
            }

            public TBuilder Swap(int i, int j)
            {
                int iStart = i * ByteWidth;
                int jStart = j * ByteWidth;
                byte[] iBytes = ValueBuffer.Span.Slice(iStart, ByteWidth).ToArray();
                Span<byte> jBytes = ValueBuffer.Span.Slice(jStart, ByteWidth);

                for (int m = 0; m < ByteWidth; m++)
                {
                    ValueBuffer.Span[iStart + m] = jBytes[m];
                    ValueBuffer.Span[jStart + m] = iBytes[m];
                }

                ValidityBuffer.Swap(i, j);
                return Instance;
            }

            public TBuilder Set(int index, byte[] value)
            {
                return Set(index, value.AsSpan());
            }

            public TBuilder Set(int index, ReadOnlySpan<byte> value)
            {
                int startIndex = index * ByteWidth;
                for (int i = 0; i < ByteWidth; i++)
                {
                    ValueBuffer.Span[startIndex + i] = value[i];
                }

                ValidityBuffer.Set(index, true);
                return Instance;
            }

            public TBuilder SetNull(int index)
            {
                int startIndex = index * ByteWidth;
                for (int i = 0; i < ByteWidth; i++)
                {
                    ValueBuffer.Span[startIndex + i] = 0;
                }

                ValidityBuffer.Set(index, false);
                return Instance;
            }
        }
    }
}
