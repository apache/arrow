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
using System.Runtime.CompilerServices;
using System.Collections;

namespace Apache.Arrow
{
    public class BinaryViewArray : Array, IReadOnlyList<byte[]>
    {
        public class Builder : BuilderBase<BinaryViewArray, Builder>
        {
            public Builder() : base(BinaryViewType.Default) { }
            public Builder(IArrowType dataType) : base(dataType) { }

            protected override BinaryViewArray Build(ArrayData data)
            {
                return new BinaryViewArray(data);
            }
        }

        public BinaryViewArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.BinaryView);
            data.EnsureVariadicBufferCount(2);
        }

        public BinaryViewArray(ArrowTypeId typeId, ArrayData data)
            : base(data)
        {
            data.EnsureDataType(typeId);
            data.EnsureVariadicBufferCount(2);
        }

        public abstract class BuilderBase<TArray, TBuilder> : IArrowArrayBuilder<byte, TArray, TBuilder>
            where TArray : IArrowArray
            where TBuilder : class, IArrowArrayBuilder<byte, TArray, TBuilder>
        {
            protected IArrowType DataType { get; }
            protected TBuilder Instance => this as TBuilder;
            protected ArrowBuffer.Builder<BinaryView> BinaryViews { get; }
            protected ArrowBuffer.Builder<byte> ValueBuffer { get; }
            protected ArrowBuffer.BitmapBuilder ValidityBuffer { get; }
            protected int NullCount => this.ValidityBuffer.UnsetBitCount;

            protected BuilderBase(IArrowType dataType)
            {
                DataType = dataType;
                BinaryViews = new ArrowBuffer.Builder<BinaryView>();
                ValueBuffer = new ArrowBuffer.Builder<byte>();
                ValidityBuffer = new ArrowBuffer.BitmapBuilder();
            }

            protected abstract TArray Build(ArrayData data);

            /// <summary>
            /// Gets the length of the array built so far.
            /// </summary>
            public int Length => BinaryViews.Length;

            /// <summary>
            /// Build an Arrow array from the appended contents so far.
            /// </summary>
            /// <param name="allocator">Optional memory allocator.</param>
            /// <returns>Returns an array of type <typeparamref name="TArray"/>.</returns>
            public TArray Build(MemoryAllocator allocator = default)
            {
                bool hasValues = ValueBuffer.Length > 0;
                var bufs = new ArrowBuffer[hasValues ? 3 : 2];
                bufs[0] = NullCount > 0 ? ValidityBuffer.Build(allocator) : ArrowBuffer.Empty;
                bufs[1] = BinaryViews.Build(allocator);
                if (hasValues) { bufs[2] = ValueBuffer.Build(allocator); }

                var data = new ArrayData(
                    DataType,
                    length: Length,
                    NullCount,
                    offset: 0,
                    bufs);

                return Build(data);
            }

            /// <summary>
            /// Append a single null value to the array.
            /// </summary>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder AppendNull()
            {
                // Do not add to the value buffer in the case of a null.
                // Note that we do not need to increment the offset as a result.
                ValidityBuffer.Append(false);
                BinaryViews.Append(default(BinaryView));
                return Instance;
            }

            /// <summary>
            /// Appends a value, consisting of a single byte, to the array.
            /// </summary>
            /// <param name="value">Byte value to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder Append(byte value)
            {
                ValidityBuffer.Append(true);
                Span<byte> buf = stackalloc[] { value };
                BinaryViews.Append(new BinaryView(buf));
                return Instance;
            }

            /// <summary>
            /// Append a value, consisting of a span of bytes, to the array.
            /// </summary>
            /// <remarks>
            /// Note that a single value is added, which consists of arbitrarily many bytes.  If multiple values are
            /// to be added, use the <see cref="AppendRange"/> method.
            /// </remarks>
            /// <param name="span">Span of bytes to add.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder Append(ReadOnlySpan<byte> span)
            {
                if (span.Length > BinaryView.MaxInlineLength)
                {
                    int offset = ValueBuffer.Length;
                    ValueBuffer.Append(span);
                    BinaryViews.Append(new BinaryView(span.Length, span.Slice(0, 4), 0, offset));
                }
                else
                {
                    BinaryViews.Append(new BinaryView(span));
                }
                ValidityBuffer.Append(true);
                return Instance;
            }

            /// <summary>
            /// Append an enumerable collection of single-byte values to the array.
            /// </summary>
            /// <remarks>
            /// Note that this method appends multiple values, each of which is a single byte
            /// </remarks>
            /// <param name="values">Single-byte values to add.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder AppendRange(IEnumerable<byte> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (byte b in values)
                {
                    Append(b);
                }

                return Instance;
            }

            /// <summary>
            /// Append an enumerable collection of values to the array.
            /// </summary>
            /// <param name="values">Values to add.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder AppendRange(IEnumerable<byte[]> values)
            {
                if (values == null)
                {
                    throw new ArgumentNullException(nameof(values));
                }

                foreach (byte[] arr in values)
                {
                    if (arr == null)
                    {
                        AppendNull();
                    }
                    else
                    {
                        Append((ReadOnlySpan<byte>)arr);
                    }
                }

                return Instance;
            }

            public TBuilder Reserve(int capacity)
            {
                // TODO: [ARROW-9366] Reserve capacity in the value buffer in a more sensible way.
                BinaryViews.Reserve(capacity);
                ValueBuffer.Reserve(capacity);
                ValidityBuffer.Reserve(capacity);
                return Instance;
            }

            public TBuilder Resize(int length)
            {
                // TODO: [ARROW-9366] Resize the value buffer to a safe length based on offsets, not `length`.
                BinaryViews.Resize(length);
                ValueBuffer.Resize(length);
                ValidityBuffer.Resize(length);
                return Instance;
            }

            public TBuilder Swap(int i, int j)
            {
                ValidityBuffer.Swap(i, j);
                BinaryView view = BinaryViews.Span[i];
                BinaryViews.Span[i] = BinaryViews.Span[j];
                BinaryViews.Span[j] = view;
                return Instance;
            }

            public TBuilder Set(int index, byte value)
            {
                // TODO: Implement
                throw new NotImplementedException();
            }

            /// <summary>
            /// Clear all contents appended so far.
            /// </summary>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder Clear()
            {
                BinaryViews.Clear();
                ValueBuffer.Clear();
                ValidityBuffer.Clear();
                return Instance;
            }
        }

        public BinaryViewArray(IArrowType dataType, int length,
            ArrowBuffer binaryViewsBuffer,
            ArrowBuffer dataBuffer,
            ArrowBuffer nullBitmapBuffer,
            int nullCount = 0, int offset = 0)
        : this(new ArrayData(dataType, length, nullCount, offset,
            new[] { nullBitmapBuffer, binaryViewsBuffer, dataBuffer }))
        { }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public ArrowBuffer ViewsBuffer => Data.Buffers[1];

        public int DataBufferCount => Data.Buffers.Length - 2;

        public ArrowBuffer DataBuffer(int index) => Data.Buffers[index + 2];

        public ReadOnlySpan<BinaryView> Views => ViewsBuffer.Span.CastTo<BinaryView>().Slice(Offset, Length);

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

            return Views[index].Length;
        }

        /// <summary>
        /// Get the collection of bytes, as a read-only span, at a given index in the array.
        /// </summary>
        /// <remarks>
        /// Note that this method cannot reliably identify null values, which are indistinguishable from empty byte
        /// collection values when seen in the context of this method's return type of <see cref="ReadOnlySpan{Byte}"/>.
        /// Use the <see cref="Array.IsNull"/> method or the <see cref="GetBytes(int, out bool)"/> overload instead
        /// to reliably determine null values.
        /// </remarks>
        /// <param name="index">Index at which to get bytes.</param>
        /// <returns>Returns a <see cref="ReadOnlySpan{Byte}"/> object.</returns>
        /// <exception cref="ArgumentOutOfRangeException">If the index is negative or beyond the length of the array.
        /// </exception>
        public ReadOnlySpan<byte> GetBytes(int index) => GetBytes(index, out _);

        /// <summary>
        /// Get the collection of bytes, as a read-only span, at a given index in the array.
        /// </summary>
        /// <param name="index">Index at which to get bytes.</param>
        /// <param name="isNull">Set to <see langword="true"/> if the value at the given index is null.</param>
        /// <returns>Returns a <see cref="ReadOnlySpan{Byte}"/> object.</returns>
        /// <exception cref="ArgumentOutOfRangeException">If the index is negative or beyond the length of the array.
        /// </exception>
        public ReadOnlySpan<byte> GetBytes(int index, out bool isNull)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            isNull = IsNull(index);

            if (isNull)
            {
                // Note that `return null;` is valid syntax, but would be misleading as `null` in the context of a span
                // is actually returned as an empty span.
                return ReadOnlySpan<byte>.Empty;
            }

            BinaryView binaryView = Views[index];
            if (binaryView.IsInline)
            {
                return ViewsBuffer.Span.Slice(16 * index + 4, binaryView.Length);
            }

            return DataBuffer(binaryView._bufferIndex).Span.Slice(binaryView._bufferOffset, binaryView.Length);
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
    }
}
