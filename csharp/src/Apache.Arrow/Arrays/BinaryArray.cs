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
            protected ArrowBuffer.BitmapBuilder ValidityBuffer { get; }
            protected int Offset { get; set; }
            protected int NullCount => this.ValidityBuffer.UnsetBitCount;

            protected BuilderBase(IArrowType dataType)
            {
                DataType = dataType;
                ValueOffsets = new ArrowBuffer.Builder<int>();
                ValueBuffer = new ArrowBuffer.Builder<byte>();
                ValidityBuffer = new ArrowBuffer.BitmapBuilder();

                // From the docs:
                //
                // The offsets buffer contains length + 1 signed integers (either 32-bit or 64-bit, depending on the
                // logical type), which encode the start position of each slot in the data buffer. The length of the
                // value in each slot is computed using the difference between the offset at that slot’s index and the
                // subsequent offset.
                //
                // In this builder, we choose to append the first offset (zero) upon construction, and each trailing
                // offset is then added after each individual item has been appended.
                ValueOffsets.Append(this.Offset);
            }

            protected abstract TArray Build(ArrayData data);

            /// <summary>
            /// Gets the length of the array built so far.
            /// </summary>
            public int Length => ValueOffsets.Length - 1;

            /// <summary>
            /// Build an Arrow array from the appended contents so far.
            /// </summary>
            /// <param name="allocator">Optional memory allocator.</param>
            /// <returns>Returns an array of type <typeparamref name="TArray"/>.</returns>
            public TArray Build(MemoryAllocator allocator = default)
            {
                var bufs = new[]
                {
                    NullCount > 0 ? ValidityBuffer.Build(allocator) : ArrowBuffer.Empty,
                    ValueOffsets.Build(allocator),
                    ValueBuffer.Build(allocator),
                };
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
                ValueOffsets.Append(Offset);
                return Instance;
            }

            /// <summary>
            /// Appends a value, consisting of a single byte, to the array.
            /// </summary>
            /// <param name="value">Byte value to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder Append(byte value)
            {
                ValueBuffer.Append(value);
                ValidityBuffer.Append(true);
                Offset++;
                ValueOffsets.Append(Offset);
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
                ValueBuffer.Append(span);
                ValidityBuffer.Append(true);
                Offset += span.Length;
                ValueOffsets.Append(Offset);
                return Instance;
            }

            /// <summary>
            /// Append a value, consisting of an enumerable collection of bytes, to the array.
            /// </summary>
            /// <remarks>
            /// Note that this method appends a single value, which may consist of arbitrarily many bytes.  If multiple
            /// values are to be added, use the <see cref="AppendRange(IEnumerable{byte})"/> method instead.
            /// </remarks>
            /// <param name="value">Enumerable collection of bytes to add.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder Append(IEnumerable<byte> value)
            {
                if (value == null)
                {
                    return AppendNull();
                }

                // Note: by looking at the length of the value buffer before and after, we avoid having to iterate
                // through the enumerable multiple times to get both length and contents.
                int priorLength = ValueBuffer.Length;
                ValueBuffer.AppendRange(value);
                int valueLength = ValueBuffer.Length - priorLength;
                Offset += valueLength;
                ValidityBuffer.Append(true);
                ValueOffsets.Append(Offset);
                return Instance;
            }

            /// <summary>
            /// Append an enumerable collection of single-byte values to the array.
            /// </summary>
            /// <remarks>
            /// Note that this method appends multiple values, each of which is a single byte.  If a single value is
            /// to be added, use the <see cref="Append(IEnumerable{byte})"/> method instead.
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
                ValueOffsets.Reserve(capacity + 1);
                ValueBuffer.Reserve(capacity);
                ValidityBuffer.Reserve(capacity + 1);
                return Instance;
            }

            public TBuilder Resize(int length)
            {
                // TODO: [ARROW-9366] Resize the value buffer to a safe length based on offsets, not `length`.
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

            /// <summary>
            /// Clear all contents appended so far.
            /// </summary>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public TBuilder Clear()
            {
                ValueOffsets.Clear();
                ValueBuffer.Clear();
                ValidityBuffer.Clear();

                // Always write the first offset before anything has been written.
                Offset = 0;
                ValueOffsets.Append(Offset);
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

            return ValueBuffer.Span.Slice(ValueOffsets[index], GetValueLength(index));
        }

    }
}
