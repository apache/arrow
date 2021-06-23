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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Apache.Arrow
{
    public partial struct ArrowBuffer
    {
        /// <summary>
        /// The <see cref="Builder{T}"/> class is able to append value-type items, with fluent-style methods, to build
        /// up an <see cref="ArrowBuffer"/> of contiguous items.
        /// </summary>
        /// <remarks>
        /// Note that <see cref="bool"/> is not supported as a generic type argument for this class.  Please use
        /// <see cref="BitmapBuilder"/> instead.
        /// </remarks>
        /// <typeparam name="T">Value-type of item to build into a buffer.</typeparam>
        public class Builder<T>
            where T : struct
        {
            private const int DefaultCapacity = 8;

            private readonly int _size;

            /// <summary>
            /// Gets the number of items that can be contained in the memory allocated by the current instance.
            /// </summary>
            public int Capacity => Memory.Length / _size;

            /// <summary>
            /// Gets the number of items currently appended.
            /// </summary>
            public int Length { get; private set; }

            /// <summary>
            /// Gets the raw byte memory underpinning the builder.
            /// </summary>
            public Memory<byte> Memory { get; private set; }

            /// <summary>
            /// Gets the span of memory underpinning the builder.
            /// </summary>
            public Span<T> Span
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Memory.Span.CastTo<T>();
            }

            /// <summary>
            /// Creates an instance of the <see cref="Builder{T}"/> class.
            /// </summary>
            /// <param name="capacity">Number of items of initial capacity to reserve.</param>
            public Builder(int capacity = DefaultCapacity)
            {
                // Using `bool` as the template argument, if used in an unrestricted fashion, would result in a buffer
                // with inappropriate contents being produced.  Because C# does not support template specialisation,
                // and because generic type constraints do not support negation, we will throw a runtime error to
                // indicate that such a template type is not supported.
                if (typeof(T) == typeof(bool))
                {
                    throw new NotSupportedException(
                        $"An instance of {nameof(Builder<T>)} cannot be instantiated, as `bool` is not an " +
                        $"appropriate generic type to use with this class - please use {nameof(BitmapBuilder)} " +
                        $"instead");
                }

                _size = Unsafe.SizeOf<T>();

                Memory = new byte[capacity * _size];
                Length = 0;
            }

            /// <summary>
            /// Append a buffer, assumed to contain items of the same type.
            /// </summary>
            /// <param name="buffer">Buffer to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Append(ArrowBuffer buffer)
            {
                Append(buffer.Span.CastTo<T>());
                return this;
            }

            /// <summary>
            /// Append a single item.
            /// </summary>
            /// <param name="value">Item to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Append(T value)
            {
                EnsureAdditionalCapacity(1);
                Span[Length++] = value;
                return this;
            }

            /// <summary>
            /// Append a span of items.
            /// </summary>
            /// <param name="source">Source of item span.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Append(ReadOnlySpan<T> source)
            {
                EnsureAdditionalCapacity(source.Length);
                source.CopyTo(Span.Slice(Length, source.Length));
                Length += source.Length;
                return this;
            }

            /// <summary>
            /// Append a number of items.
            /// </summary>
            /// <param name="values">Items to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> AppendRange(IEnumerable<T> values)
            {
                if (values != null)
                {
                    foreach (T v in values)
                    {
                        Append(v);
                    }
                }

                return this;
            }

            /// <summary>
            /// Reserve a given number of items' additional capacity.
            /// </summary>
            /// <param name="additionalCapacity">Number of items of required additional capacity.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Reserve(int additionalCapacity)
            {
                if (additionalCapacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(additionalCapacity));
                }

                EnsureAdditionalCapacity(additionalCapacity);
                return this;
            }

            /// <summary>
            /// Resize the buffer to a given size.
            /// </summary>
            /// <remarks>
            /// Note that if the required capacity is larger than the current length of the populated buffer so far,
            /// the buffer's contents in the new, expanded region are undefined.
            /// </remarks>
            /// <remarks>
            /// Note that if the required capacity is smaller than the current length of the populated buffer so far,
            /// the buffer will be truncated and items at the end of the buffer will be lost.
            /// </remarks>
            /// <param name="capacity">Number of items of required capacity.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Resize(int capacity)
            {
                if (capacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be non-negative");
                }

                EnsureCapacity(capacity);
                Length = capacity;

                return this;
            }

            /// <summary>
            /// Clear all contents appended so far.
            /// </summary>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Clear()
            {
                Span.Fill(default);
                Length = 0;
                return this;
            }

            /// <summary>
            /// Build an Arrow buffer from the appended contents so far.
            /// </summary>
            /// <param name="allocator">Optional memory allocator.</param>
            /// <returns>Returns an <see cref="ArrowBuffer"/> object.</returns>
            public ArrowBuffer Build(MemoryAllocator allocator = default)
            {
                return Build(64, allocator);
            }

            /// <summary>
            /// Build an Arrow buffer from the appended contents so far of the specified byte size.
            /// </summary>
            /// <param name="allocator">Optional memory allocator.</param>
            /// <returns>Returns an <see cref="ArrowBuffer"/> object.</returns>
            internal ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default)
            {
                int currentBytesLength = Length * _size;
                int bufferLength = checked((int)BitUtility.RoundUpToMultiplePowerOfTwo(currentBytesLength, byteSize));

                MemoryAllocator memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
                IMemoryOwner<byte> memoryOwner = memoryAllocator.Allocate(bufferLength);
                Memory.Slice(0, currentBytesLength).CopyTo(memoryOwner.Memory);

                return new ArrowBuffer(memoryOwner);
            }

            private void EnsureAdditionalCapacity(int additionalCapacity)
            {
                EnsureCapacity(checked(Length + additionalCapacity));
            }

            private void EnsureCapacity(int requiredCapacity)
            {
                if (requiredCapacity > Capacity)
                {
                    // TODO: specifiable growth strategy
                    // Double the length of the in-memory array, or use the byte count of the capacity, whichever is
                    // greater.
                    int capacity = Math.Max(requiredCapacity * _size, Memory.Length * 2);
                    Reallocate(capacity);
                }
            }

            private void Reallocate(int numBytes)
            {
                if (numBytes != 0)
                {
                    var memory = new Memory<byte>(new byte[numBytes]);
                    Memory.CopyTo(memory);

                    Memory = memory;
                }
            }

        }

    }
}
