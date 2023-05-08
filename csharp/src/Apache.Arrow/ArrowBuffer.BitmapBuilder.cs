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
using System.Collections.Generic;
using System.Diagnostics;
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    public partial struct ArrowBuffer
    {
        /// <summary>
        /// The <see cref="BitmapBuilder"/> class is a complement to <see cref="ArrowBuffer.Builder{T}"/>
        /// and is designed for boolean fields, which are efficiently bit-packed into byte-aligned memory.
        /// </summary>
        public class BitmapBuilder
        {
            private const int DefaultBitCapacity = 64;

            /// <summary>
            /// Gets the number of bits that can be contained in the memory allocated by the current instance.
            /// </summary>
            public int Capacity { get; private set; }

            /// <summary>
            /// Gets the number of bits currently appended.
            /// </summary>
            public int Length { get; private set; }

            /// <summary>
            /// Gets the raw byte memory underpinning the builder.
            /// </summary>
            public Memory<byte> Memory { get; private set; }

            /// <summary>
            /// Gets the span of (bit-packed byte) memory underpinning the builder.
            /// </summary>
            public Span<byte> Span => Memory.Span;

            /// <summary>
            /// Gets the number of set bits (i.e. set to 1).
            /// </summary>
            public int SetBitCount { get; private set; }

            /// <summary>
            /// Gets the number of unset bits (i.e. set to 0).
            /// </summary>
            public int UnsetBitCount => Length - SetBitCount;

            /// <summary>
            /// Creates an instance of the <see cref="BitmapBuilder"/> class.
            /// </summary>
            /// <param name="capacity">Number of bits of initial capacity to reserve.</param>
            public BitmapBuilder(int capacity = DefaultBitCapacity)
            {
                Memory = new byte[BitUtility.ByteCount(capacity)];
                Capacity = capacity;
            }

            /// <summary>
            /// Append a single bit.
            /// </summary>
            /// <param name="value">Bit to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Append(bool value)
            {
                if (Length % 8 == 0)
                {
                    // Append a new byte to the buffer when needed.
                    EnsureAdditionalCapacity(1);
                }

                BitUtility.SetBit(Span, Length, value);
                Length++;
                SetBitCount += value ? 1 : 0;
                return this;
            }

            /// <summary>
            /// Append a span of bits.
            /// </summary>
            /// <param name="source">Source of bits to append.</param>
            /// <param name="validBits">Number of valid bits in the source span.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Append(ReadOnlySpan<byte> source, int validBits)
            {                
                if (!source.IsEmpty && validBits > source.Length * 8)
                    throw new ArgumentException($"Number of valid bits ({validBits}) cannot be greater than the the source span length ({source.Length * 8} bits).", nameof(validBits));
                
                // Check if memory copy can be used from the source array (performance optimization for byte-aligned coping)
                if (!source.IsEmpty && Length % 8 == 0)
                {
                    EnsureAdditionalCapacity(validBits);
                    source.Slice(0, BitUtility.ByteCount(validBits)).CopyTo(Span.Slice(Length / 8));
                    
                    Length += validBits;
                    SetBitCount += BitUtility.CountBits(source, 0, validBits);
                }
                else
                {
                    for (int i = 0; i < validBits; i++)
                    {
                        Append(source.IsEmpty || BitUtility.GetBit(source, i));
                    }
                }

                return this;
            }

            /// <summary>
            /// Append multiple bits.
            /// </summary>
            /// <param name="values">Bits to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder AppendRange(IEnumerable<bool> values)
            {
                if (values != null)
                {
                    foreach (var v in values)
                    {
                        Append(v);
                    }
                }

                return this;
            }

            /// <summary>
            /// Append multiple bits.
            /// </summary>
            /// <param name="value">Value of bits to append.</param>
            /// <param name="length">Number of times the value should be added.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder AppendRange(bool value, int length)
            {
                if (length < 0)
                    throw new ArgumentOutOfRangeException(nameof(length));

                EnsureAdditionalCapacity(length);
                Span<byte> span = Span;
                BitUtility.SetBits(span, Length, length, value);

                Length += length;
                SetBitCount += value ? length : 0;

                return this;
            }

            /// <summary>
            /// Toggle the bit at a particular index.
            /// </summary>
            /// <param name="index">Index of bit to toggle.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Toggle(int index)
            {
                CheckIndex(index);
                bool priorValue = BitUtility.GetBit(Span, index);
                SetBitCount += priorValue ? -1 : 1;
                BitUtility.ToggleBit(Span, index);
                return this;
            }

            /// <summary>
            /// Set the bit at a particular index to 1.
            /// </summary>
            /// <param name="index">Index of bit to set.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Set(int index)
            {
                CheckIndex(index);
                bool priorValue = BitUtility.GetBit(Span, index);
                SetBitCount += priorValue ? 0 : 1;
                BitUtility.SetBit(Span, index);
                return this;
            }

            /// <summary>
            /// Set the bit at a particular index to a given value.
            /// </summary>
            /// <param name="index">Index of bit to set/unset.</param>
            /// <param name="value">Value of bit.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Set(int index, bool value)
            {
                CheckIndex(index);
                bool priorValue = BitUtility.GetBit(Span, index);
                SetBitCount -= priorValue ? 1 : 0;
                SetBitCount += value ? 1 : 0;
                BitUtility.SetBit(Span, index, value);
                return this;
            }

            /// <summary>
            /// Swap the bits at two given indices.
            /// </summary>
            /// <param name="i">First index.</param>
            /// <param name="j">Second index.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Swap(int i, int j)
            {
                CheckIndex(i);
                CheckIndex(j);
                bool bi = BitUtility.GetBit(Span, i);
                bool bj = BitUtility.GetBit(Span, j);
                BitUtility.SetBit(Span, i, bj);
                BitUtility.SetBit(Span, j, bi);
                return this;
            }

            /// <summary>
            /// Reserve a given number of bits' additional capacity.
            /// </summary>
            /// <param name="additionalCapacity">Number of bits of required additional capacity.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Reserve(int additionalCapacity)
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
            /// <param name="capacity">Number of bits of required capacity.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Resize(int capacity)
            {
                if (capacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be non-negative");
                }

                EnsureCapacity(capacity);
                Length = capacity;

                SetBitCount = BitUtility.CountBits(Span, 0, Length);

                return this;
            }

            /// <summary>
            /// Clear all contents appended so far.
            /// </summary>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitmapBuilder Clear()
            {
                Span.Fill(default);
                Length = 0;
                SetBitCount = 0;
                return this;
            }

            /// <summary>
            /// Build an Arrow buffer from the appended contents so far.
            /// </summary>
            /// <param name="allocator">Optional memory allocator.</param>
            /// <returns>Returns an <see cref="ArrowBuffer"/> object.</returns>
            public ArrowBuffer Build(MemoryAllocator allocator = default)
            {
                int bufferLength = checked((int)BitUtility.RoundUpToMultipleOf64(Memory.Length));
                var memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
                var memoryOwner = memoryAllocator.Allocate(bufferLength);
                Memory.Slice(0, Memory.Length).CopyTo(memoryOwner.Memory);
                return new ArrowBuffer(memoryOwner);
            }

            private void CheckIndex(int index)
            {
                if (index < 0 || index >= Length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }
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
                    int byteCount = Math.Max(BitUtility.ByteCount(requiredCapacity), Memory.Length * 2);
                    Reallocate(byteCount);
                    Capacity = byteCount * 8;
                }
            }

            private void Reallocate(int numBytes)
            {
                if (numBytes != 0)
                {
                    Debug.Assert(numBytes > Memory.Length);
                    var memory = new Memory<byte>(new byte[numBytes]);
                    Memory.CopyTo(memory);

                    Memory = memory;
                }
            }
        }
    }
}
