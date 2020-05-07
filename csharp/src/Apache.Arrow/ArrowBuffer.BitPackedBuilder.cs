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

namespace Apache.Arrow
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Arrow.Memory;

    public partial struct ArrowBuffer
    {
        /// <summary>
        /// The <see cref="ArrowBuffer.BitPackedBuilder"/> class is a complement to <see cref="ArrowBuffer.Builder{T}"/>
        /// and is designed for boolean fields, which are efficiently bit-packed into byte-aligned memory.
        /// </summary>
        public class BitPackedBuilder
        {
            private const int DefaultBitCapacity = 8;

            /// <summary>
            /// Gets the number of bits of current capacity.
            /// </summary>
            public int BitCapacity { get; private set; }

            /// <summary>
            /// Gets the number of bits currently appended.
            /// </summary>
            public int BitCount { get; private set; }

            /// <summary>
            /// Gets the raw byte memory underpinning the builder.
            /// </summary>
            public Memory<byte> Memory { get; private set; }

            /// <summary>
            /// Gets the span of (bit-packed byte) memory underpinning the builder.
            /// </summary>
            public Span<byte> Span => Memory.Span;

            /// <summary>
            /// Creates an instance of the <see cref="BitPackedBuilder"/> class.
            /// </summary>
            /// <param name="bitCapacity">Number of bits of initial capacity to reserve.</param>
            public BitPackedBuilder(int bitCapacity = DefaultBitCapacity)
            {
                Memory = new byte[BitUtility.ByteCount(bitCapacity)];
                BitCapacity = bitCapacity;
                BitCount = 0;
            }

            /// <summary>
            /// Append a single bit.
            /// </summary>
            /// <param name="bit">Bit to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder Append(bool bit)
            {
                if (BitCount % 8 == 0)
                {
                    // Append a new byte to the buffer when needed.
                    EnsureAdditionalCapacity(1);
                    Span[BitCount / 8] = 0;
                }

                BitUtility.SetBit(Span, BitCount, bit);
                BitCount++;
                return this;
            }

            /// <summary>
            /// Append multiple bits.
            /// </summary>
            /// <param name="bits">Bits to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder AppendRange(IEnumerable<bool> bits)
            {
                if (bits != null)
                {
                    foreach (var v in bits)
                    {
                        Append(v);
                    }
                }

                return this;
            }

            /// <summary>
            /// Count the number of set bits (i.e. set to 1).
            /// </summary>
            /// <returns>Returns the number of set bits.</returns>
            public int CountSetBits() => BitUtility.CountBits(this.Span);

            /// <summary>
            /// Count the number of unset bits (i.e. set to 0).
            /// </summary>
            /// <returns>Returns the number of unset bits.</returns>
            public int CountUnsetBits() => this.BitCount - this.CountSetBits();

            /// <summary>
            /// Toggle the bit at a particular index.
            /// </summary>
            /// <param name="index">Index of bit to toggle.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder Toggle(int index)
            {
                CheckIndex(index);
                BitUtility.ToggleBit(Span, index);
                return this;
            }

            /// <summary>
            /// Set the bit at a particular index to 1.
            /// </summary>
            /// <param name="index">Index of bit to set.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder Set(int index)
            {
                CheckIndex(index);
                BitUtility.SetBit(Span, index);
                return this;
            }

            /// <summary>
            /// Set the bit at a particular index to a given value.
            /// </summary>
            /// <param name="index">Index of bit to set/unset.</param>
            /// <param name="value">Value of bit.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder Set(int index, bool value)
            {
                CheckIndex(index);
                BitUtility.SetBit(Span, index, value);
                return this;
            }

            /// <summary>
            /// Swap the bits at two given indices.
            /// </summary>
            /// <param name="i">First index.</param>
            /// <param name="j">Second index.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder Swap(int i, int j)
            {
                CheckIndex(i);
                CheckIndex(j);
                var bi = BitUtility.GetBit(Span, i);
                var bj = BitUtility.GetBit(Span, j);
                BitUtility.SetBit(Span, i, bj);
                BitUtility.SetBit(Span, j, bi);
                return this;
            }

            /// <summary>
            /// Reserve a given number of bits' additional capacity.
            /// </summary>
            /// <param name="bitAdditionalCapacity">Number of bits of required additional capacity.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder Reserve(int bitAdditionalCapacity)
            {
                if (bitAdditionalCapacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(bitAdditionalCapacity));
                }

                EnsureAdditionalCapacity(bitAdditionalCapacity);
                return this;
            }

            /// <summary>
            /// Resize the buffer to a given size.
            /// </summary>
            /// <remarks>
            /// Note that if the required capacity is smaller than the current length of the populated buffer so far,
            /// the buffer will be truncated and items at the end of the buffer will be lost.
            /// </remarks>
            /// <remarks>
            /// Note also that a negative capacity will result in the buffer being resized to zero.
            /// </remarks>
            /// <param name="bitCapacity">Number of bits of required capacity.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder Resize(int bitCapacity)
            {
                bitCapacity = bitCapacity < 0 ? 0 : bitCapacity;
                EnsureCapacity(bitCapacity);
                BitCount = Math.Max(0, bitCapacity);

                return this;
            }

            /// <summary>
            /// Clear all contents appended so far.
            /// </summary>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public BitPackedBuilder Clear()
            {
                Span.Fill(default);
                BitCount = 0;
                return this;
            }

            /// <summary>
            /// Build an Arrow buffer from the appended contents so far.
            /// </summary>
            /// <param name="allocator">Optional memory allocator.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public ArrowBuffer Build(MemoryAllocator allocator = default)
            {
                var bufferLength = checked((int)BitUtility.RoundUpToMultipleOf64(Memory.Length));
                var memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
                var memoryOwner = memoryAllocator.Allocate(bufferLength);
                Memory.Slice(0, Memory.Length).CopyTo(memoryOwner.Memory);
                return new ArrowBuffer(memoryOwner);
            }

            private void CheckIndex(int index)
            {
                if (index < 0 || index >= BitCount)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }
            }

            private void EnsureAdditionalCapacity(int additionalCapacity)
            {
                EnsureCapacity(checked(BitCount + additionalCapacity));
            }

            private void EnsureCapacity(int requiredCapacity)
            {
                if (requiredCapacity > BitCapacity)
                {
                    // TODO: specifiable growth strategy
                    // Double the length of the in-memory array, or use the byte count of the capacity, whichever is
                    // greater.
                    var byteCount = Math.Max(BitUtility.ByteCount(requiredCapacity), Memory.Length * 2);
                    Reallocate(byteCount);
                    BitCapacity = byteCount * 8;
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