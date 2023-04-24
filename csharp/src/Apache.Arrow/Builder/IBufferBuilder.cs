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
using Apache.Arrow.Memory;

namespace Apache.Arrow.Builder
{
    public interface IBufferBuilder
    {
        int ValueLength { get; }
        Memory<byte> Memory { get; }
        
        int ValueBitSize { get; }
        int Capacity { get; }

        void AppendBit(bool bit);

        void AppendBits(ReadOnlySpan<bool> bits);
        void AppendBits(ReadOnlySpan<bool> bits, int length);

        void AppendByte(byte byteValue);
        void AppendByte(byte byteValue, int length);

        void AppendBytes(ReadOnlySpan<byte> bytes);
        void AppendBytes(ReadOnlySpan<byte> bytes, int length);
        
        /// <summary>
        /// Reserve a given number of items' additional capacity.
        /// </summary>
        /// <param name="additionalCapacity">Number of items of required additional capacity.</param>
        void Reserve(int additionalCapacity);

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
        void Resize(int capacity);

        /// <summary>
        /// Clear all contents appended so far.
        /// </summary>
        void Clear();

        /// <summary>
        /// Build an Arrow buffer from the appended contents so far.
        /// </summary>
        /// <param name="allocator">Optional memory allocator.</param>
        /// <returns>Returns an <see cref="ArrowBuffer"/> object.</returns>
        ArrowBuffer Build(MemoryAllocator allocator = default);

        /// <summary>
        /// Build an Arrow buffer from the appended contents so far of the specified byte size.
        /// </summary>
        /// <param name="allocator">Optional memory allocator.</param>
        /// <returns>Returns an <see cref="ArrowBuffer"/> object.</returns>
        ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default);
    }

    public interface IStructBufferBuilder<T> : IBufferBuilder where T : struct
    {
        void AppendNull();
        void AppendValue(T value);
        void AppendValues(ReadOnlySpan<T> values);
    }
}

