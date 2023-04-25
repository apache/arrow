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
        Memory<byte> Memory { get; }

        int ValueBitSize { get; }

        IBufferBuilder AppendBit(bool bit);
        IBufferBuilder AppendBits(ReadOnlySpan<bool> bits);

        IBufferBuilder AppendByte(byte byteValue);
        IBufferBuilder AppendBytes(ReadOnlySpan<byte> bytes);

        IBufferBuilder AppendStruct(bool value);
        IBufferBuilder AppendStruct(byte value);
        IBufferBuilder AppendStruct<T>(T value) where T : struct;

        IBufferBuilder AppendStructs(ReadOnlySpan<bool> values);
        IBufferBuilder AppendStructs(ReadOnlySpan<byte> values);
        IBufferBuilder AppendStructs<T>(ReadOnlySpan<T> values) where T : struct;

        /// <summary>
        /// Reserve a given number of items' additional capacity.
        /// </summary>
        /// <param name="numBytes">Number of new bytes.</param>
        IBufferBuilder ReserveBytes(int numBytes);

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
        /// <param name="numBytes">Number of bytes.</param>
        IBufferBuilder ResizeBytes(int numBytes);

        /// <summary>
        /// Clear all contents appended so far.
        /// </summary>
        IBufferBuilder Clear();

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

    public interface IValueBufferBuilder<T> : IBufferBuilder where T : struct
    {
        IValueBufferBuilder<T> AppendValue(T value);
        IValueBufferBuilder<T> AppendValue(T? value);
        IValueBufferBuilder<T> AppendValues(ReadOnlySpan<T> values);
        IValueBufferBuilder<T> AppendValues(ReadOnlySpan<T?> values);
    }
}

