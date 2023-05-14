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
using System.Threading.Tasks;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Builder
{
    public interface IBufferBuilder
    {
        /// <summary>
        /// Raw byte Memory buffer.
        /// </summary>
        Memory<byte> Memory { get; }

        /// <summary>
        /// Append bool value too buffer.
        /// </summary>
        /// <param name="bit">bool to append</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendBit(bool bit);

        /// <summary>
        /// Append boolean value too buffer.
        /// </summary>
        /// <param name="bits">booleans to append</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendBits(ReadOnlySpan<bool> bits);

        /// <summary>
        /// Append booleans value too buffer
        /// </summary>
        /// <param name="value">boolean to append</param>
        /// <param name="count">number of time to append</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendBits(bool value, int count);

        /// <summary>
        /// Append empty boolean values too buffer
        /// </summary>
        /// <param name="count">number of time to append</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendEmptyBits(int count);

        /// <summary>
        /// Append byte value too buffer.
        /// </summary>
        /// <param name="value">byte to append</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendByte(byte value);

        /// <summary>
        /// Append byte values too buffer.
        /// </summary>
        /// <param name="bytes">bytes to append</param>
        /// <param name="fixedSize">fixed length</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendFixedSizeBytes(ICollection<byte[]> bytes, int fixedSize, Span<bool> validity, out int nullCount);

        /// <summary>
        /// Append byte values too buffer.
        /// </summary>
        /// <param name="bytes">bytes to append</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendBytes(ReadOnlySpan<byte> bytes);

        /// <summary>
        /// Append byte values too buffer repeated x times.
        /// </summary>
        /// <param name="bytes">bytes value</param>
        /// <param name="count">bytes value</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendBytes(ReadOnlySpan<byte> bytes, int count);

        /// <summary>
        /// Append empty bytes values too buffer
        /// </summary>
        /// <param name="count">number of time to append</param>
        /// <returns>Current <see cref="IBufferBuilder"/></returns>
        IBufferBuilder AppendEmptyBytes(int count);

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
        Task<ArrowBuffer> BuildAsync(MemoryAllocator allocator = default);

        /// <summary>
        /// Build an Arrow buffer from the appended contents so far of the specified byte size.
        /// </summary>
        /// <param name="allocator">Optional memory allocator.</param>
        /// <returns>Returns an <see cref="ArrowBuffer"/> object.</returns>
        ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default);
        Task<ArrowBuffer> BuildAsync(int byteSize, MemoryAllocator allocator = default);
    }

    public interface ITypedBufferBuilder : IBufferBuilder
    {
        int ValueBitSize { get; }
        int ValueByteSize { get; }
        int ValueLength { get; }

        /// <summary>
        /// Reserve a given number of items' additional capacity.
        /// </summary>
        /// <param name="capacity">Number of new values.</param>
        ITypedBufferBuilder Reserve(int capacity);

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
        /// <param name="capacity">Number of values.</param>
        ITypedBufferBuilder Resize(int capacity);
    }

    public interface ITypedBufferBuilder<T> : ITypedBufferBuilder where T : struct
    {
        /// <summary>
        /// Append struct values too buffer.
        /// </summary>
        /// <param name="values">structs to append</param>
        /// <returns>Current <see cref="ITypedBufferBuilder"/></returns>
        ITypedBufferBuilder<T> AppendValues(ReadOnlySpan<T> values);
        ITypedBufferBuilder<T> AppendValues(ReadOnlySpan<bool> values);

        /// <summary>
        /// Append struct value too buffer.
        /// </summary>
        /// <param name="value">struct to append</param>
        /// <returns>Current <see cref="ITypedBufferBuilder"/></returns>
        ITypedBufferBuilder<T> AppendValue(T value);
        ITypedBufferBuilder<T> AppendValue(bool value);

        /// <summary>
        /// Append struct values too buffer
        /// </summary>
        /// <param name="value">struct to append</param>
        /// <param name="count">number of time to append</param>
        /// <returns>Current <see cref="ITypedBufferBuilder"/></returns>
        ITypedBufferBuilder<T> AppendValues(T value, int count);
        ITypedBufferBuilder<T> AppendValues(bool value, int count);

        /// <summary>
        /// Append nullable struct values too buffer.
        /// </summary>
        /// <param name="values">structs to append</param>
        /// <returns>Current <see cref="ITypedBufferBuilder"/></returns>
        ITypedBufferBuilder<T> AppendValues(ICollection<T?> values, Span<bool> validity, out int nullCount);
        ITypedBufferBuilder<T> AppendValues(ICollection<bool?> values, Span<bool> validity, out int nullCount);
    }
}

