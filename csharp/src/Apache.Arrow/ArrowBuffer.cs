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
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow
{
    public partial class ArrowBuffer: IEquatable<ArrowBuffer>
    {
        public ArrowBuffer(Memory<byte> data, int size)
        {
            Memory = data;
            Size = size;
        }

        /// <summary>
        /// Allocates an Arrow buffer from a memory pool.
        /// </summary>
        /// <param name="size">Size of buffer (in bytes) to allocate.</param>
        /// <param name="memoryPool">Memory pool to use for allocation. If null, a default memory pool is used.</param>
        /// <returns></returns>
        public static ArrowBuffer Allocate(int size, MemoryPool memoryPool = null)
        {
            if (memoryPool == null)
                memoryPool = DefaultMemoryPool.Instance.Value;

            var buffer = memoryPool.Allocate(size);

            return new ArrowBuffer(buffer, size);
        }

        /// <summary>
        /// Allocates an Arrow buffer the same length as the incoming data, then
        /// copies the specified data to the arrow buffer.
        /// </summary>
        /// <param name="data">Data to copy into a new arrow buffer.</param>
        /// <param name="memoryPool">Memory pool to use for allocation. If null, a default memory pool is used.</param>
        /// <returns></returns>
        public static ArrowBuffer FromMemory(Memory<byte> data, MemoryPool memoryPool = default)
        {
            var buffer = Allocate(data.Length, memoryPool);
            data.CopyTo(buffer.Memory);
            return buffer;
        }

        public async Task CopyToAsync(Stream stream, CancellationToken cancellationToken = default)
        {
            const float chunkSize = 8192f;

            // TODO: Is there a better copy mechanism to use here that does not involve allocating buffers and targets .NET Standard 1.3?
            // NOTE: Consider specialization for .NET Core 2.1

            var length = Convert.ToInt32(chunkSize);
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            var count = Convert.ToInt32(Math.Ceiling(Memory.Length / chunkSize));
            var offset = 0;

            try
            {
                for (var i = 0; i < count; i++)
                {
                    var n = Math.Min(length, Memory.Length);
                    var slice = Memory.Slice(offset, n);

                    slice.CopyTo(buffer);

                    await stream.WriteAsync(buffer, 0, n, cancellationToken);

                    offset += n;
                }
            }
            finally
            {
                if (buffer != null)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }

        public Memory<byte> Memory { get; }

        public bool IsEmpty => Memory.IsEmpty;

        public int Size { get; }

        public int Capacity => Memory.Length;

        public Span<T> GetSpan<T>(int offset)
            where T : struct =>
            MemoryMarshal.Cast<byte, T>(
                Memory.Span.Slice(offset));

        public Span<T> GetSpan<T>(int offset, int length)
            where T : struct =>
            MemoryMarshal.Cast<byte, T>(
                Memory.Span.Slice(offset, length));

        public Span<T> GetSpan<T>()
            where T: struct =>
            MemoryMarshal.Cast<byte, T>(Memory.Span);

        public bool Equals(ArrowBuffer other)
        {
            var lhs = GetSpan<byte>();
            var rhs = other.GetSpan<byte>();
            return lhs.SequenceEqual(rhs);
        }
    }
}
