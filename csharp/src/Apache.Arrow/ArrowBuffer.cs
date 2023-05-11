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
using System.Buffers;
using System.Runtime.CompilerServices;
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    // See https://arrow.apache.org/docs/cpp/api/memory.html#_CPPv4N5arrow6BufferE

    public readonly partial struct ArrowBuffer : IEquatable<ArrowBuffer>, IDisposable
    {
        private readonly IMemoryOwner<byte> _memoryOwner;
        private readonly ReadOnlyMemory<byte> _memory;

        public static ArrowBuffer Empty => new ArrowBuffer(Memory<byte>.Empty);

        public ArrowBuffer(ReadOnlyMemory<byte> data)
        {
            _memoryOwner = null;
            _memory = data;
        }

        internal ArrowBuffer(IMemoryOwner<byte> memoryOwner)
        {
            // When wrapping an IMemoryOwner, don't cache the Memory<byte>
            // since the owner may be disposed, and the cached Memory would
            // be invalid.

            _memoryOwner = memoryOwner;
            _memory = Memory<byte>.Empty;
        }

        public ReadOnlyMemory<byte> Memory =>
            _memoryOwner != null ? _memoryOwner.Memory : _memory;

        public bool IsEmpty => Memory.IsEmpty;

        public int Length => Memory.Length;

        public ReadOnlySpan<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => AsBytes();
        }

        public ArrowBuffer Clone(MemoryAllocator allocator = default)
        {
            return new Builder.BufferBuilder(Span.Length)
                .AppendBytes(Span)
                .Build(allocator);
        }

        public bool Equals(ArrowBuffer other)
        {
            return Span.SequenceEqual(other.Span);
        }

        public void Dispose()
        {
            _memoryOwner?.Dispose();
        }

        /// <summary>
        /// Forms a slice out of the given memory, beginning at 'start', of given length
        /// </summary>
        /// <param name="start">The index at which to begin this slice.</param>
        /// <param name="length">The desired length for the slice (exclusive).</param>
        /// <exception cref="System.ArgumentOutOfRangeException">
        /// Thrown when the specified <paramref name="start"/> or end index is not in range (&lt;0 or &gt;Length).
        /// </exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ArrowBuffer Slice(int start, int length) => new(Memory.Slice(start, length));

        /// <summary>
        /// Returns a byte span from the memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ReadOnlySpan<byte> AsBytes() => new((byte*)Memory.Pin().Pointer, Length);
    }
}
