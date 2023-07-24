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
            get => Memory.Span;
        }

        public ArrowBuffer Clone(MemoryAllocator allocator = default)
        {
            return Span.Length == 0 ? Empty : new Builder<byte>(Span.Length)
                .Append(Span)
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

        internal bool TryExport(ExportedAllocationOwner newOwner, out IntPtr ptr)
        {
            if (_memoryOwner == null && IsEmpty)
            {
                ptr = IntPtr.Zero;
                return true;
            }

            if (_memoryOwner is IOwnableAllocation ownable && ownable.TryAcquire(out ptr, out int offset, out int length))
            {
                newOwner.Acquire(ptr, offset, length);
                ptr += offset;
                return true;
            }

            ptr = IntPtr.Zero;
            return false;
        }
    }
}
