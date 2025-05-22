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
using System.Buffers;
using System.Threading;

namespace Apache.Arrow.Tests
{
    public class TestMemoryAllocator : MemoryAllocator
    {
        private int _rented = 0;
        public int Rented => _rented;

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            var mem = MemoryPool<byte>.Shared.Rent(length);
            bytesAllocated = mem.Memory.Length;
            Interlocked.Increment(ref _rented);
            return new TestMemoryOwner(mem, this);
        }

        private class TestMemoryOwner : IMemoryOwner<byte>
        {
            private readonly IMemoryOwner<byte> _inner;
            private readonly TestMemoryAllocator _allocator;
            private bool _disposed;

            public TestMemoryOwner(IMemoryOwner<byte> inner, TestMemoryAllocator allocator)
            {
                _inner = inner;
                _allocator = allocator;
            }

            public Memory<byte> Memory
            {
                get
                {
                    if (_disposed)
                        throw new ObjectDisposedException(nameof(TestMemoryOwner));
                    return _inner.Memory;
                }
            }

            public void Dispose()
            {
                if (_disposed)
                    return;
                _disposed = true;
                Interlocked.Decrement(ref _allocator._rented);
                _inner?.Dispose();
            }
        }
    }
}
