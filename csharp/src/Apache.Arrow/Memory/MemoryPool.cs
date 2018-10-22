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
using System.Threading;

namespace Apache.Arrow.Memory
{

    public abstract class MemoryPool
    {
        public class Stats
        {
            private long _bytesAllocated;
            private long _allocations;

            public long Allocations => Interlocked.Read(ref _allocations);
            public long BytesAllocated => Interlocked.Read(ref _bytesAllocated);

            internal void Allocate(int n)
            {
                Interlocked.Increment(ref _allocations);
                Interlocked.Add(ref _bytesAllocated, n);
            }
        }

        public Stats Statistics { get; }

        protected MemoryPool()
        {
            Statistics = new Stats();
        }

        public Memory<byte> Allocate(int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            var bytesAllocated = 0;
            var memory = AllocateInternal(length, out bytesAllocated);

            Statistics.Allocate(length);

            // Ensure all allocated memory is zeroed.

            ZeroMemory(memory);
            
            return memory;
        }

        public Memory<byte> Reallocate(Memory<byte> memory, int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            var bytesAllocated = 0;
            var buffer = ReallocateInternal(memory, length, out bytesAllocated);

            Statistics.Allocate(bytesAllocated);

            return buffer;

        }

        private static void ZeroMemory(Memory<byte> memory)
        {
            memory.Span.Fill(0);
        }

        protected abstract Memory<byte> AllocateInternal(int length, out int bytesAllocated);
        protected abstract Memory<byte> ReallocateInternal(Memory<byte> memory, int length, out int bytesAllocated);
    }
}
