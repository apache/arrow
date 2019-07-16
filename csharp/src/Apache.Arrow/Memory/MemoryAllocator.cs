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
using System.Threading;

namespace Apache.Arrow.Memory
{
    public abstract class MemoryAllocator
    {
        public const int DefaultAlignment = 64;

        public static Lazy<MemoryAllocator> Default { get; } = new Lazy<MemoryAllocator>(BuildDefault, true);

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

        protected int Alignment { get; }

        protected MemoryAllocator(int alignment = DefaultAlignment)
        {
            Statistics = new Stats();
            Alignment = alignment;
        }

        public IMemoryOwner<byte> Allocate(int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            if (length == 0)
            {
                return null;
            }

            var memoryOwner = AllocateInternal(length, out var bytesAllocated);

            Statistics.Allocate(bytesAllocated);

            return memoryOwner;
        }

        private static MemoryAllocator BuildDefault()
        {
            return new NativeMemoryAllocator(DefaultAlignment);
        }

        protected abstract IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated);
    }
}
