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
using System.Threading;

namespace Apache.Arrow.Memory
{
    public class NativeMemoryManager : MemoryManager<byte>, IOwnableAllocation
    {
        private IntPtr _ptr;
        private readonly int _offset;
        private readonly int _length;
        private readonly INativeAllocationOwner _owner;

        public NativeMemoryManager(IntPtr ptr, int offset, int length)
            : this(NativeMemoryAllocator.ExclusiveOwner, ptr, offset, length)
        {
        }

        internal NativeMemoryManager(INativeAllocationOwner owner, IntPtr ptr, int offset, int length)
        {
            _ptr = ptr;
            _offset = offset;
            _length = length;
            _owner = owner;
        }

        ~NativeMemoryManager()
        {
            Dispose(false);
        }

        public override unsafe Span<byte> GetSpan()
        {
            void* ptr = CalculatePointer(0);
            return new Span<byte>(ptr, _length);
        }

        public override unsafe MemoryHandle Pin(int elementIndex = 0)
        {
            // NOTE: Unmanaged memory doesn't require GC pinning because by definition it's not
            // managed by the garbage collector.

            void* ptr = CalculatePointer(elementIndex);
            return new MemoryHandle(ptr, default, this);
        }

        public override void Unpin()
        {
            // SEE: Pin implementation
            return;
        }

        protected override void Dispose(bool disposing)
        {
            // Only free once.
            IntPtr ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
            if (ptr != IntPtr.Zero)
            {
                _owner.Release(ptr, _offset, _length);
            }
        }

        bool IOwnableAllocation.TryAcquire(out IntPtr ptr, out int offset, out int length)
        {
            // TODO: implement refcounted buffers?

            if (object.ReferenceEquals(_owner, NativeMemoryAllocator.ExclusiveOwner))
            {
                ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
                if (ptr != IntPtr.Zero)
                {
                    offset = _offset;
                    length = _length;
                    return true;
                }
            }

            ptr = IntPtr.Zero;
            offset = 0;
            length = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void* CalculatePointer(int index) =>
            (_ptr + _offset + index).ToPointer();
    }
}
