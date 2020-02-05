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
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Apache.Arrow.Memory
{
    public class NativeMemoryManager: MemoryManager<byte>
    {
        private IntPtr _ptr;
        private readonly int _offset;
        private readonly int _length;

        public NativeMemoryManager(IntPtr ptr, int offset, int length)
        {
            _ptr = ptr;
            _offset = offset;
            _length = length;
        }

        ~NativeMemoryManager()
        {
            Dispose(false);
        }

        public override unsafe Span<byte> GetSpan()
        {
            var ptr = CalculatePointer(0);
            return new Span<byte>(ptr, _length);
        }

        public override unsafe MemoryHandle Pin(int elementIndex = 0)
        {
            // NOTE: Unmanaged memory doesn't require GC pinning because by definition it's not
            // managed by the garbage collector.

            var ptr = CalculatePointer(elementIndex);
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

            lock (this)
            {
                if (_ptr != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(_ptr);
                    Interlocked.Exchange(ref _ptr, IntPtr.Zero);
                    GC.RemoveMemoryPressure(_length);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void* CalculatePointer(int index) => 
            (_ptr + _offset + index).ToPointer();
    }
}
