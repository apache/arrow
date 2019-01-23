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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Memory
{
    public class NativeMemoryPool : MemoryPool
    {
        public NativeMemoryPool(int alignment = DefaultAlignment) 
            : base(alignment) { }

        protected override Memory<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            // TODO: Ensure memory is released if exception occurs.

            // TODO: Optimize storage overhead; native memory manager stores a pointer
            // to allocated memory, offset, and the allocation size. 

            // TODO: Should the allocation be moved to NativeMemory?

            var size = length + Alignment;
            var ptr =  Marshal.AllocHGlobal(size);
            var offset = (int)(Alignment - (ptr.ToInt64() & (Alignment - 1)));
            var manager = new NativeMemoryManager(ptr, offset, length);

            bytesAllocated = (length + Alignment);

            GC.AddMemoryPressure(bytesAllocated);

            return manager.Memory;
        }

        protected override Memory<byte> ReallocateInternal(Memory<byte> memory, int length, out int bytesAllocated)
        {
            var buffer = AllocateInternal(length, out bytesAllocated);
            memory.CopyTo(buffer);
            return buffer;
        }
    }
}
