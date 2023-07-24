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
    internal abstract class ImportedAllocationOwner : INativeAllocationOwner
    {
        long _referenceCount;
        long _managedMemory;

        protected ImportedAllocationOwner()
        {
            _referenceCount = 1;
        }

        public IMemoryOwner<byte> AddMemory(IntPtr ptr, int offset, int length)
        {
            if (_referenceCount <= 0)
            {
                throw new ObjectDisposedException(typeof(ImportedAllocationOwner).Name);
            }

            NativeMemoryManager memory = new NativeMemoryManager(this, ptr, offset, length);
            Interlocked.Increment(ref _referenceCount);
            if (length > 0)
            {
                Interlocked.Add(ref _managedMemory, length);
                GC.AddMemoryPressure(length);
            }
            return memory;
        }

        public void Release(IntPtr ptr, int offset, int length)
        {
            Release();
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _referenceCount) == 0)
            {
                if (_managedMemory > 0)
                {
                    GC.RemoveMemoryPressure(_managedMemory);
                }
                FinalRelease();
            }
        }

        protected abstract void FinalRelease();
    }
}
