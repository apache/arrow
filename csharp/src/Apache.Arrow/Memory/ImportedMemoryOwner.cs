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
    internal abstract class ImportedMemoryOwner
    {
        long _referenceCount;
        long _managedMemory;

        protected ImportedMemoryOwner()
        {
            _referenceCount = 1;
        }

        public IMemoryOwner<byte> AddMemory(IntPtr ptr, int offset, int length)
        {
            if (_referenceCount <= 0)
            {
                throw new ObjectDisposedException(typeof(ImportedMemoryManager).Name);
            }

            IMemoryOwner<byte> memory = new ImportedMemoryManager(this, ptr, offset, length);
            Interlocked.Increment(ref _referenceCount);
            Interlocked.Add(ref _managedMemory, length);
            if (length > 0)
            {
                GC.AddMemoryPressure(length);
            }
            return memory;
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

        sealed private class ImportedMemoryManager : MemoryManager<byte>
        {
            private ImportedMemoryOwner _owner;
            private IntPtr _ptr;
            private readonly int _offset;
            private readonly int _length;

            public ImportedMemoryManager(ImportedMemoryOwner owner, IntPtr ptr, int offset, int length)
            {
                _owner = owner;
                _ptr = ptr;
                _offset = offset;
                _length = length;
            }

            ~ImportedMemoryManager()
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
                // NOTE: Imported memory doesn't require GC pinning because by definition it's not
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

                ImportedMemoryOwner owner = Interlocked.Exchange(ref _owner, null);
                if (owner != null)
                {
                    _ptr = IntPtr.Zero;
                    owner.Release();
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private unsafe void* CalculatePointer(int index) =>
                (_ptr + _offset + index).ToPointer();
        }
    }
}
