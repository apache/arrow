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
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Apache.Arrow.Memory
{
    internal sealed class ExportedAllocationOwner : INativeAllocationOwner, IDisposable
    {
        private readonly List<IntPtr> _pointers = new List<IntPtr>();
        private int _allocationSize;

        ~ExportedAllocationOwner()
        {
            Dispose();
        }

        public IntPtr Allocate(int size)
        {
            GC.AddMemoryPressure(size);
            return Acquire(Marshal.AllocHGlobal(size), 0, size);
        }

        public IntPtr Acquire(IntPtr ptr, int offset, int length)
        {
            _pointers.Add(ptr);
            _allocationSize += length;
            return ptr;
        }

        public void Release(IntPtr ptr, int offset, int length)
        {
            throw new InvalidOperationException();
        }

        public void Dispose()
        {
            for (int i = 0; i < _pointers.Count; i++)
            {
                if (_pointers[i] != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(_pointers[i]);
                    _pointers[i] = IntPtr.Zero;
                }
            }
            GC.RemoveMemoryPressure(_allocationSize);
            GC.SuppressFinalize(this);
        }
    }
}
