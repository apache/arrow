// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Runtime.InteropServices;

namespace Apache.Arrow.C
{
    /// <summary>
    /// An Arrow C Data Interface Schema, which represents the data in an exported array or record batch.
    /// </summary>
    /// <remarks>
    /// This is used to export <see cref="RecordBatch"/> or <see cref="IArrowArray"/> to other languages. It matches
    /// the layout of the ArrowArray struct described in https://github.com/apache/arrow/blob/main/cpp/src/arrow/c/abi.h.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CArrowArray
    {
        public long length;
        public long null_count;
        public long offset;
        public long n_buffers;
        public long n_children;
        public byte** buffers;
        public CArrowArray** children;
        public CArrowArray* dictionary;
        public delegate* unmanaged[Stdcall]<CArrowArray*, void> release;
        public void* private_data;

        /// <summary>
        /// Allocate and zero-initialize an unmanaged pointer of this type.
        /// </summary>
        /// <remarks>
        /// This pointer must later be freed by <see cref="Free"/>.
        /// </remarks>
        public static CArrowArray* Create()
        {
            var ptr = (CArrowArray*)Marshal.AllocHGlobal(sizeof(CArrowArray));

            ptr->length = 0;
            ptr->n_buffers = 0;
            ptr->offset = 0;
            ptr->buffers = null;
            ptr->n_children = 0;
            ptr->children = null;
            ptr->dictionary = null;
            ptr->null_count = 0;
            ptr->release = null;
            ptr->private_data = null;

            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="Create"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void Free(CArrowArray* array)
        {
            if (array->release != null)
            {
                // Call release if not already called.
                array->release(array);
            }
            Marshal.FreeHGlobal((IntPtr)array);
        }
    }
}
