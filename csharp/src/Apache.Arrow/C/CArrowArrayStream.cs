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
using Apache.Arrow.Ipc;

namespace Apache.Arrow.C
{
    /// <summary>
    /// An Arrow C Data Interface ArrowArrayStream, which represents a stream of record batches.
    /// </summary>
    /// <remarks>
    /// This is used to export <see cref="IArrowArrayStream"/> to other languages. It matches the layout of the
    /// ArrowArrayStream struct described in https://github.com/apache/arrow/blob/main/cpp/src/arrow/c/abi.h.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CArrowArrayStream
    {
        /// <summary>
        /// Callback to get the stream type. Will be the same for all arrays in the stream.
        /// If successful, the ArrowSchema must be released independently from the stream.
        ///
        /// Return value: 0 if successful, an `errno`-compatible error code otherwise.
        ///</summary>
        public delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowSchema*, int> get_schema;

        /// <summary>
        /// Callback to get the next array. If no error and the array is released, the stream has ended.
        /// If successful, the ArrowArray must be released independently from the stream.
        /// 
        /// Return value: 0 if successful, an `errno`-compatible error code otherwise.
        /// </summary>
        public delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowArray*, int> get_next;

        /// <summary>
        /// Callback to get optional detailed error information. This must only
        /// be called if the last stream operation failed with a non-0 return code.
        /// The returned pointer is only valid until the next operation on this stream
        /// (including release).
        ///
        /// Return value: pointer to a null-terminated character array describing the last
        /// error, or NULL if no description is available.
        ///</summary>
        public delegate* unmanaged[Stdcall]<CArrowArrayStream*, byte*> get_last_error;

        /// <summary>
        /// Release callback: release the stream's own resources. Note that arrays returned by
        /// get_next must be individually released.
        /// </summary>
        public delegate* unmanaged[Stdcall]<CArrowArrayStream*, void> release;

        public void* private_data;

        /// <summary>
        /// Allocate and zero-initialize an unmanaged pointer of this type.
        /// </summary>
        /// <remarks>
        /// This pointer must later be freed by <see cref="Free"/>.
        /// </remarks>
        public static CArrowArrayStream* Create()
        {
            var ptr = (CArrowArrayStream*)Marshal.AllocHGlobal(sizeof(CArrowArrayStream));

            ptr->get_schema = null;
            ptr->get_next = null;
            ptr->get_last_error = null;
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
        public static void Free(CArrowArrayStream* arrayStream)
        {
            if (arrayStream->release != null)
            {
                // Call release if not already called.
                arrayStream->release(arrayStream);
            }
            Marshal.FreeHGlobal((IntPtr)arrayStream);
        }
    }
}
