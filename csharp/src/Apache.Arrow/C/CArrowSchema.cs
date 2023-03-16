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
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    /// <summary>
    /// An Arrow C Data Interface Schema, which represents a type, field, or schema.
    /// </summary>
    /// <remarks>
    /// This is used to export <see cref="ArrowType"/>, <see cref="Field"/>, or
    /// <see cref="Schema"/> to other languages. It matches the layout of the
    /// ArrowSchema struct described in https://github.com/apache/arrow/blob/main/cpp/src/arrow/c/abi.h.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct CArrowSchema
    {
        public byte* format;
        public byte* name;
        public byte* metadata;
        public long flags;
        public long n_children;
        public CArrowSchema** children;
        public CArrowSchema* dictionary;
        public delegate* unmanaged[Stdcall]<CArrowSchema*, void> release;
        public void* private_data;

        /// <summary>
        /// Allocate and zero-initialize an unmanaged pointer of this type.
        /// </summary>
        /// <remarks>
        /// This pointer must later be freed by <see cref="Free"/>.
        /// </remarks>
        public static CArrowSchema* Create()
        {
            var ptr = (CArrowSchema*)Marshal.AllocHGlobal(sizeof(CArrowSchema));

            ptr->format = null;
            ptr->name = null;
            ptr->metadata = null;
            ptr->flags = 0;
            ptr->n_children = 0;
            ptr->children = null;
            ptr->dictionary = null;
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
        public static void Free(CArrowSchema* schema)
        {
            if (schema->release != null)
            {
                // Call release if not already called.
                schema->release(schema);
            }
            Marshal.FreeHGlobal((IntPtr)schema);
        }


        /// <summary>
        /// For dictionary-encoded types, whether the ordering of dictionary indices is semantically meaningful.
        /// </summary>
        public const long ArrowFlagDictionaryOrdered = 1;
        /// <summary>
        /// Whether this field is semantically nullable (regardless of whether it actually has null values)
        /// </summary>
        public const long ArrowFlagNullable = 2;
        /// <summary>
        /// For map types, whether the keys within each map value are sorted.
        /// </summary>
        public const long ArrowFlagMapKeysSorted = 4;

        /// <summary>
        /// Get the value of a particular flag.
        /// </summary>
        /// <remarks>
        /// Known valid flags are <see cref="ArrowFlagDictionaryOrdered" />,
        /// <see cref="ArrowFlagNullable" />, and <see cref="ArrowFlagMapKeysSorted" />.
        /// </remarks>
        public readonly bool GetFlag(long flag)
        {
            return (flags & flag) == flag;
        }

        internal readonly CArrowSchema* GetChild(long i)
        {
            if ((ulong)i >= (ulong)n_children)
            {
                throw new ArgumentOutOfRangeException("Child index out of bounds.");
            }
            if (children == null)
            {
                throw new ArgumentOutOfRangeException($"Child index '{i}' out of bounds.");
            }

            return children[i];
        }
    }
}
