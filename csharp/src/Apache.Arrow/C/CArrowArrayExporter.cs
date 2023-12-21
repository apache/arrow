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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    public static class CArrowArrayExporter
    {
#if NET5_0_OR_GREATER
        private static unsafe delegate* unmanaged<CArrowArray*, void> ReleaseArrayPtr => &ReleaseArray;
#else
        internal unsafe delegate void ReleaseArrowArray(CArrowArray* cArray);
        private static unsafe readonly NativeDelegate<ReleaseArrowArray> s_releaseArray = new NativeDelegate<ReleaseArrowArray>(ReleaseArray);
        private static IntPtr ReleaseArrayPtr => s_releaseArray.Pointer;
#endif
        /// <summary>
        /// Export an <see cref="IArrowArray"/> to a <see cref="CArrowArray"/>. Whether or not the
        /// export succeeds, the original array becomes invalid. Clone an array to continue using it
        /// after a copy has been exported.
        /// </summary>
        /// <param name="array">The array to export</param>
        /// <param name="cArray">An allocated but uninitialized CArrowArray pointer.</param>
        /// <example>
        /// <code>
        /// CArrowArray* exportPtr = CArrowArray.Create();
        /// CArrowArrayExporter.ExportArray(array, exportPtr);
        /// foreign_import_function(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportArray(IArrowArray array, CArrowArray* cArray)
        {
            if (array == null)
            {
                throw new ArgumentNullException(nameof(array));
            }
            if (cArray == null)
            {
                throw new ArgumentNullException(nameof(cArray));
            }

            ExportedAllocationOwner allocationOwner = new ExportedAllocationOwner();
            try
            {
                ConvertArray(allocationOwner, array.Data, cArray);
                allocationOwner = null;
            }
            finally
            {
                allocationOwner?.Dispose();
            }
        }

        /// <summary>
        /// Export a <see cref="RecordBatch"/> to a <see cref="CArrowArray"/>. Whether or not the
        /// export succeeds, the original record batch becomes invalid. Clone the batch to continue using it
        /// after a copy has been exported.
        /// </summary>
        /// <param name="batch">The record batch to export</param>
        /// <param name="cArray">An allocated but uninitialized CArrowArray pointer.</param>
        /// <example>
        /// <code>
        /// CArrowArray* exportPtr = CArrowArray.Create();
        /// CArrowArrayExporter.ExportRecordBatch(batch, exportPtr);
        /// foreign_import_function(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportRecordBatch(RecordBatch batch, CArrowArray* cArray)
        {
            if (batch == null)
            {
                throw new ArgumentNullException(nameof(batch));
            }
            if (cArray == null)
            {
                throw new ArgumentNullException(nameof(cArray));
            }
            if (cArray->release != default)
            {
                throw new ArgumentException("Cannot export array to a struct that is already initialized.", nameof(cArray));
            }

            ExportedAllocationOwner allocationOwner = new ExportedAllocationOwner();
            try
            {
                ConvertRecordBatch(allocationOwner, batch, cArray);
                allocationOwner = null;
            }
            finally
            {
                allocationOwner?.Dispose();
            }
        }

        private unsafe static void ConvertArray(ExportedAllocationOwner sharedOwner, ArrayData array, CArrowArray* cArray)
        {
            cArray->length = array.Length;
            cArray->offset = array.Offset;
            cArray->null_count = array.NullCount;
            cArray->release = ReleaseArrayPtr;
            cArray->private_data = MakePrivateData(sharedOwner);

            cArray->n_buffers = array.Buffers?.Length ?? 0;
            cArray->buffers = null;
            if (cArray->n_buffers > 0)
            {
                long* lengths = null;
                int bufferCount = array.Buffers.Length;
                if (array.DataType.TypeId == ArrowTypeId.BinaryView || array.DataType.TypeId == ArrowTypeId.StringView)
                {
                    lengths = (long*)sharedOwner.Allocate(8 * bufferCount); // overallocation to avoid edge case
                    bufferCount++;
                    cArray->n_buffers++;
                }

                cArray->buffers = (byte**)sharedOwner.Allocate(bufferCount * IntPtr.Size);
                for (int i = 0; i < array.Buffers.Length; i++)
                {
                    ArrowBuffer buffer = array.Buffers[i];
                    IntPtr ptr;
                    if (!buffer.TryExport(sharedOwner, out ptr))
                    {
                        throw new NotSupportedException($"An ArrowArray of type {array.DataType.TypeId} could not be exported: failed on buffer #{i}");
                    }
                    cArray->buffers[i] = (byte*)ptr;
                    if (lengths != null && i >= 2)
                    {
                        lengths[i - 2] = array.Buffers[i].Length;
                    }
                }

                if (lengths != null)
                {
                    cArray->buffers[array.Buffers.Length] = (byte*)lengths;
                }
            }

            cArray->n_children = array.Children?.Length ?? 0;
            cArray->children = null;
            if (cArray->n_children > 0)
            {
                cArray->children = (CArrowArray**)sharedOwner.Allocate(IntPtr.Size * array.Children.Length);
                for (int i = 0; i < array.Children.Length; i++)
                {
                    cArray->children[i] = MakeArray(sharedOwner);
                    ConvertArray(sharedOwner, array.Children[i], cArray->children[i]);
                }
            }

            cArray->dictionary = null;
            if (array.Dictionary != null)
            {
                cArray->dictionary = MakeArray(sharedOwner);
                ConvertArray(sharedOwner, array.Dictionary, cArray->dictionary);
            }
        }

        private unsafe static void ConvertRecordBatch(ExportedAllocationOwner sharedOwner, RecordBatch batch, CArrowArray* cArray)
        {
            cArray->length = batch.Length;
            cArray->offset = 0;
            cArray->null_count = 0;
            cArray->release = ReleaseArrayPtr;
            cArray->private_data = MakePrivateData(sharedOwner);

            cArray->n_buffers = 1;
            cArray->buffers = (byte**)sharedOwner.Allocate(IntPtr.Size);

            cArray->n_children = batch.ColumnCount;
            cArray->children = null;
            // XXX sharing the same ExportedAllocationOwner for all columns
            // and child arrays makes memory tracking inflexible.
            // If the consumer keeps only a single record batch column,
            // the entire record batch memory is nevertheless kept alive.
            if (cArray->n_children > 0)
            {
                cArray->children = (CArrowArray**)sharedOwner.Allocate(IntPtr.Size * batch.ColumnCount);
                int i = 0;
                foreach (IArrowArray child in batch.Arrays)
                {
                    cArray->children[i] = MakeArray(sharedOwner);
                    ConvertArray(sharedOwner, child.Data, cArray->children[i]);
                    i++;
                }
            }

            cArray->dictionary = null;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void ReleaseArray(CArrowArray* cArray)
        {
            for (long i = 0; i < cArray->n_children; i++)
            {
                CArrowArray.CallReleaseFunc(cArray->children[i]);
            }
            if (cArray->dictionary != null)
            {
                CArrowArray.CallReleaseFunc(cArray->dictionary);
            }
            DisposePrivateData(&cArray->private_data);
            cArray->release = default;
        }

        private unsafe static CArrowArray* MakeArray(ExportedAllocationOwner sharedOwner)
        {
            var array = (CArrowArray*)sharedOwner.Allocate(sizeof(CArrowArray));
            *array = default;
            return array;
        }

        private unsafe static void* MakePrivateData(ExportedAllocationOwner sharedOwner)
        {
            GCHandle gch = GCHandle.Alloc(sharedOwner);
            sharedOwner.IncRef();
            return (void*)GCHandle.ToIntPtr(gch);
        }

        private unsafe static void DisposePrivateData(void** ptr)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr) (*ptr));
            if (!gch.IsAllocated)
            {
                return;
            }
            // We can't call IDisposable.Dispose() here as we create multiple
            // GCHandles to the same object. Instead, refcounting ensures
            // timely memory deallocation when all GCHandles are freed.
            ((ExportedAllocationOwner) gch.Target).DecRef();
            gch.Free();
        }
    }
}
