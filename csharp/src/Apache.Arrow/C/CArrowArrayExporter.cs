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
using Apache.Arrow.Memory;

namespace Apache.Arrow.C
{
    public static class CArrowArrayExporter
    {
        private unsafe delegate void ReleaseArrowArray(CArrowArray* cArray);
        private static unsafe readonly NativeDelegate<ReleaseArrowArray> s_releaseArray = new NativeDelegate<ReleaseArrowArray>(ReleaseArray);

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
            if (cArray->release != null)
            {
                throw new ArgumentException("Cannot export array to a struct that is already initialized.", nameof(cArray));
            }

            ExportedAllocationOwner allocationOwner = new ExportedAllocationOwner();
            try
            {
                ConvertArray(allocationOwner, array.Data, cArray);
                cArray->release = (delegate* unmanaged[Stdcall]<CArrowArray*, void>)(IntPtr)s_releaseArray.Pointer;
                cArray->private_data = FromDisposable(allocationOwner);
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
            if (cArray->release != null)
            {
                throw new ArgumentException("Cannot export array to a struct that is already initialized.", nameof(cArray));
            }

            ExportedAllocationOwner allocationOwner = new ExportedAllocationOwner();
            try
            {
                ConvertRecordBatch(allocationOwner, batch, cArray);
                cArray->release = (delegate* unmanaged[Stdcall]<CArrowArray*, void>)s_releaseArray.Pointer;
                cArray->private_data = FromDisposable(allocationOwner);
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
            cArray->release = (delegate* unmanaged[Stdcall]<CArrowArray*, void>)s_releaseArray.Pointer;
            cArray->private_data = null;

            cArray->n_buffers = array.Buffers?.Length ?? 0;
            cArray->buffers = null;
            if (cArray->n_buffers > 0)
            {
                cArray->buffers = (byte**)sharedOwner.Allocate(array.Buffers.Length * IntPtr.Size);
                for (int i = 0; i < array.Buffers.Length; i++)
                {
                    ArrowBuffer buffer = array.Buffers[i];
                    IntPtr ptr;
                    if (!buffer.TryExport(sharedOwner, out ptr))
                    {
                        throw new NotSupportedException($"An ArrowArray of type {array.DataType.TypeId} could not be exported");
                    }
                    cArray->buffers[i] = (byte*)ptr;
                }
            }

            cArray->n_children = array.Children?.Length ?? 0;
            cArray->children = null;
            if (cArray->n_children > 0)
            {
                cArray->children = (CArrowArray**)sharedOwner.Allocate(IntPtr.Size * array.Children.Length);
                for (int i = 0; i < array.Children.Length; i++)
                {
                    cArray->children[i] = CArrowArray.Create();
                    ConvertArray(sharedOwner, array.Children[i], cArray->children[i]);
                }
            }

            cArray->dictionary = null;
            if (array.Dictionary != null)
            {
                cArray->dictionary = CArrowArray.Create();
                ConvertArray(sharedOwner, array.Dictionary, cArray->dictionary);
            }
        }

        private unsafe static void ConvertRecordBatch(ExportedAllocationOwner sharedOwner, RecordBatch batch, CArrowArray* cArray)
        {
            cArray->length = batch.Length;
            cArray->offset = 0;
            cArray->null_count = 0;
            cArray->release = (delegate* unmanaged[Stdcall]<CArrowArray*, void>)s_releaseArray.Pointer;
            cArray->private_data = null;

            cArray->n_buffers = 1;
            cArray->buffers = (byte**)sharedOwner.Allocate(IntPtr.Size);

            cArray->n_children = batch.ColumnCount;
            cArray->children = null;
            if (cArray->n_children > 0)
            {
                cArray->children = (CArrowArray**)sharedOwner.Allocate(IntPtr.Size * batch.ColumnCount);
                int i = 0;
                foreach (IArrowArray child in batch.Arrays)
                {
                    cArray->children[i] = CArrowArray.Create();
                    ConvertArray(sharedOwner, child.Data, cArray->children[i]);
                    i++;
                }
            }

            cArray->dictionary = null;
        }

        private unsafe static void ReleaseArray(CArrowArray* cArray)
        {
            if (cArray->private_data != null)
            {
                Dispose(&cArray->private_data);
            }
            cArray->private_data = null;
            cArray->release = null;
        }

        private unsafe static void* FromDisposable(IDisposable disposable)
        {
            GCHandle gch = GCHandle.Alloc(disposable);
            return (void*)GCHandle.ToIntPtr(gch);
        }

        private unsafe static void Dispose(void** ptr)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)(*ptr));
            ((IDisposable)gch.Target).Dispose();
            gch.Free();
            *ptr = null;
        }
    }
}
