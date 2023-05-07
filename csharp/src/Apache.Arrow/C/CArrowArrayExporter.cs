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

        /// <summary>
        /// Export an <see cref="IArrowArray"/> to a <see cref="CArrowArray"/>.
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

            ConvertArray(new SharedNativeAllocationOwner(), array.Data, cArray);
        }

        /// <summary>
        /// Export a <see cref="RecordBatch"/> to a <see cref="CArrowArray"/>.
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

            ConvertRecordBatch(new SharedNativeAllocationOwner(), batch, cArray);
        }

        private unsafe static void ConvertArray(SharedNativeAllocationOwner sharedOwner, ArrayData array, CArrowArray* cArray)
        {
            cArray->length = array.Length;
            cArray->offset = array.Offset;
            cArray->null_count = array.NullCount;
            cArray->release = (delegate* unmanaged[Stdcall]<CArrowArray*, void>)Marshal.GetFunctionPointerForDelegate<ReleaseArrowArray>(ReleaseArray);
            cArray->private_data = FromDisposable(array);

            cArray->n_buffers = array.Buffers?.Length ?? 0;
            cArray->buffers = null;
            if (cArray->n_buffers > 0)
            {
                cArray->buffers = (byte**)Marshal.AllocCoTaskMem(array.Buffers.Length * IntPtr.Size);
                for (int i = 0; i < array.Buffers.Length; i++)
                {
                    ArrowBuffer buffer = array.Buffers[i];
                    if (!buffer.TryShare(sharedOwner))
                    {
                        throw new NotSupportedException(); // TODO
                    }
                    cArray->buffers[i] = (byte*)buffer.Memory.Pin().Pointer;
                }
            }

            cArray->n_children = array.Children?.Length ?? 0;
            cArray->children = null;
            if (cArray->n_children > 0)
            {
                cArray->children = (CArrowArray**)Marshal.AllocCoTaskMem(IntPtr.Size * array.Children.Length);
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

        private unsafe static void ConvertRecordBatch(SharedNativeAllocationOwner sharedOwner, RecordBatch batch, CArrowArray* cArray)
        {
            cArray->length = batch.Length;
            cArray->offset = 0;
            cArray->null_count = 0;
            cArray->release = (delegate* unmanaged[Stdcall]<CArrowArray*, void>)Marshal.GetFunctionPointerForDelegate<ReleaseArrowArray>(ReleaseArray);
            cArray->private_data = FromDisposable(batch);

            cArray->n_buffers = 1;
            cArray->buffers = (byte**)Marshal.AllocCoTaskMem(IntPtr.Size);

            cArray->n_children = batch.ColumnCount;
            cArray->children = null;
            if (cArray->n_children > 0)
            {
                cArray->children = (CArrowArray**)Marshal.AllocCoTaskMem(IntPtr.Size * batch.ColumnCount);
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
            if (cArray->n_children != 0)
            {
                for (int i = 0; i < cArray->n_children; i++)
                {
                    ReleaseArray(cArray->children[i]);
                    Marshal.FreeCoTaskMem((IntPtr)cArray->children[i]);
                }
                Marshal.FreeCoTaskMem((IntPtr)cArray->children);
                cArray->children = null;
                cArray->n_children = 0;
            }

            if (cArray->buffers != null)
            {
                Marshal.FreeCoTaskMem((IntPtr)cArray->buffers);
                cArray->buffers = null;
            }

            Dispose(&cArray->private_data);
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
