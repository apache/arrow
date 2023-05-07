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

namespace Apache.Arrow.C
{
    public static class CArrowArrayExporter
    {
        private unsafe delegate void ReleaseArrowArray(CArrowArray* nativeArray);

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

            ConvertArray(array.Data, cArray);
        }

        private unsafe static void ConvertArray(ArrayData array, CArrowArray* cArray)
        {
            // TODO: Figure out a sane memory management strategy

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
                    cArray->buffers[i] = (byte*)array.Buffers[i].Memory.Pin().Pointer;
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
                    ConvertArray(array.Children[i], cArray->children[i]);
                }
            }

            cArray->dictionary = null;
            if (array.Dictionary != null)
            {
                cArray->dictionary = CArrowArray.Create();
                ConvertArray(array.Dictionary, cArray->dictionary);
            }
        }

        private unsafe static void ReleaseArray(CArrowArray* nativeArray)
        {
            if (nativeArray->n_children != 0)
            {
                for (int i = 0; i < nativeArray->n_children; i++)
                {
                    ReleaseArray(nativeArray->children[i]);
                    Marshal.FreeCoTaskMem((IntPtr)nativeArray->children[i]);
                }
                Marshal.FreeCoTaskMem((IntPtr)nativeArray->children);
                nativeArray->children = null;
                nativeArray->n_children = 0;
            }

            if (nativeArray->buffers != null)
            {
                Marshal.FreeCoTaskMem((IntPtr)nativeArray->buffers);
                nativeArray->buffers = null;
            }

            Dispose(&nativeArray->private_data);
            nativeArray->release = null;
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
