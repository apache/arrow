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
using Apache.Arrow.Ipc;

namespace Apache.Arrow.C
{
    public static class CArrowArrayStreamExporter
    {
        private unsafe delegate int GetSchemaArrayStream(CArrowArrayStream* cArrayStream, CArrowSchema* cSchema);
        private unsafe delegate int GetNextArrayStream(CArrowArrayStream* cArrayStream, CArrowArray* cArray);
        private unsafe delegate byte* GetLastErrorArrayStream(CArrowArrayStream* cArrayStream);
        private unsafe delegate void ReleaseArrayStream(CArrowArrayStream* cArrayStream);

        /// <summary>
        /// Export an <see cref="IArrowArrayStream"/> to a <see cref="CArrowArrayStream"/>.
        /// </summary>
        /// <param name="arrayStream">The array stream to export</param>
        /// <param name="cArrayStream">An allocated but uninitialized CArrowArrayStream pointer.</param>
        /// <example>
        /// <code>
        /// CArrowArrayStream* exportPtr = CArrowArrayStream.Create();
        /// CArrowArrayStreamExporter.ExportArray(arrayStream, exportPtr);
        /// foreign_import_function(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportArrayStream(IArrowArrayStream arrayStream, CArrowArrayStream* cArrayStream)
        {
            if (arrayStream == null)
            {
                throw new ArgumentNullException(nameof(arrayStream));
            }
            if (arrayStream == null)
            {
                throw new ArgumentNullException(nameof(arrayStream));
            }
            if (cArrayStream->release != null)
            {
                throw new ArgumentException("Cannot export array to a struct that is already initialized.", nameof(cArrayStream));
            }

            cArrayStream->private_data = FromDisposable(arrayStream);
            cArrayStream->get_schema = (delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowSchema*, int>)Marshal.GetFunctionPointerForDelegate<GetSchemaArrayStream>(GetSchema);
            cArrayStream->get_next = (delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowArray*, int>)Marshal.GetFunctionPointerForDelegate<GetNextArrayStream>(GetNext);
            cArrayStream->get_last_error = (delegate* unmanaged[Stdcall]<CArrowArrayStream*, byte*>)Marshal.GetFunctionPointerForDelegate<GetLastErrorArrayStream>(GetLastError);
            cArrayStream->release = (delegate* unmanaged[Stdcall]<CArrowArrayStream*, void>)Marshal.GetFunctionPointerForDelegate<ReleaseArrayStream>(Release);
        }

        private unsafe static int GetSchema(CArrowArrayStream* cArrayStream, CArrowSchema* cSchema)
        {
            try
            {
                IArrowArrayStream arrayStream = FromPointer(cArrayStream->private_data);
                CArrowSchemaExporter.ExportSchema(arrayStream.Schema, cSchema);
                return 0;
            }
            catch (Exception)
            {
                return 1;
            }
        }

        private unsafe static int GetNext(CArrowArrayStream* cArrayStream, CArrowArray* cArray)
        {
            try
            {
                cArray->release = null;
                IArrowArrayStream arrayStream = FromPointer(cArrayStream->private_data);
                RecordBatch recordBatch = arrayStream.ReadNextRecordBatchAsync().Result;
                if (recordBatch != null)
                {
                    CArrowArrayExporter.ExportRecordBatch(recordBatch, cArray);
                }
                return 0;
            }
            catch (Exception)
            {
                return 1;
            }
        }

        private unsafe static byte* GetLastError(CArrowArrayStream* cArrayStream)
        {
            return null;
        }

        private unsafe static void Release(CArrowArrayStream* cArrayStream)
        {
            FromPointer(cArrayStream->private_data).Dispose();
        }

        private unsafe static void* FromDisposable(IDisposable disposable)
        {
            GCHandle gch = GCHandle.Alloc(disposable);
            return (void*)GCHandle.ToIntPtr(gch);
        }

        private unsafe static IArrowArrayStream FromPointer(void* ptr)
        {
            GCHandle gch = GCHandle.FromIntPtr((IntPtr)ptr);
            return (IArrowArrayStream)gch.Target;
        }
    }
}
