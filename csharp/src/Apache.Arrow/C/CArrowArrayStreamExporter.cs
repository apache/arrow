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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.C
{
    public static class CArrowArrayStreamExporter
    {
#if NET5_0_OR_GREATER
        private static unsafe delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowSchema*, int> GetSchemaPtr => &GetSchema;
        private static unsafe delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowArray*, int> GetNextPtr => &GetNext;
        private static unsafe delegate* unmanaged[Stdcall]<CArrowArrayStream*, byte*> GetLastErrorPtr => &GetLastError;
        private static unsafe delegate* unmanaged[Stdcall]<CArrowArrayStream*, void> ReleasePtr => &Release;
#else
        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        private unsafe delegate int GetSchemaArrayStream(CArrowArrayStream* cArrayStream, CArrowSchema* cSchema);
        private static unsafe NativeDelegate<GetSchemaArrayStream> s_getSchemaArrayStream = new NativeDelegate<GetSchemaArrayStream>(GetSchema);
        private static unsafe delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowSchema*, int> GetSchemaPtr =>
            (delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowSchema*, int>)s_getSchemaArrayStream.Pointer;
        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        private unsafe delegate int GetNextArrayStream(CArrowArrayStream* cArrayStream, CArrowArray* cArray);
        private static unsafe NativeDelegate<GetNextArrayStream> s_getNextArrayStream = new NativeDelegate<GetNextArrayStream>(GetNext);
        private static unsafe delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowArray*, int> GetNextPtr =>
            (delegate* unmanaged[Stdcall]<CArrowArrayStream*, CArrowArray*, int>)s_getNextArrayStream.Pointer;
        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        private unsafe delegate byte* GetLastErrorArrayStream(CArrowArrayStream* cArrayStream);
        private static unsafe NativeDelegate<GetLastErrorArrayStream> s_getLastErrorArrayStream = new NativeDelegate<GetLastErrorArrayStream>(GetLastError);
        private static unsafe delegate* unmanaged[Stdcall]<CArrowArrayStream*, byte*> GetLastErrorPtr =>
            (delegate* unmanaged[Stdcall]<CArrowArrayStream*, byte*>)s_getLastErrorArrayStream.Pointer;
        [UnmanagedFunctionPointer(CallingConvention.StdCall)]
        private unsafe delegate void ReleaseArrayStream(CArrowArrayStream* cArrayStream);
        private static unsafe NativeDelegate<ReleaseArrayStream> s_releaseArrayStream = new NativeDelegate<ReleaseArrayStream>(Release);
        private static unsafe delegate* unmanaged[Stdcall]<CArrowArrayStream*, void> ReleasePtr =>
            (delegate* unmanaged[Stdcall]<CArrowArrayStream*, void>)s_releaseArrayStream.Pointer;
#endif

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

            cArrayStream->private_data = ExportedArrayStream.Export(arrayStream);
            cArrayStream->get_schema = GetSchemaPtr;
            cArrayStream->get_next = GetNextPtr;
            cArrayStream->get_last_error = GetLastErrorPtr;
            cArrayStream->release = ReleasePtr;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvStdcall) })]
#endif
        private unsafe static int GetSchema(CArrowArrayStream* cArrayStream, CArrowSchema* cSchema)
        {
            ExportedArrayStream arrayStream = null;
            try
            {
                arrayStream = ExportedArrayStream.FromPointer(cArrayStream->private_data);
                CArrowSchemaExporter.ExportSchema(arrayStream.ArrowArrayStream.Schema, cSchema);
                return arrayStream.ClearError();
            }
            catch (Exception ex)
            {
                return arrayStream?.SetError(ex) ?? ExportedArrayStream.EOTHER;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvStdcall) })]
#endif
        private unsafe static int GetNext(CArrowArrayStream* cArrayStream, CArrowArray* cArray)
        {
            ExportedArrayStream arrayStream = null;
            try
            {
                cArray->release = null;
                arrayStream = ExportedArrayStream.FromPointer(cArrayStream->private_data);
                RecordBatch recordBatch = arrayStream.ArrowArrayStream.ReadNextRecordBatchAsync().Result;
                if (recordBatch != null)
                {
                    CArrowArrayExporter.ExportRecordBatch(recordBatch, cArray);
                }
                return arrayStream.ClearError();
            }
            catch (Exception ex)
            {
                return arrayStream?.SetError(ex) ?? ExportedArrayStream.EOTHER;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvStdcall) })]
#endif
        private unsafe static byte* GetLastError(CArrowArrayStream* cArrayStream)
        {
            try
            {
                ExportedArrayStream arrayStream = ExportedArrayStream.FromPointer(cArrayStream->private_data);
                return arrayStream.LastError;
            }
            catch (Exception)
            {
                return null;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvStdcall) })]
#endif
        private unsafe static void Release(CArrowArrayStream* cArrayStream)
        {
            ExportedArrayStream arrayStream = ExportedArrayStream.FromPointer(cArrayStream->private_data);
            arrayStream.Dispose();
            cArrayStream->release = null;
        }

        sealed unsafe class ExportedArrayStream : IDisposable
        {
            public const int EOTHER = 131;

            ExportedArrayStream(IArrowArrayStream arrayStream)
            {
                ArrowArrayStream = arrayStream;
                LastError = null;
            }

            public IArrowArrayStream ArrowArrayStream { get; private set; }
            public byte* LastError { get; private set; }

            public static void* Export(IArrowArrayStream arrayStream)
            {
                ExportedArrayStream result = new ExportedArrayStream(arrayStream);
                GCHandle gch = GCHandle.Alloc(result);
                return (void*)GCHandle.ToIntPtr(gch);
            }

            public static ExportedArrayStream FromPointer(void* ptr)
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)ptr);
                return (ExportedArrayStream)gch.Target;
            }

            public int SetError(Exception ex)
            {
                ReleaseLastError();
                LastError = StringUtil.ToCStringUtf8(ex.Message);
                return EOTHER;
            }

            public int ClearError()
            {
                ReleaseLastError();
                return 0;
            }

            public void Dispose()
            {
                ReleaseLastError();
                ArrowArrayStream?.Dispose();
                ArrowArrayStream = null;
            }

            void ReleaseLastError()
            {
                if (LastError != null)
                {
                    Marshal.FreeCoTaskMem((IntPtr)LastError);
                    LastError = null;
                }
            }
        }
    }
}
