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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.C
{
    public static class CArrowArrayStreamImporter
    {
        /// <summary>
        /// Import C pointer as an <see cref="IArrowArrayStream"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback on the passed struct if the function fails.
        /// Otherwise, the release callback is called when the IArrowArrayStream is disposed.
        /// </remarks>
        /// <examples>
        /// Typically, you will allocate an uninitialized CArrowArrayStream pointer,
        /// pass that to external function, and then use this method to import
        /// the result.
        /// 
        /// <code>
        /// CArrowArrayStream* importedPtr = CArrowArrayStream.Create();
        /// foreign_export_function(importedPtr);
        /// IArrowArrayStream importedStream = CArrowArrayStreamImporter.ImportStream(importedPtr);
        /// </code>
        /// </examples>
        /// <param name="ptr">The pointer to the stream being imported</param>
        /// <returns>The imported C# array stream</returns>
        public static unsafe IArrowArrayStream ImportArrayStream(CArrowArrayStream* ptr)
        {
            return new ImportedArrowArrayStream(ptr);
        }

        private sealed unsafe class ImportedArrowArrayStream : IArrowArrayStream
        {
            private readonly CArrowArrayStream _cArrayStream;
            private readonly Schema _schema;
            private bool _disposed;

            internal static string GetLastError(CArrowArrayStream* arrayStream, int errno)
            {
#if NET5_0_OR_GREATER
                byte* error = arrayStream->get_last_error(arrayStream);
#else
                byte* error = Marshal.GetDelegateForFunctionPointer<CArrowArrayStreamExporter.GetLastErrorArrayStream>(arrayStream->get_last_error)(arrayStream);
#endif
                if (error == null)
                {
                    return $"Array stream operation failed with no message. Error code: {errno}";
                }
                return StringUtil.PtrToStringUtf8(error);
            }

            public ImportedArrowArrayStream(CArrowArrayStream* cArrayStream)
            {
                if (cArrayStream == null)
                {
                    throw new ArgumentNullException(nameof(cArrayStream));
                }
                if (cArrayStream->release == default)
                {
                    throw new ArgumentException("Tried to import an array stream that has already been released.", nameof(cArrayStream));
                }

                CArrowSchema cSchema = new CArrowSchema();
#if NET5_0_OR_GREATER
                int errno = cArrayStream->get_schema(cArrayStream, &cSchema);
#else
                int errno = Marshal.GetDelegateForFunctionPointer<CArrowArrayStreamExporter.GetSchemaArrayStream>(cArrayStream->get_schema)(cArrayStream, &cSchema);
#endif
                if (errno != 0)
                {
                    throw new Exception(GetLastError(cArrayStream, errno));
                }
                _schema = CArrowSchemaImporter.ImportSchema(&cSchema);

                _cArrayStream = *cArrayStream;
                cArrayStream->release = default;
            }

            ~ImportedArrowArrayStream()
            {
                Dispose();
            }

            public Schema Schema => _schema;

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(typeof(ImportedArrowArrayStream).Name);
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return new(Task.FromCanceled<RecordBatch>(cancellationToken));
                }

                RecordBatch result = null;
                CArrowArray cArray = new CArrowArray();
                fixed (CArrowArrayStream* cArrayStream = &_cArrayStream)
                {
#if NET5_0_OR_GREATER
                    int errno = cArrayStream->get_next(cArrayStream, &cArray);
#else
                    int errno = Marshal.GetDelegateForFunctionPointer<CArrowArrayStreamExporter.GetNextArrayStream>(cArrayStream->get_next)(cArrayStream, &cArray);
#endif
                    if (errno != 0)
                    {
                        return new(Task.FromException<RecordBatch>(new Exception(GetLastError(cArrayStream, errno))));
                    }
                    if (cArray.release != default)
                    {
                        result = CArrowArrayImporter.ImportRecordBatch(&cArray, _schema);
                    }
                }

                return new ValueTask<RecordBatch>(result);
            }

            public void Dispose()
            {
                if (!_disposed && _cArrayStream.release != default)
                {
                    _disposed = true;
                    fixed (CArrowArrayStream* cArrayStream = &_cArrayStream)
                    {
#if NET5_0_OR_GREATER
                        cArrayStream->release(cArrayStream);
#else
                        Marshal.GetDelegateForFunctionPointer<CArrowArrayStreamExporter.ReleaseArrayStream>(cArrayStream->release)(cArrayStream);
#endif
                    }
                }
                GC.SuppressFinalize(this);
            }
        }
    }
}
