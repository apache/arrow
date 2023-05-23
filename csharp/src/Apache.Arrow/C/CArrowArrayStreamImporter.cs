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
        public static unsafe IArrowArrayStream ImportArrayStream(CArrowArrayStream* ptr)
        {
            return new ImportedArrowArrayStream(ptr);
        }

        private sealed unsafe class ImportedArrowArrayStream : IArrowArrayStream
        {
            private readonly CArrowArrayStream* _cArrayStream;
            private readonly Schema _schema;
            private bool _disposed;

            public ImportedArrowArrayStream(CArrowArrayStream* cArrayStream)
            {
                if (cArrayStream == null)
                {
                    throw new ArgumentNullException(nameof(cArrayStream));
                }
                _cArrayStream = cArrayStream;
                if (_cArrayStream->release == null)
                {
                    throw new ArgumentException("Tried to import an array stream that has already been released.", nameof(cArrayStream));
                }

                CArrowSchema* cSchema = CArrowSchema.Create();
                try
                {
                    int errno = _cArrayStream->get_schema(_cArrayStream, cSchema);
                    if (errno != 0)
                    {
                        throw new Exception($"Unexpected error recieved from external stream. Errno: {errno}");
                    }
                    _schema = CArrowSchemaImporter.ImportSchema(cSchema);
                }
                finally
                {
                    if (_schema == null)
                    {
                        CArrowSchema.Free(cSchema);
                    }
                }
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

                RecordBatch result = null;
                CArrowArray* cArray = CArrowArray.Create();
                try
                {
                    int errno = _cArrayStream->get_next(_cArrayStream, cArray);
                    if (errno != 0)
                    {
                        throw new Exception($"Unexpected error recieved from external stream. Errno: {errno}");
                    }
                    if (cArray->release != null)
                    {
                        result = CArrowArrayImporter.ImportRecordBatch(cArray, _schema);
                    }
                }
                finally
                {
                    if (result == null)
                    {
                        CArrowArray.Free(cArray);
                    }
                }

                return new ValueTask<RecordBatch>(result);
            }

            public void Dispose()
            {
                if (!_disposed && _cArrayStream->release != null)
                {
                    _disposed = true;
                    _cArrayStream->release(_cArrayStream);
                }
                GC.SuppressFinalize(this);
            }
        }
    }
}
