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

using System.Runtime.InteropServices;
using Apache.Arrow.Acero.CLib;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Acero
{
    internal static class ExportUtil
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowRecordBatch* d_garrow_record_batch_import(CArrowArray* c_abi_array, GArrowSchema* schema, out GError** error);
        private static d_garrow_record_batch_import garrow_record_batch_import = FuncLoader.LoadFunction<d_garrow_record_batch_import>("garrow_record_batch_import");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowSchema* d_garrow_schema_import(CArrowSchema* c_abi_schema, out GError** error);
        private static d_garrow_schema_import garrow_schema_import = FuncLoader.LoadFunction<d_garrow_schema_import>("garrow_schema_import");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowRecordBatchReader* d_garrow_record_batch_reader_import(CArrowArrayStream* c_abi_array_stream, out GError** error);
        private static d_garrow_record_batch_reader_import garrow_record_batch_reader_import = FuncLoader.LoadFunction<d_garrow_record_batch_reader_import>("garrow_record_batch_reader_import");

        public static unsafe GArrowSchema* ExportAndGetSchemaPtr(Schema schema)
        {
            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(schema, cSchema);
            GArrowSchema* gSchema = garrow_schema_import(cSchema, out GError** error);

            ExceptionUtil.ThrowOnError(error);

            return gSchema;
        }

        public static unsafe GArrowRecordBatch* ExportAndGetRecordBatchPtr(RecordBatch recordBatch)
        {
            GArrowSchema* schemaPtr = ExportAndGetSchemaPtr(recordBatch.Schema);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(recordBatch, cArray);
            GArrowRecordBatch* gRecordBatch = garrow_record_batch_import(cArray, schemaPtr, out GError** error);

            ExceptionUtil.ThrowOnError(error);

            return gRecordBatch;
        }

        public static unsafe GArrowRecordBatchReader* ExportAndGetRecordBatchReaderPtr(IArrowArrayStream recordBatchReader)
        {
            CArrowArrayStream* cArrayStream = CArrowArrayStream.Create();
            CArrowArrayStreamExporter.ExportArrayStream(recordBatchReader, cArrayStream);
            GArrowRecordBatchReader* gRecordBatchReader = garrow_record_batch_reader_import(cArrayStream, out GError** error);

            ExceptionUtil.ThrowOnError(error);

            return gRecordBatchReader;
        }
    }
}
