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

using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Acero
{
    internal static class ExportUtil
    {
        public static unsafe CLib.GArrowSchema* ExportAndGetSchemaPtr(Schema schema)
        {
            // allocate a C ABI ArrowSchema
            CArrowSchema* cSchema = CArrowSchema.Create();

            // export the c# schema to the newly allocated CArrowSchema
            CArrowSchemaExporter.ExportSchema(schema, cSchema);

            // Import CArrowSchema into a GArrowSchema
            var gSchema = CLib.garrow_schema_import(cSchema, null);

            return gSchema;
        }

        public static unsafe CLib.GArrowRecordBatch* ExportAndGetRecordBatchPtr(RecordBatch recordBatch, Schema schema)
        {
            var schemaPtr = ExportAndGetSchemaPtr(schema);

            // allocate  a C ABI CArrowArray
            CArrowArray* cArray = CArrowArray.Create();

            // export the c# record batch to the CArrowArray
            CArrowArrayExporter.ExportRecordBatch(recordBatch, cArray);

            // import the CArrowArray into a gArrowRecordBatch
            var gRecordBatch = CLib.garrow_record_batch_import(cArray, schemaPtr, null);

            return gRecordBatch;
        }

        public static unsafe CLib.GArrowRecordBatchReader* ExportAndGetRecordBatchReaderPtr(IArrowArrayStream recordBatchReader)
        {
            // Allocate a CArrowArray
            CArrowArrayStream* cArrayStream = CArrowArrayStream.Create();

            // export the c# record batch to the CArrowArray
            CArrowArrayStreamExporter.ExportArrayStream(recordBatchReader, cArrayStream);

            // next import the c arrow as a gArrowRecordBatch
            var gRecordBatchReader = CLib.garrow_record_batch_reader_import(cArrayStream, null);

            return gRecordBatchReader;
        }
    }
}
