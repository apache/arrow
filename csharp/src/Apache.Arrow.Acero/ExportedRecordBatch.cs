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

namespace Apache.Arrow.Acero
{
    public class ExportedRecordBatch
    {
        private readonly ExportedSchema _exportedSchema;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowRecordBatch* d_garrow_record_batch_import(CArrowArray* c_abi_array, GArrowSchema* schema, out GError** error);
        private static d_garrow_record_batch_import garrow_record_batch_import = FuncLoader.LoadFunction<d_garrow_record_batch_import>("garrow_record_batch_import");

        public unsafe GArrowRecordBatch* Handle { get; }

        public unsafe ExportedRecordBatch(RecordBatch recordBatch)
        {
            _exportedSchema = new ExportedSchema(recordBatch.Schema);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowArrayExporter.ExportRecordBatch(recordBatch, cArray);
            Handle = garrow_record_batch_import(cArray, _exportedSchema.Handle, out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }
    }
}
