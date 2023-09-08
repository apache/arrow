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
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class SinkNodeOptions : ExecNodeOptions
    {
        private readonly unsafe GArrowSinkNodeOptions* _optionsPtr;
        private readonly Schema _schema;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        protected unsafe delegate GArrowSinkNodeOptions* d_garrow_sink_node_options_new();
        protected static d_garrow_sink_node_options_new garrow_sink_node_options_new = FuncLoader.LoadFunction<d_garrow_sink_node_options_new>("garrow_sink_node_options_new");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        protected unsafe delegate GArrowRecordBatchReader* d_garrow_sink_node_options_get_reader(GArrowSinkNodeOptions* options, GArrowSchema* schema);
        protected static d_garrow_sink_node_options_get_reader garrow_sink_node_options_get_reader = FuncLoader.LoadFunction<d_garrow_sink_node_options_get_reader>("garrow_sink_node_options_get_reader");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate CArrowArrayStream* d_garrow_record_batch_reader_export(GArrowRecordBatchReader* reader, out GError** error);
        private static d_garrow_record_batch_reader_export garrow_record_batch_reader_export = FuncLoader.LoadFunction<d_garrow_record_batch_reader_export>("garrow_record_batch_reader_export");

        internal unsafe GArrowSinkNodeOptions* Handle => _optionsPtr;

        public unsafe SinkNodeOptions(Schema schema)
        {
            _optionsPtr = garrow_sink_node_options_new();
            _schema = schema;
        }

        public unsafe IArrowArrayStream GetRecordBatchReader()
        {
            GArrowSchema* schemaPtr = ExportUtil.ExportAndGetSchemaPtr(_schema);
            GArrowRecordBatchReader* recordBatchReaderPtr = garrow_sink_node_options_get_reader(_optionsPtr, schemaPtr);
            CArrowArrayStream* arrayStreamPtr = garrow_record_batch_reader_export(recordBatchReaderPtr, out GError** error);

            ExceptionUtil.ThrowOnError(error);

            IArrowArrayStream arrayStream = CArrowArrayStreamImporter.ImportArrayStream(arrayStreamPtr);

            return arrayStream;
        }
    }
}
