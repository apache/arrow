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
using static Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class SinkNodeOptions : ExecNodeOptions
    {
        private readonly unsafe CLib.GArrowSinkNodeOptions* _optionsPtr;
        private readonly Schema _schema;

        public unsafe SinkNodeOptions(Schema schema)
        {
            _optionsPtr = CLib.garrow_sink_node_options_new();
            _schema = schema;
        }

        public unsafe IArrowArrayStream GetRecordBatchReader()
        {
            var schemaPtr = ExportUtil.ExportAndGetSchemaPtr(_schema);
            var recordBatchReaderPtr = CLib.garrow_sink_node_options_get_reader(_optionsPtr, schemaPtr);

            GError** error;

            var arrayStreamPtr = CLib.garrow_record_batch_reader_export(recordBatchReaderPtr, out error);

            ExceptionUtil.ThrowOnError(error);

            var arrayStream = CArrowArrayStreamImporter.ImportArrayStream(arrayStreamPtr);

            return arrayStream;
        }

        internal unsafe CLib.GArrowSinkNodeOptions* GetPtr()
        {
            return _optionsPtr;
        }
    }
}
