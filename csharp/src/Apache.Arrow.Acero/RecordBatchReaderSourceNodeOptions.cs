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
    public class RecordBatchReaderSourceNodeOptions : ExecNodeOptions
    {
        private unsafe CLib.GArrowSourceNodeOptions* _optionsPtr;

        public unsafe RecordBatchReaderSourceNodeOptions(IArrowArrayStream recordBatchReader)
        {
            var recordBatchReaderPtr = ExportUtil.ExportAndGetRecordBatchReaderPtr(recordBatchReader);

            _optionsPtr = CLib.garrow_source_node_options_new_record_batch_reader(recordBatchReaderPtr);
        }

        internal unsafe CLib.GArrowSourceNodeOptions* GetPtr()
        {
            return _optionsPtr;
        }
    }
}
