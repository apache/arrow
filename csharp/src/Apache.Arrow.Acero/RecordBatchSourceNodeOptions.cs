﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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

namespace Apache.Arrow.Acero
{
    public class RecordBatchSourceNodeOptions : ExecNodeOptions
    {
        private readonly ExportedRecordBatch _exportedRecordBatch;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        protected unsafe delegate GArrowSourceNodeOptions* d_garrow_source_node_options_new_record_batch(GArrowRecordBatch* record_batch);
        protected static d_garrow_source_node_options_new_record_batch garrow_source_node_options_new_record_batch = FuncLoader.LoadFunction<d_garrow_source_node_options_new_record_batch>("garrow_source_node_options_new_record_batch");

        public unsafe GArrowSourceNodeOptions* Handle { get; }

        public unsafe RecordBatchSourceNodeOptions(RecordBatch recordBatch)
        {
            _exportedRecordBatch = new ExportedRecordBatch(recordBatch);

            Handle = garrow_source_node_options_new_record_batch(_exportedRecordBatch.Handle);
        }
    }
}
