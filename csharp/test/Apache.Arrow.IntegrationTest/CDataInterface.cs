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
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using Apache.Arrow.C;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Apache.Arrow.IntegrationTest
{
    /// <summary>
    /// Bridge for C Data Interface integration testing.
    /// These methods are called from the Python integration testing
    /// harness provided by Archery.
    /// </summary>
    public static class CDataInterface
    {
        // Archery uses the `pythonnet` library (*) to invoke .Net DLLs.
        // `pythonnet` is only able to marshal simple types such as int and
        // str, which is why we provide trivial wrappers around other APIs.
        //
        // (*) https://pythonnet.github.io/

        public static void Initialize()
        {
            // Allow debugging using Debug.WriteLine()
            Trace.Listeners.Add(new ConsoleTraceListener());
        }

        public static unsafe Schema ImportSchema(long ptr)
        {
            return CArrowSchemaImporter.ImportSchema((CArrowSchema*) ptr);
        }

        public static unsafe void ExportSchema(Schema schema, long ptr)
        {
            CArrowSchemaExporter.ExportSchema(schema, (CArrowSchema*) ptr);
        }

        public static unsafe RecordBatch ImportRecordBatch(long ptr, Schema schema)
        {
            return CArrowArrayImporter.ImportRecordBatch((CArrowArray*) ptr, schema);
        }

        public static unsafe void ExportRecordBatch(RecordBatch batch, long ptr)
        {
            CArrowArrayExporter.ExportRecordBatch(batch, (CArrowArray*) ptr);
        }

        public static JsonFile ParseJsonFile(string jsonPath)
        {
            return JsonFile.Parse(new FileInfo(jsonPath));
        }

        public static void RunGC()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }
}
