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
    public class ExportedSchema
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowSchema* d_garrow_schema_import(CArrowSchema* c_abi_schema, out GError** error);
        private static d_garrow_schema_import garrow_schema_import = FuncLoader.LoadFunction<d_garrow_schema_import>("garrow_schema_import");

        public unsafe GArrowSchema* Handle { get; }

        public unsafe ExportedSchema(Schema schema)
        {
            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportSchema(schema, cSchema);
            Handle = garrow_schema_import(cSchema, out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }
    }
}
