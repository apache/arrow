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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Acero
{
    public abstract class Expression
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate IntPtr d_garrow_expression_to_string(IntPtr expression);
        private static d_garrow_expression_to_string garrow_expression_to_string = FuncLoader.LoadFunction<d_garrow_expression_to_string>("garrow_expression_to_string");

        public IntPtr Handle { get; protected set; }

        public override unsafe string ToString()
        {
            IntPtr strPtr = garrow_expression_to_string(Handle);

            return GLib.Marshaller.PtrToStringGFree(strPtr);
        }
    }
}
