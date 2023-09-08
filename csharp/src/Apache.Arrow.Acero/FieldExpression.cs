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
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class FieldExpression : Expression
    {
        private readonly IntPtr _expressionPtr;
        private readonly IntPtr _referencePtr;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate IntPtr d_garrow_field_expression_new(IntPtr reference, out GError** error);
        private static d_garrow_field_expression_new garrow_field_expression_new = FuncLoader.LoadFunction<d_garrow_field_expression_new>("garrow_field_expression_new");

        public override IntPtr Handle => _expressionPtr;

        public unsafe FieldExpression(string reference)
        {
            _referencePtr = GLib.Marshaller.StringToPtrGStrdup(reference);
            _expressionPtr = garrow_field_expression_new(_referencePtr, out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }

        ~FieldExpression()
        {
            GLib.Marshaller.Free(_referencePtr);
        }
    }
}
