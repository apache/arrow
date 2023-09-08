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
    public class LiteralExpression : Expression
    {
        private unsafe readonly GArrowFieldExpression* _expressionPtr;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowFieldExpression* d_garrow_literal_expression_new(GArrowDatum* datum);
        private static d_garrow_literal_expression_new garrow_literal_expression_new = FuncLoader.LoadFunction<d_garrow_literal_expression_new>("garrow_literal_expression_new");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowScalarDatum* d_garrow_scalar_datum_new(IntPtr value);
        private static d_garrow_scalar_datum_new garrow_scalar_datum_new = FuncLoader.LoadFunction<d_garrow_scalar_datum_new>("garrow_scalar_datum_new");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowBuffer* d_garrow_buffer_new(IntPtr data, long size);
        private static d_garrow_buffer_new garrow_buffer_new = FuncLoader.LoadFunction<d_garrow_buffer_new>("garrow_buffer_new");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowStringScalar* d_garrow_string_scalar_new(GArrowBuffer* value);
        private static d_garrow_string_scalar_new garrow_string_scalar_new = FuncLoader.LoadFunction<d_garrow_string_scalar_new>("garrow_string_scalar_new");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowInt32Scalar* d_garrow_int32_scalar_new(int value);
        private static d_garrow_int32_scalar_new garrow_int32_scalar_new = FuncLoader.LoadFunction<d_garrow_int32_scalar_new>("garrow_int32_scalar_new");

        public unsafe override IntPtr Handle => (IntPtr)_expressionPtr;

        public unsafe LiteralExpression(string literal)
        {
            IntPtr dataPtr = GLib.Marshaller.StringToPtrGStrdup(literal);
            GArrowBuffer* bufferPtr = garrow_buffer_new(dataPtr, literal.Length);
            GArrowStringScalar* scalarPtr = garrow_string_scalar_new(bufferPtr);
            GArrowScalarDatum* datumPtr = garrow_scalar_datum_new((IntPtr)scalarPtr);

            _expressionPtr = garrow_literal_expression_new((GArrowDatum*)datumPtr);
        }

        public unsafe LiteralExpression(int literal)
        {
            GArrowInt32Scalar* scalarPtr = garrow_int32_scalar_new(literal);
            GArrowScalarDatum* datumPtr = garrow_scalar_datum_new((IntPtr)scalarPtr);

            _expressionPtr = garrow_literal_expression_new((GArrowDatum*)datumPtr);
        }
    }
}
