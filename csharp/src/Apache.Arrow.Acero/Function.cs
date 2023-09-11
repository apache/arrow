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
    public class Function : Expression
    {
        private readonly IntPtr _functionNamePtr;
        private readonly GLib.List _functionArgs;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowFilterNodeOptions* d_garrow_call_expression_new(IntPtr function, IntPtr arguments, GArrowFunctionOptions* options);
        private static d_garrow_call_expression_new garrow_call_expression_new = FuncLoader.LoadFunction<d_garrow_call_expression_new>("garrow_call_expression_new");

        public unsafe Function(string functionName, Expression lhs, Expression rhs)
            : this(functionName, new[] { lhs, rhs })
        {
        }

        public unsafe Function(string functionName, params Expression[] args)
        {
            _functionNamePtr = GLib.Marshaller.StringToPtrGStrdup(functionName);
            _functionArgs = new GLib.List(IntPtr.Zero);

            foreach (Expression arg in args)
                _functionArgs.Append(arg.Handle);

            Handle = (IntPtr)garrow_call_expression_new(_functionNamePtr, _functionArgs.Handle, null);
        }

        ~Function()
        {
            GLib.Marshaller.Free(_functionNamePtr);
        }
    }

    public class Equal : Function
    {
        public Equal(Expression lhs, Expression rhs) : base("equal", lhs, rhs) { }
    }

    public class Or : Function
    {
        public Or(Expression lhs, Expression rhs) : base("or", lhs, rhs) { }
    }
}
