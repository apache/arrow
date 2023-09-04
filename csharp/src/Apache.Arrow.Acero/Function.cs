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
    public class Function : Expression
    {
        private IntPtr _ptr;

        public unsafe Function(string functionName, Expression lhs, Expression rhs)
        {
            var functionNamePtr = StringUtil.ToCStringUtf8(functionName);

            var rhsItem = new GList
            {
                data = rhs.GetPtr()
            };

            var lhsItem = new GList
            {
                data = lhs.GetPtr(),
                next = &rhsItem
            };

            IntPtr glistPtr = Marshal.AllocHGlobal(Marshal.SizeOf<GList>());
            Marshal.StructureToPtr<GList>(lhsItem, glistPtr, false);

            _ptr = (nint)CLib.garrow_call_expression_new((IntPtr)functionNamePtr, glistPtr, null);
        }

        public override IntPtr GetPtr()
        {
            return _ptr;
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
