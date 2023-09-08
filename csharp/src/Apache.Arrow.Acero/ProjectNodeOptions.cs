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
using System.Runtime.InteropServices;
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class ProjectNodeOptions : ExecNodeOptions
    {
        private readonly unsafe GArrowProjectNodeOptions* _optionsPtr;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowProjectNodeOptions* d_garrow_project_node_options_new(GList* expressions, IntPtr names, int n_names);
        private static d_garrow_project_node_options_new garrow_project_node_options_new = FuncLoader.LoadFunction<d_garrow_project_node_options_new>("garrow_project_node_options_new");

        internal unsafe GArrowProjectNodeOptions* Handle => _optionsPtr;

        public unsafe ProjectNodeOptions(List<Expression> expressions, List<string> names)
        {
            var list = new GLib.List(IntPtr.Zero);

            foreach (Expression expression in expressions)
                list.Append(expression.Handle);

            IntPtr namesPtr = GLib.Marshaller.StringArrayToStrvPtr(names.ToArray());

            _optionsPtr = garrow_project_node_options_new((GList*)list.Handle, namesPtr, names.Count);
        }
    }
}
