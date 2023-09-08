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
    public class UnionNode : ExecNode
    {
        private readonly unsafe GArrowExecuteNode* _nodePtr;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        protected unsafe delegate IntPtr d_garrow_sink_node_options_new();
        protected static d_garrow_sink_node_options_new garrow_sink_node_options_new = FuncLoader.LoadFunction<d_garrow_sink_node_options_new>("garrow_sink_node_options_new");

        public override unsafe GArrowExecuteNode* Handle => _nodePtr;

        public unsafe UnionNode(ExecPlan plan, List<ExecNode> nodes)
        {
            string factoryName = "union";
            IntPtr factoryNamePtr = GLib.Marshaller.StringToPtrGStrdup(factoryName);

            ExecNode lhs = nodes[0];
            ExecNode rhs = nodes[1];

            var list = new GLib.List(IntPtr.Zero);
            list.Append((IntPtr)lhs.Handle);
            list.Append((IntPtr)rhs.Handle);

            // todo: using this ptr causes the following error
            // g_object_new_valist: invalid unclassed object pointer for value type 'GArrowExecuteNodeOptions'
            //IntPtr optionsPtr = GLib.Marshaller.StructureToPtrAlloc(new GArrowExecuteNodeOptions());

            // for now just use the sink options
            IntPtr optionsPtr2 = garrow_sink_node_options_new();

            _nodePtr = garrow_execute_plan_build_node(plan.Handle, factoryNamePtr, list.Handle, optionsPtr2, out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }
    }
}
