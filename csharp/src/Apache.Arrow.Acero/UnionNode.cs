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
    public class UnionNode : ExecNode, IDisposable
    {
        private readonly unsafe GArrowExecuteNodeOptions* _optionsPtr;
        private readonly IntPtr _factoryNamePtr;
        private readonly GLib.List _inputs;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        protected unsafe delegate IntPtr d_garrow_sink_node_options_new();
        protected static d_garrow_sink_node_options_new garrow_sink_node_options_new = FuncLoader.LoadFunction<d_garrow_sink_node_options_new>("garrow_sink_node_options_new");

        public unsafe UnionNode(ExecPlan plan, List<ExecNode> inputs) : base(plan, inputs)
        {
            _factoryNamePtr = GLib.Marshaller.StringToPtrGStrdup("union");

            ExecNode lhs = inputs[0];
            ExecNode rhs = inputs[1];

            _inputs = new GLib.List(IntPtr.Zero);
            _inputs.Append((IntPtr)lhs.Handle);
            _inputs.Append((IntPtr)rhs.Handle);

            _optionsPtr = (GArrowExecuteNodeOptions*)Marshal.AllocHGlobal(sizeof(GArrowExecuteNodeOptions));
            *_optionsPtr = default;

            Handle = garrow_execute_plan_build_node(plan.Handle, _factoryNamePtr, _inputs.Handle, (IntPtr)_optionsPtr, out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }

        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual unsafe void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inputs.Dispose();
            }

            GLib.Marshaller.Free(_factoryNamePtr);
            Marshal.FreeHGlobal((IntPtr)_optionsPtr);
        }

        ~UnionNode()
        {
            Dispose(false);
        }
    }
}
