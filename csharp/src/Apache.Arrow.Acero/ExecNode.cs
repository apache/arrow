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
using System.Threading.Tasks;
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public abstract class ExecNode : IDisposable
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        protected unsafe delegate GArrowExecuteNode* d_garrow_execute_plan_build_node(GArrowExecutePlan* plan, IntPtr factory_name, IntPtr inputs, IntPtr options, out GError** error);
        protected static d_garrow_execute_plan_build_node garrow_execute_plan_build_node = FuncLoader.LoadFunction<d_garrow_execute_plan_build_node>("garrow_execute_plan_build_node");

        public unsafe GArrowExecuteNode* Handle { get; protected set; }

        public ExecPlan Plan { get; }

        public ExecNode Output { get; set; }

        public List<ExecNode> Inputs { get; }

        public ExecNode(ExecPlan plan, List<ExecNode> inputs)
        {
            Plan = plan;
            Inputs = inputs;

            foreach (ExecNode input in inputs)
                input.Output = this;
        }

        public virtual Task Init() => Task.CompletedTask;

        public abstract void Dispose();
    }
}
