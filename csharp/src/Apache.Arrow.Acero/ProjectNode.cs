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

using System.Collections.Generic;
using System.Runtime.InteropServices;
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class ProjectNode : ExecNode
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        protected unsafe delegate GArrowExecuteNode* d_garrow_execute_plan_build_project_node(GArrowExecutePlan* plan, GArrowExecuteNode* input, GArrowProjectNodeOptions* options, out GError** error);
        protected static d_garrow_execute_plan_build_project_node garrow_execute_plan_build_project_node = FuncLoader.LoadFunction<d_garrow_execute_plan_build_project_node>("garrow_execute_plan_build_project_node");

        public unsafe ProjectNode(ProjectNodeOptions options, ExecPlan plan, List<ExecNode> inputs)
            : base(plan, inputs)
        {
            Handle = garrow_execute_plan_build_project_node(plan.Handle, inputs[0].Handle, options.Handle, out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }

        public override void Dispose() { }
    }
}
