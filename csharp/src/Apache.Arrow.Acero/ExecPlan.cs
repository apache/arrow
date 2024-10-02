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
using System.Threading.Tasks;
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class ExecPlan
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowExecutePlan* d_garrow_execute_plan_new(out GError** error);
        private static d_garrow_execute_plan_new garrow_execute_plan_new = FuncLoader.LoadFunction<d_garrow_execute_plan_new>("garrow_execute_plan_new");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate bool d_garrow_execute_plan_validate(GArrowExecutePlan* plan, out GError** error);
        private static d_garrow_execute_plan_validate garrow_execute_plan_validate = FuncLoader.LoadFunction<d_garrow_execute_plan_validate>("garrow_execute_plan_validate");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate bool d_garrow_execute_plan_start(GArrowExecutePlan* plan);
        private static d_garrow_execute_plan_start garrow_execute_plan_start = FuncLoader.LoadFunction<d_garrow_execute_plan_start>("garrow_execute_plan_start");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate bool d_garrow_execute_plan_wait(GArrowExecutePlan* plan);
        private static d_garrow_execute_plan_wait garrow_execute_plan_wait = FuncLoader.LoadFunction<d_garrow_execute_plan_wait>("garrow_execute_plan_wait");

        public unsafe GArrowExecutePlan* Handle { get; }

        public List<ExecNode> Nodes { get; internal set; } = new();

        public unsafe ExecPlan()
        {
            Handle = garrow_execute_plan_new(out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }

        public unsafe bool Validate()
        {
            bool valid = garrow_execute_plan_validate(Handle, out GError** error);

            ExceptionUtil.ThrowOnError(error);

            return valid;
        }

        public async Task Init()
        {
            ExecNode[] sortedNodes = TopoSort();

            foreach (ExecNode node in sortedNodes)
                await node.Init();
        }

        public unsafe void StartProducing()
        {
            garrow_execute_plan_start(Handle);
        }

        public unsafe void Wait()
        {
            garrow_execute_plan_wait(Handle);
        }

        internal void AddNode(ExecNode node)
        {
            Nodes.Add(node);
        }

        /// <summary>
        /// Sort nodes, producers precede consumers
        /// https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/exec_plan.cc#L239
        /// </summary>
        /// <returns>Sorted nodes</returns>
        public ExecNode[] TopoSort()
        {
            var visited = new List<ExecNode>();
            var sorted = new ExecNode[Nodes.Count];

            foreach (ExecNode node in Nodes)
                Visit(node);

            return sorted;

            void Visit(ExecNode node)
            {
                if (visited.Contains(node)) return;

                foreach (ExecNode input in node.Inputs)
                    Visit(input);

                sorted[visited.Count] = node;
                visited.Add(node);
            }
        }
    }
}
