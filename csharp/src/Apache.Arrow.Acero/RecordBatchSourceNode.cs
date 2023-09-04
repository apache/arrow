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

namespace Apache.Arrow.Acero
{
    public class RecordBatchSourceNode : ExecNode
    {
        private unsafe CLib.GArrowExecuteNode* _nodePtr;

        public unsafe RecordBatchSourceNode(RecordBatchSourceNodeOptions options, ExecPlan plan)
        {
            _nodePtr = CLib.garrow_execute_plan_build_source_node(plan.GetPtr(), options.GetPtr(), null);
        }

        public override unsafe CLib.GArrowExecuteNode* GetPtr()
        {
            return _nodePtr;
        }
    }
}
