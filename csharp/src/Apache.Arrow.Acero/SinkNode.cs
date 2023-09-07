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
using static Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class SinkNode : ExecNode
    {
        private unsafe CLib.GArrowExecuteNode* _nodePtr;

        public unsafe SinkNode(SinkNodeOptions _options, ExecPlan plan, List<ExecNode> inputs)
        {
            GError** error;

            _nodePtr = CLib.garrow_execute_plan_build_sink_node(plan.GetPtr(), inputs[0].GetPtr(), _options.GetPtr(), out error);

            ExceptionUtil.ThrowOnError(error);
        }

        public unsafe override CLib.GArrowExecuteNode* GetPtr()
        {
            return _nodePtr;
        }
    }
}
