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

namespace Apache.Arrow.Acero
{
    public abstract class ExecNodeFactoryRegistry
    {
        public abstract ExecNode MakeNode(string factoryName, ExecPlan plan, ExecNodeOptions options, List<ExecNode> inputs);
    }

    public class DefaultExecNodeFactoryRegistry : ExecNodeFactoryRegistry
    {
        public override ExecNode MakeNode(string factoryName, ExecPlan plan, ExecNodeOptions options, List<ExecNode> inputs)
        {
            switch (factoryName)
            {
                case "sink":
                    return new SinkNode(options as SinkNodeOptions, plan, inputs);

                case "record_batch_source":
                    return new RecordBatchSourceNode(options as RecordBatchSourceNodeOptions, plan);

                case "record_batch_reader_source":
                    return new RecordBatchReaderSourceNode(options as RecordBatchReaderSourceNodeOptions, plan);

                case "hashjoin":
                    return new HashJoinNode(options as HashJoinNodeOptions, plan, inputs);

                case "filter":
                    return new FilterNode(options as FilterNodeOptions, plan, inputs);

                case "union":
                    return new UnionNode(plan, inputs);

                case "project":
                    return new ProjectNode(options as ProjectNodeOptions, plan, inputs);

                default:
                    throw new InvalidOperationException($"Unknown factory {factoryName}");
            }
        }
    }
}
