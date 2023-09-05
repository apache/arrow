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
// using Apache.Arrow.Ipc;

using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Acero
{
    public class Declaration
    {
        private readonly string _factoryName;
        private readonly ExecNodeOptions _options;

        public List<Declaration> Inputs { get; set; } = new();

        public Declaration(string factoryName, ExecNodeOptions options = null, List<Declaration> inputs = null)
        {
            _factoryName = factoryName;
            _options = options;

            if (inputs != null) Inputs = inputs;
        }

        public static Declaration FromSequence(List<Declaration> decls)
        {
            var items = decls.ToList();

            Declaration first = items.Last();
            items.RemoveAt(items.Count - 1);

            Declaration receiver = first;

            while (items.Any())
            {
                Declaration input = items.Last();
                items.RemoveAt(items.Count - 1);

                receiver.Inputs.Add(input);
                receiver = receiver.Inputs.First();
            }

            return first;
        }

        public IArrowArrayStream ToRecordBatchReader(Schema schema)
        {
            return ToRecordBatchReader(this, schema);
        }

        public static IArrowArrayStream ToRecordBatchReader(Declaration declaration, Schema schema)
        {
            var sinkNodeOptions = new SinkNodeOptions(schema);
            var plan = new ExecPlan();

            Declaration sinkNode = FromSequence(
                new List<Declaration>
                {
                    declaration,
                    new Declaration("table_sink", sinkNodeOptions)
                });

            sinkNode.AddToPlan(plan);

            if (!plan.Validate())
                throw new InvalidOperationException("Plan is not valid");

            plan.StartProducing();
            plan.Wait();

            return sinkNodeOptions.GetRecordBatchReader();
        }

        private ExecNode AddToPlan(ExecPlan plan)
        {
            var nodes = new List<ExecNode>();

            foreach (var input in Inputs)
            {
                var node = input.AddToPlan(plan);
                nodes.Add(node);
            }

            switch (_factoryName)
            {
                case "table_sink":
                    return new SinkNode(_options as SinkNodeOptions, plan, nodes);

                case "record_batch_source":
                    return new RecordBatchSourceNode(_options as RecordBatchSourceNodeOptions, plan);

                case "record_batch_reader_source":
                    return new RecordBatchReaderSourceNode(_options as RecordBatchReaderSourceNodeOptions, plan);

                case "hashjoin":
                    return new HashJoinNode(_options as HashJoinNodeOptions, plan, nodes);

                case "filter":
                    return new FilterNode(_options as FilterNodeOptions, plan, nodes);

                case "union":
                    return new UnionNode(plan, nodes);

                default:
                    throw new Exception($"Unknown factory {_factoryName}");
            }
        }
    }
}
