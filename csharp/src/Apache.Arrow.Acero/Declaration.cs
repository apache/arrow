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
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Acero
{
    public class Declaration
    {
        private readonly string _factoryName;
        private readonly ExecNodeOptions _options;
        private readonly ExecNodeFactoryRegistry _registry;

        public List<Declaration> Inputs { get; set; } = new();

        public Declaration(string factoryName, ExecNodeOptions options = null, List<Declaration> inputs = null, ExecNodeFactoryRegistry registry = null)
        {
            _factoryName = factoryName;
            _options = options;
            _registry = registry ?? new DefaultExecNodeFactoryRegistry();

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

        public Task<IArrowArrayStream> ToRecordBatchReader(Schema schema)
        {
            return ToRecordBatchReader(this, schema);
        }

        public static async Task<IArrowArrayStream> ToRecordBatchReader(Declaration declaration, Schema schema)
        {
            var sinkNodeOptions = new SinkNodeOptions(schema);
            var plan = new ExecPlan();

            Declaration sinkNode = FromSequence(
                new List<Declaration>
                {
                    declaration,
                    new Declaration("sink", sinkNodeOptions)
                });

            sinkNode.AddToPlan(plan);

            if (!plan.Validate())
                throw new InvalidOperationException("Plan is not valid");

            await plan.Init();

            plan.StartProducing();
            plan.Wait();

            return sinkNodeOptions.GetRecordBatchReader();
        }

        private ExecNode AddToPlan(ExecPlan plan)
        {
            var inputNodes = new List<ExecNode>();

            foreach (Declaration input in Inputs)
            {
                ExecNode inputNode = input.AddToPlan(plan);
                inputNodes.Add(inputNode);
            }

            ExecNode node = _registry.MakeNode(_factoryName, plan, _options, inputNodes);

            plan.AddNode(node);

            return node;
        }
    }
}
