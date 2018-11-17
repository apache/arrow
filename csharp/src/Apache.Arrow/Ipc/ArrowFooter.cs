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
using System.Linq;

namespace Apache.Arrow.Ipc
{
    internal class ArrowFooter
    {
        public Schema Schema { get; }
        private readonly List<Block> _dictionaries;
        private readonly List<Block> _recordBatches;

        public IEnumerable<Block> Dictionaries => _dictionaries;
        public IEnumerable<Block> RecordBatches => _recordBatches;

        public Block GetRecordBatchBlock(int i) => _recordBatches[i];

        public Block GetDictionaryBlock(int i) => _dictionaries[i];

        public int RecordBatchCount => _recordBatches.Count;
        public int DictionaryCount => _dictionaries.Count;

        public ArrowFooter(Schema schema, IEnumerable<Block> dictionaries, IEnumerable<Block> recordBatches)
        {
            Schema = schema;

            _dictionaries = dictionaries.ToList();
            _recordBatches = recordBatches.ToList();
        }

        public ArrowFooter(Flatbuf.Footer footer)
            : this(Ipc.MessageSerializer.GetSchema(footer.Schema.GetValueOrDefault()), GetDictionaries(footer),
                GetRecordBatches(footer))
        { }

        private static IEnumerable<Block> GetDictionaries(Flatbuf.Footer footer)
        {
            for (var i = 0; i < footer.DictionariesLength; i++)
            {
                var block = footer.Dictionaries(i);

                if (block.HasValue)
                {
                    yield return new Block(block.Value);
                }
            }
        }

        private static IEnumerable<Block> GetRecordBatches(Flatbuf.Footer footer)
        {
            for (var i = 0; i < footer.RecordBatchesLength; i++)
            {
                var block = footer.RecordBatches(i);

                if (block.HasValue)
                {
                    yield return new Block(block.Value);
                }
            }
        }

    }
}
