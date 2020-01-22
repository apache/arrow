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

using Apache.Arrow.Ipc;
using Apache.Arrow.Tests;
using BenchmarkDotNet.Attributes;
using System.IO;
using System.Threading.Tasks;

namespace Apache.Arrow.Benchmarks
{
    //[EtwProfiler] - needs elevated privileges
    [MemoryDiagnoser]
    public class ArrowWriterBenchmark
    {
        [Params(10_000, 1_000_000)]
        public int BatchLength{ get; set; }

        [Params(10, 25)]
        public int ColumnSetCount { get; set; }

        private MemoryStream _memoryStream;
        private RecordBatch _batch;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _batch = TestData.CreateSampleRecordBatch(BatchLength, ColumnSetCount);
            _memoryStream = new MemoryStream();
        }

        [IterationSetup]
        public void Setup()
        {
            _memoryStream.Position = 0;
        }

        [Benchmark]
        public async Task WriteBatch()
        {
            ArrowStreamWriter writer = new ArrowStreamWriter(_memoryStream, _batch.Schema);
            await writer.WriteRecordBatchAsync(_batch);
        }
    }
}
