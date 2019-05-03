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
using Apache.Arrow.Memory;
using Apache.Arrow.Tests;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Apache.Arrow.Benchmarks
{
    //[EtwProfiler] - needs elevated privileges
    [MemoryDiagnoser]
    public class ArrowReaderBenchmark
    {
        [Params(10_000, 1_000_000)]
        public int Count { get; set; }

        private MemoryStream _memoryStream;
        private static readonly MemoryAllocator s_allocator = new TestMemoryAllocator();

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            RecordBatch batch = TestData.CreateSampleRecordBatch(length: Count);
            _memoryStream = new MemoryStream();

            ArrowStreamWriter writer = new ArrowStreamWriter(_memoryStream, batch.Schema);
            await writer.WriteRecordBatchAsync(batch);
        }

        [IterationSetup]
        public void Setup()
        {
            _memoryStream.Position = 0;
        }

        [Benchmark]
        public async Task<double> ArrowReaderWithMemoryStream()
        {
            double sum = 0;
            var reader = new ArrowStreamReader(_memoryStream);
            RecordBatch recordBatch;
            while ((recordBatch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                using (recordBatch)
                {
                    sum += SumAllNumbers(recordBatch);
                }
            }
            return sum;
        }

        [Benchmark]
        public async Task<double> ArrowReaderWithMemoryStream_ManagedMemory()
        {
            double sum = 0;
            var reader = new ArrowStreamReader(_memoryStream, s_allocator);
            RecordBatch recordBatch;
            while ((recordBatch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                using (recordBatch)
                {
                    sum += SumAllNumbers(recordBatch);
                }
            }
            return sum;
        }

        [Benchmark]
        public async Task<double> ArrowReaderWithMemory()
        {
            double sum = 0;
            var reader = new ArrowStreamReader(_memoryStream.GetBuffer());
            RecordBatch recordBatch;
            while ((recordBatch = await reader.ReadNextRecordBatchAsync()) != null)
            {
                using (recordBatch)
                {
                    sum += SumAllNumbers(recordBatch);
                }
            }
            return sum;
        }

        private static double SumAllNumbers(RecordBatch recordBatch)
        {
            double sum = 0;

            for (int k = 0; k < recordBatch.ColumnCount; k++)
            {
                var array = recordBatch.Arrays.ElementAt(k);
                switch (recordBatch.Schema.GetFieldByIndex(k).DataType.TypeId)
                {
                    case ArrowTypeId.Int64:
                        Int64Array int64Array = (Int64Array)array;
                        sum += Sum(int64Array);
                        break;
                    case ArrowTypeId.Double:
                        DoubleArray doubleArray = (DoubleArray)array;
                        sum += Sum(doubleArray);
                        break;
                }
            }
            return sum;
        }

        private static double Sum(DoubleArray doubleArray)
        {
            double sum = 0;
            ReadOnlySpan<double> values = doubleArray.Values;
            for (int valueIndex = 0; valueIndex < values.Length; valueIndex++)
            {
                sum += values[valueIndex];
            }
            return sum;
        }

        private static long Sum(Int64Array int64Array)
        {
            long sum = 0;
            ReadOnlySpan<long> values = int64Array.Values;
            for (int valueIndex = 0; valueIndex < values.Length; valueIndex++)
            {
                sum += values[valueIndex];
            }
            return sum;
        }
    }
}
