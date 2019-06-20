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

using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace FluentBuilderExample
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            // Use a specific memory pool from which arrays will be allocated (optional)

            var memoryAllocator = new NativeMemoryAllocator(alignment: 64);

            // Build a record batch using the Fluent API

            var recordBatch = new RecordBatch.Builder(memoryAllocator)
                .Append("Column A", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(0, 10))))
                .Append("Column B", false, col => col.Float(array => array.AppendRange(Enumerable.Range(0, 10).Select(x => Convert.ToSingle(x * 2)))))
                .Append("Column C", false, col => col.String(array => array.AppendRange(Enumerable.Range(0, 10).Select(x => $"Item {x+1}"))))
                .Append("Column D", false, col => col.Boolean(array => array.AppendRange(Enumerable.Range(0, 10).Select(x => x % 2 == 0))))
                .Build();

            // Print memory allocation statistics

            Console.WriteLine("Allocations: {0}", memoryAllocator.Statistics.Allocations);
            Console.WriteLine("Allocated: {0} byte(s)", memoryAllocator.Statistics.BytesAllocated);

            // Write record batch to a file

            using (var stream = File.OpenWrite("test.arrow"))
            using (var writer = new ArrowFileWriter(stream, recordBatch.Schema))
            {
                await writer.WriteRecordBatchAsync(recordBatch);
                await writer.WriteFooterAsync();
            }

            Console.WriteLine("Done");
            Console.ReadKey();
        }
    }
}
