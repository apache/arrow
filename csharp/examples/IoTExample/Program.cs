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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;

namespace IoTExample
{
    class Program
    {
        public static int concurrencyLevel = 1;
        public static int totalInputs = 1_000_000_000;
        public static int queueCapacity = 1_000_000;
        // Use a specific memory pool from which arrays will be allocated (optional)
        static NativeMemoryAllocator memoryAllocator;


        // A Real-time C# memory-based smartwatch and smartphone IoT data analytics platform,
        // which leverages Apache Arrow as the unified data store.
        public static async Task Main(string[] args)
        {
            memoryAllocator = new NativeMemoryAllocator(alignment: 64);

            SampleDataset sd = new SampleDataset(totalInputs, queueCapacity, memoryAllocator);

            Console.WriteLine("Producing data...");
            Task t1 = Task.Run(() => sd.Produce());

            Console.WriteLine("Consuming data...");
            Task t2 = Task.Run(() => sd.Consume());

            // Wait for all tasks to complete
            Task.WaitAll(t1, t2);

            // Data analytics in real time
            // Your business logics go here

            Console.WriteLine("Reading arrow files...");
            var stream = File.OpenWrite(@"c:\temp\data\");
            var reader = new ArrowFileReader(stream, true);

            var recordBatch = reader.ReadNextRecordBatch();

            Console.WriteLine(recordBatch.Column(0).Data.NullCount);

        }

    }

}
