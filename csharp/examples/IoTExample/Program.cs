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
using System.Diagnostics;
using System.Collections.Generic;
using IoTExample.Model;

namespace IoTExample
{
    class Program
    {
        public static int concurrencyLevel = 1;
        public static int inputs = 1_000_000_000;
        public static int capacity = 100_000;

        // A Real-time C# memory-based smartwatch and smartphone IoT data analytics platform,
        // which leverages Apache Arrow as the unified data store.
        public static async Task Main(string[] args)
        {
            SampleDataset sd = new SampleDataset(inputs, capacity);
            Dictionary<string, List<object>> results = new Dictionary<string, List<object>>();

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            Console.WriteLine("Producing data...");
            Task t1 = Task.Run(() => sd.Produce());

            Console.WriteLine("Consuming data...");
            Task t2 = Task.Run(() => sd.Consume());

            // Wait for all tasks to complete
            Task.WaitAll(t1, t2);

            stopwatch.Stop();
            Console.WriteLine($"Elapsed Milliseconds is: {stopwatch.ElapsedMilliseconds}");

            TimeSpan ts = stopwatch.Elapsed;
            Console.WriteLine($"Total Runtime is: {ts.Minutes} min {ts.Seconds} sec");
        }
    }
}
