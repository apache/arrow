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
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace IoTExample
{
    class Program
    {
        public static int concurrencyLevel = 1;
        public static int totalInputs = 10_000_000;
        public static int queueCapacity = 1_000_000;

        public static async Task Main(string[] args)
        {
            SampleDataset sd = new SampleDataset(totalInputs, queueCapacity);

            Console.WriteLine("Receiving IoT data...");
            Task t1 = Task.Run(() => sd.Produce());

            Console.WriteLine("Transforming data...");
            Task t2 = Task.Run(() => sd.Consume());

            // Wait for all tasks to complete
            Task.WaitAll(t1, t2);

            var success = await sd.PersistData();

            if (!success)
                return;

            Console.WriteLine("Reading arrow files...");
            var stream = File.OpenRead(@"c:\temp\data\iotbigdata.arrow");
            var reader = new ArrowFileReader(stream);
            int totalSubject = 0;
            var count = await reader.RecordBatchCountAsync();

            for (int i = 0; i < count; i++)
            {
                var recordBatch = await reader.ReadRecordBatchAsync(i);

                for (int j = 0; j < recordBatch.ColumnCount; j++)
                {
                    Console.WriteLine($"Record count in recordBatch {i} column {j} is: " + recordBatch.Column(j).Data.Length);
                    Console.WriteLine($"Null count in recordBatch {i} column {j} is: " + recordBatch.Column(j).Data.NullCount);
                }

                totalSubject += recordBatch.Column("SubjectId").Length;
            }

            Console.WriteLine(totalSubject);
        }

    }

}
