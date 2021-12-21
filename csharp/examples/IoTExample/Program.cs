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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace IoTPipelineExample
{
    class Program
    {
        public static int concurrencyLevel = 8;
        public static int totalSensorData = 1_000_000;
        public static int queueCapacity = 1_000_000;

        public static async Task Main(string[] args)
        {
            //SampleDataPipeline sdp = new SampleDataPipeline(concurrencyLevel, totalSensorData, queueCapacity);
            //List<Task> tasks = new List<Task>();

            //Console.WriteLine("Producing IoT sensor data concurrently...");
            //for (int i = 0; i < concurrencyLevel; i++)
            //{
            //    int j = i;
            //    Task t = Task.Run(() => sdp.WriteToChannel(j));
            //    tasks.Add(t);
            //}

            //Console.WriteLine("Consuming IoT sensor data concurrently...");
            //for (int i = 0; i < concurrencyLevel; i++)
            //{
            //    int j = i;
            //    Task t = Task.Run(() => sdp.ReadFromChannel(j));
            //    tasks.Add(t);
            //}

            //Console.WriteLine("Waiting for all tasks to complete...");
            //Task.WaitAll(tasks.ToArray());

            //var success = await sdp.PersistData();

            ////string filePath = "iotbigdata.arrow";
            //if (!success)
            //    return;

            Console.WriteLine("Loading arrow data file into memory...");
            string[] fileEntries = Directory.GetFiles(@"c:\temp\data");

            foreach (string fileName in fileEntries)
            {
                ProcessFile(fileName);
            }
        }

        static void ProcessFile(string fileName)
        {
            //var stream = File.OpenRead(fileName);
            //var stream = File.OpenRead(@"c:\temp\data\iotbigdata_2.arrow");
            //var count =

            //try { await reader.RecordBatchCountAsync(); }
            //catch(Exception ex) { Console.WriteLine(ex.ToString()); }
                

            Console.WriteLine($"Reading data from arrow record batch {fileName}...");
            using (var stream = File.OpenRead(fileName))
            using (var reader = new ArrowFileReader(stream))
            {
                for (int i = 0; i < 1; i++)
                {
                    //var recordBatch = await reader.ReadRecordBatchAsync(i);
                    var recordBatch = reader.ReadNextRecordBatch();

                    for (int j = 0; j < recordBatch.ColumnCount; j++)
                    {
                        Console.WriteLine($"Total records in record batch {i} column {j} is: " + recordBatch.Column(j).Data.Length);
                        Console.WriteLine($"Null data count in record batch {i} column {j} is: " + recordBatch.Column(j).Data.NullCount);
                    }
                }
            }
                
        }
    }

}
