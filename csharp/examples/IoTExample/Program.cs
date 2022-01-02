﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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
using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow;

namespace IoTPipelineExample
{
    class Program
    {
        public static int concurrencyLevel = 8;
        public static int totalInputs = 1_000;
        public static int queueCapacity = 1_000_000;

        public static async Task Main(string[] args)
        {
            SensorDataPipeline sdp = new SensorDataPipeline(totalInputs, queueCapacity);
            List<Task> tasks = new List<Task>();
            List<RecordBatch> recordBatches = new List<RecordBatch>();

            Console.WriteLine("Producing IoT sensor data...");
            for (int i = 0; i < concurrencyLevel; i++)
            {
                int j = i;
                Task t = Task.Run(() => sdp.WriteToChannel(j));
                tasks.Add(t);
            }

            Console.WriteLine("Consuming IoT sensor data...");
            tasks.Add(Task.Run(() => sdp.ReadFromChannel()));

            Console.WriteLine("Waiting for all tasks to complete...");
            Task.WaitAll(tasks.ToArray());

            Console.WriteLine("Persisting data to disk...");
            var arrowDataPath = await sdp.PersistData();

            Console.WriteLine("Loading arrow data file into memory...");
            string[] fileEntries = Directory.GetFiles(arrowDataPath);

            foreach (string fileEntry in fileEntries)
            {
                await ReadArrowFile(fileEntry);
            }

            // The logical table is for duplication removal, late arrival data manipulation and adding feature columns
            Table table1001 = Table.TableFromRecordBatches(recordBatches[0].Schema, recordBatches);

            async Task ReadArrowFile(string fileEntry)
            {
                Console.WriteLine($"Reading data from arrow file {Path.GetFileName(fileEntry)}...");

                using (var stream = File.OpenRead(fileEntry))
                using (var reader = new ArrowFileReader(stream))
                {
                    try
                    {
                        int count = await reader.RecordBatchCountAsync();

                        for (int i = 0; i < count; i++)
                        {
                            var recordBatch = await reader.ReadRecordBatchAsync(i);

                            for (int j = 0; j < recordBatch.ColumnCount; j++)
                            {
                                Console.WriteLine($"RecordBatch {i.ToString().PadLeft(6)} Column {j} Length is: "
                                    + recordBatch.Column(j).Data.Length.ToString().PadLeft(6)
                                    + " NULL Count is: "
                                    + recordBatch.Column(j).Data.NullCount);
                            }

                            if (recordBatch.Schema.HasMetadata && recordBatch.Schema.Metadata.TryGetValue("SubjectId", out string subjectId))
                            {
                                if (subjectId == "1001")
                                {
                                    recordBatches.Add(recordBatch);
                                    break;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }
    }
}
