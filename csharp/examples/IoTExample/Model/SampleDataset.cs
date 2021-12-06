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
using System.Collections.Concurrent;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;

namespace IoTExample.Model
{
    public class SampleDataset
    {
        private int _size;
        private int _inputs;
        private int _capacity;
        private BlockingCollection<SensorData> _rows;

        public List<int> colSubjectId;
        public List<string> colActivityLabel;
        public List<long> colTimestamp;
        public Dictionary<string, string> activityLabel;

        public SampleDataset(int inputs, int capacity)
        {
            _inputs = inputs;
            _capacity = capacity;
            _rows = new BlockingCollection<SensorData>(capacity);

            colSubjectId = new List<int>();
            colActivityLabel = new List<string>();
            colTimestamp = new List<long>();

            activityLabel = new Dictionary<string, string>()
            {
                {"walking", "A"},
                {"jogging", "B"},
                {"stairs", "C"},
                {"sitting", "D"},
                {"standing", "E"},
                {"typing", "F"},
                {"teeth", "G"},
                {"soup", "H"},
                {"chips", "I"},
                {"pasta", "J"},
                {"drinking", "K"},
                {"sandwich", "L"},
                {"kicking", "M"},
                {"catch", "O"},
                {"dribbling", "P"},
                {"writing", "Q"},
                {"clapping", "R"},
                {"folding", "S"},
            };
        }

        public int _checkpoint = 0;

        public void Produce()
        {
            Random rand = new Random();
            bool success;

            List<string> keyList = new List<string>(activityLabel.Keys);
            int count = keyList.Count;

            DateTime now = DateTime.Now;
            long unixTime = ((DateTimeOffset)now).ToUnixTimeSeconds();

            while (_size < _inputs)
            {
                string randomKey = keyList[rand.Next(count)];
                
                success = _rows.TryAdd(new SensorData
                {
                    subjectId = rand.Next(1000, 2001),
                    activityLabel = activityLabel[randomKey],
                    timestamp = unixTime++,
                    //x_Axis = rand.NextDouble(),
                    //y_Axis = rand.NextDouble(),
                    //z_Axis = rand.NextDouble(),
                }); 

                if (success)
                {
                    //Console.WriteLine($"Enqueue Task 0");
                    _size++;
                }
                else
                {
                    Console.WriteLine("Producing is blocked, percent completed is: {0}%", Math.Round((double)_size / _inputs, 4) * 100);
                }
            }

            _rows.CompleteAdding();
        }

        public void Consume()
        {
            while (!_rows.IsCompleted)
            {
                if (!_rows.TryTake(out SensorData item, 3000))
                {
                    Console.WriteLine("Consuming is blocked!");
                }
                else
                {
                    //Console.WriteLine($"Dequeue Task 1");
                    if (item != null)
                    {
                        colSubjectId.Add(item.subjectId);
                        colActivityLabel.Add(item.activityLabel);
                        colTimestamp.Add(item.timestamp);
                        //_cols["x_Axis"].Add(item.x_Axis);
                        //_cols["y_Axis"].Add(item.y_Axis);
                        //_cols["z_Axis"].Add(item.z_Axis);
                    }

                    _checkpoint++;

                    // 100_000_000 is the checkpoint threshold
                    if (_checkpoint > 100_000_000)
                    {
                        PersistData();

                        colSubjectId.Clear();
                        colActivityLabel.Clear();
                        colTimestamp.Clear();

                        _checkpoint = 0;
                    }
                }
            }
        }

        private async void PersistData()
        {
            // Use a specific memory pool from which arrays will be allocated (optional)

            var memoryAllocator = new NativeMemoryAllocator(alignment: 64);

            // Build a record batch using the Fluent API

            var recordBatch = new RecordBatch.Builder(memoryAllocator)
                .Append("Subject Id", false, col => col.Int32(array => array.AppendRange(colSubjectId)))
                .Append("Activity Label", false, col => col.String(array => array.AppendRange(colActivityLabel)))
                .Append("Timestamp", false, col => col.Int64(array => array.AppendRange(colTimestamp)))
                //.Append("X Axis", false, col => col.Int32(array => array.AppendRange(result[3])))
                //.Append("Y Axis", false, col => col.Int32(array => array.AppendRange(result[4])))
                //.Append("Z Axis", false, col => col.Int32(array => array.Append(2)))
                .Build();

            //recordBatch.Column(0).Data.

            // Print memory allocation statistics

            Console.WriteLine("Allocations: {0}", memoryAllocator.Statistics.Allocations);
            Console.WriteLine("Allocated: {0} byte(s)", memoryAllocator.Statistics.BytesAllocated);

            // Write record batch to a file
            string time = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            using (var stream = File.OpenWrite(@"c:\temp\data\" + time + ".arrow"))
            //using (var stream = File.OpenWrite($"c:\\temp\\data\\test.arrow"))
            using (var writer = new ArrowFileWriter(stream, recordBatch.Schema))
            {
                await writer.WriteRecordBatchAsync(recordBatch);
                await writer.WriteEndAsync();
            }

            Console.WriteLine("Checkpointing is done!");

        }
    }

}
