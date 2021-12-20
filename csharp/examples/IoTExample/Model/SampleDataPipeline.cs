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
using System.Diagnostics;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using System.Threading.Channels;
using System.Threading;
using System.Collections.Concurrent;

namespace IoTPipelineExample
{
    public class SampleDataPipeline
    {
        private int _size;
        private readonly int _inputs;
        private readonly int _capacity;
        private readonly Channel<SensorData> _channel;
        ChannelWriter<SensorData> _writer;
        ChannelReader<SensorData> _reader;

        private readonly ConcurrentBag<int> _colSubjectId;
        private readonly ConcurrentBag<string> _colActivityLabel;
        private readonly ConcurrentBag<long> _colTimestamp;
        private readonly ConcurrentBag<double> _colXAxis;
        private readonly ConcurrentBag<double> _colYAxis;
        private readonly ConcurrentBag<double> _colZAxis;

        private readonly int _threshold = 1_000_00;
        private readonly List<RecordBatch> _recordBatches;
        private readonly MemoryAllocator _memoryAllocator;

        public Dictionary<string, string> activityLabel = new Dictionary<string, string>()
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

        public SampleDataPipeline(int totalSensorData, int queueCapacity)
        {
            _inputs = totalSensorData;
            _capacity = queueCapacity;
            _channel = Channel.CreateBounded<SensorData>(_capacity);
            _writer = _channel.Writer;
            _reader = _channel.Reader;

            _colSubjectId = new ConcurrentBag<int>();
            _colActivityLabel = new ConcurrentBag<string>();
            _colTimestamp = new ConcurrentBag<long>();
            _colXAxis = new ConcurrentBag<double>();
            _colYAxis = new ConcurrentBag<double>();
            _colZAxis = new ConcurrentBag<double>();

            _recordBatches = new List<RecordBatch>();
            _memoryAllocator = new NativeMemoryAllocator(alignment: 64);
        }

        public async Task WriteToChannel()
        {
            Random rand = new Random();
            List<string> keyList = new List<string>(activityLabel.Keys);
            int count = keyList.Count;
            DateTime now = DateTime.Now;
            long unixTime = ((DateTimeOffset)now).ToUnixTimeSeconds();

            while (_size <= _inputs)
            {
                string randomKey = keyList[rand.Next(count)];
                string label = activityLabel[randomKey];

                // generate random missing values
                if (rand.Next(10_000) == 9_999)
                {
                    label = null;
                }

                await _writer.WriteAsync(new SensorData
                {
                    subjectId = rand.Next(1000, 2001),
                    activityLabel = label,
                    timestamp = unixTime++,
                    x_Axis = rand.NextDouble(),
                    y_Axis = rand.NextDouble(),
                    z_Axis = rand.NextDouble(),
                });

                Interlocked.Increment(ref _size);
            }
            _writer.TryComplete();
        }

        public async Task ReadFromChannel()
        {
            while (await _reader.WaitToReadAsync())
            {
                while (_reader.TryRead(out SensorData item))
                {
                    if (item != null && item.subjectId != null)
                    {
                        _colSubjectId.Add((int)item.subjectId);
                        _colActivityLabel.Add(item.activityLabel);
                        _colTimestamp.Add((long)item.timestamp);
                        _colXAxis.Add((double)item.x_Axis);
                        _colYAxis.Add((double)item.y_Axis);
                        _colZAxis.Add((double)item.z_Axis);
                    }

                    if (_colSubjectId.Count == _threshold)
                    {
                        // Build a record batch using the Fluent API
                        var recordBatch = new RecordBatch.Builder(_memoryAllocator)
                            .Append("SubjectId", false, col => col.Int32(array => array.AppendRange(_colSubjectId)))
                            .Append("ActivityLabel", false, col => col.String(array => array.AppendRange(_colActivityLabel)))
                            .Append("Timestamp", false, col => col.Int64(array => array.AppendRange(_colTimestamp)))
                            .Append("XAxis", false, col => col.Double(array => array.AppendRange(_colXAxis)))
                            .Append("YAxis", false, col => col.Double(array => array.AppendRange(_colYAxis)))
                            .Append("ZAxis", false, col => col.Double(array => array.AppendRange(_colZAxis)))
                            .Build();

                        _recordBatches.Add(recordBatch);

                        _colSubjectId.Clear();
                        _colActivityLabel.Clear();
                        _colTimestamp.Clear();
                        _colXAxis.Clear();
                        _colYAxis.Clear();
                        _colZAxis.Clear();
                    }
                }
            }
        }

        public async Task<string> PersistData(string path)
        {
            await _reader.Completion;

            if (_colSubjectId.Count > 0)
            {
                // Build a final record batch using the Fluent API
                var recordBatch = new RecordBatch.Builder(_memoryAllocator)
                    .Append("SubjectId", false, col => col.Int32(array => array.AppendRange(_colSubjectId)))
                    .Append("ActivityLabel", false, col => col.String(array => array.AppendRange(_colActivityLabel)))
                    .Append("Timestamp", false, col => col.Int64(array => array.AppendRange(_colTimestamp)))
                    .Append("XAxis", false, col => col.Double(array => array.AppendRange(_colXAxis)))
                    .Append("YAxis", false, col => col.Double(array => array.AppendRange(_colYAxis)))
                    .Append("ZAxis", false, col => col.Double(array => array.AppendRange(_colZAxis)))
                    .Build();

                _recordBatches.Add(recordBatch);
            }

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            Schema schema = _recordBatches[0].Schema;
            // Write record batches to an arrow file
            using (var stream = File.OpenWrite(path))
            using (var writer = new ArrowFileWriter(stream, schema))
            {
                foreach (RecordBatch recordBatch in _recordBatches)
                {
                    try
                    {
                        await writer.WriteRecordBatchAsync(recordBatch);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
                await writer.WriteEndAsync();
            }

            stopwatch.Stop();
            TimeSpan ts = stopwatch.Elapsed;
            Console.WriteLine($"Checkpointing is done, time used is: {ts.Minutes} min {ts.Seconds} sec");

            return path;
        }
    }

}
