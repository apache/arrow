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
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using System.Threading.Channels;
using System.Threading;

namespace IoTPipelineExample
{
    public class SampleDataPipeline
    {
        private int _size;
        private readonly int _totalInputs;
        private readonly int _queueCapacity;
        private readonly Channel<SensorData> _channel;
        ChannelWriter<SensorData> _writer;
        ChannelReader<SensorData> _reader;

        private readonly Int32Array.Builder _colSubjectIdBuilder;
        private readonly StringArray.Builder _colActivityLabelBuilder;
        private readonly TimestampArray.Builder _colTimestampBuilder;
        private readonly DoubleArray.Builder _colXAxisBuilder;
        private readonly DoubleArray.Builder _colYAxisBuilder;
        private readonly DoubleArray.Builder _colZAxisBuilder;

        private readonly Dictionary<int, Int32Array.Builder> _colSubjectIdBuilderDict;
        private readonly Dictionary<int, StringArray.Builder> _colActivityLabelBuilderDict;
        private readonly Dictionary<int, TimestampArray.Builder> _colTimestampBuilderDict;
        private readonly Dictionary<int, DoubleArray.Builder> _colXAxisBuilderDict;
        private readonly Dictionary<int, DoubleArray.Builder> _colYAxisBuilderDict;
        private readonly Dictionary<int, DoubleArray.Builder> _colZAxisBuilderDict;

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

        public SampleDataPipeline(int totalInputs, int queueCapacity)
        {
            _totalInputs = totalInputs;
            _queueCapacity = queueCapacity;
            _channel = Channel.CreateBounded<SensorData>(_queueCapacity);
            _writer = _channel.Writer;
            _reader = _channel.Reader;

            _colSubjectIdBuilder = new Int32Array.Builder();
            _colActivityLabelBuilder = new StringArray.Builder();
            _colTimestampBuilder = new TimestampArray.Builder();
            _colXAxisBuilder = new DoubleArray.Builder();
            _colYAxisBuilder = new DoubleArray.Builder();
            _colZAxisBuilder = new DoubleArray.Builder();

            _colSubjectIdBuilderDict = new Dictionary<int, Int32Array.Builder>();
            _colActivityLabelBuilderDict = new Dictionary<int, StringArray.Builder>();
            _colTimestampBuilderDict = new Dictionary<int, TimestampArray.Builder>();
            _colXAxisBuilderDict = new Dictionary<int, DoubleArray.Builder>();
            _colYAxisBuilderDict = new Dictionary<int, DoubleArray.Builder>();
            _colZAxisBuilderDict = new Dictionary<int, DoubleArray.Builder>();
        }

        public async Task WriteToChannel(int taskNumber)
        {
            Random rand = new Random();
            List<string> keyList = new List<string>(activityLabel.Keys);
            int count = keyList.Count;
            DateTime now = DateTime.Now;
            long unixTime = ((DateTimeOffset)now).ToUnixTimeSeconds();
            var basis = DateTimeOffset.UtcNow;

            Console.WriteLine($"Write to channel task {taskNumber} started!");
            while (await _writer.WaitToWriteAsync())
            {
                string randomKey = keyList[rand.Next(count)];
                string label = activityLabel[randomKey];

                // generate random missing values
                if (rand.Next(10_000) == 9_999)
                {
                    label = null;
                }

                var item = new SensorData
                {
                    subjectId = rand.Next(1_000, 10_000),
                    activityLabel = label,
                    timestamp = basis.AddMilliseconds(1),
                    x_Axis = rand.NextDouble(),
                    y_Axis = rand.NextDouble(),
                    z_Axis = rand.NextDouble(),
                };

                if (_writer.TryWrite(item))
                {
                    Interlocked.Increment(ref _size);

                    if (_size >= _totalInputs)
                    {
                        _writer.TryComplete();
                    }
                }
            }

            Console.WriteLine($"Write to channel task {taskNumber} finished!");
        }

        public async Task ReadFromChannel()
        {

            Console.WriteLine($"Read from channel task started!");
            while (await _reader.WaitToReadAsync())
            {
                while (_reader.TryRead(out SensorData item))
                {
                    int builderId = (int)item.subjectId;

                    if (item != null)
                    {
                        if (!_colSubjectIdBuilderDict.ContainsKey(builderId))
                        {
                            _colSubjectIdBuilderDict.Add(builderId, new Int32Array.Builder());
                            _colActivityLabelBuilderDict.Add(builderId, new StringArray.Builder());
                            _colTimestampBuilderDict.Add(builderId, new TimestampArray.Builder());
                            _colXAxisBuilderDict.Add(builderId, new DoubleArray.Builder());
                            _colYAxisBuilderDict.Add(builderId, new DoubleArray.Builder());
                            _colZAxisBuilderDict.Add(builderId, new DoubleArray.Builder());
                        }
                        _colSubjectIdBuilderDict[builderId].Append((int)item.subjectId);
                        _colActivityLabelBuilderDict[builderId].Append(item.activityLabel);
                        _colTimestampBuilderDict[builderId].Append((DateTimeOffset)item.timestamp);
                        _colXAxisBuilderDict[builderId].Append((double)item.x_Axis);
                        _colYAxisBuilderDict[builderId].Append((double)item.y_Axis);
                        _colZAxisBuilderDict[builderId].Append((double)item.z_Axis);
                    }
                }
            }
            Console.WriteLine($"Read from channel task finished!");
        }

        public async Task<string> PersistData()
        {
            int partitionNumber = 0;
            string currentPath = Directory.GetCurrentDirectory();
            string arrowDataPath = Path.Combine(currentPath, "arrow");
            if (!Directory.Exists(arrowDataPath))
                Directory.CreateDirectory(arrowDataPath);

            using (var stream = File.OpenWrite(arrowDataPath + @"\iotbigdata_" + partitionNumber + ".arrow"))
            using (var writer = new ArrowFileWriter(stream, recordBatch.Schema))
            {
                var memoryAllocator = new NativeMemoryAllocator(alignment: 64);

                foreach (var keyValuePair in _colSubjectIdBuilderDict)
                {
                    var builderId = keyValuePair.Key;
                    var subjectIdBuilder = keyValuePair.Value;

                    var recordBatch = new RecordBatch.Builder(memoryAllocator)
                    .Append("SubjectId", false, subjectIdBuilder.Build())
                    .Append("ActivityLabel", false, _colActivityLabelBuilderDict[builderId].Build())
                    .Append("Timestamp", false, _colTimestampBuilderDict[builderId].Build())
                    .Append("XAxis", false, _colXAxisBuilderDict[builderId].Build())
                    .Append("YAxis", false, _colYAxisBuilderDict[builderId].Build())
                    .Append("ZAxis", false, _colZAxisBuilderDict[builderId].Build())
                    .Build();

                    await writer.WriteRecordBatchAsync(recordBatch);

                }
                await writer.WriteEndAsync();
            }

            return arrowDataPath;
        }
    }
}
