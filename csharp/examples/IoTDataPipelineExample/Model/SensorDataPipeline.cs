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
using System.Threading.Channels;
using System.Threading;
using Apache.Arrow.Types;

namespace IoTPipelineExample
{
    public class SensorDataPipeline
    {
        private int _size;
        private readonly int _totalInputs;
        private readonly Channel<SensorData> _channel;
        ChannelWriter<SensorData> _writer;
        ChannelReader<SensorData> _reader;

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

        public SensorDataPipeline(int totalInputs)
        {
            _totalInputs = totalInputs;
            _channel = Channel.CreateBounded<SensorData>(1_000_000);
            _writer = _channel.Writer;
            _reader = _channel.Reader;
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
                    // approximately 9_000 unique subjects/sensors
                    SubjectId = rand.Next(1_000, 10_000),
                    ActivityLabel = label,
                    Timestamp = DateTimeOffset.UtcNow,
                    X_Axis = rand.NextDouble(),
                    Y_Axis = rand.NextDouble(),
                    Z_Axis = rand.NextDouble(),
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
                    if (item != null)
                    {
                        int subjectId = (int)item.SubjectId;
                        if (!_colSubjectIdBuilderDict.ContainsKey(subjectId))
                        {
                            _colSubjectIdBuilderDict.Add(subjectId, new Int32Array.Builder());
                            _colActivityLabelBuilderDict.Add(subjectId, new StringArray.Builder());
                            _colTimestampBuilderDict.Add(subjectId, new TimestampArray.Builder());
                            _colXAxisBuilderDict.Add(subjectId, new DoubleArray.Builder());
                            _colYAxisBuilderDict.Add(subjectId, new DoubleArray.Builder());
                            _colZAxisBuilderDict.Add(subjectId, new DoubleArray.Builder());
                        }
                        _colSubjectIdBuilderDict[subjectId].Append((int)item.SubjectId);
                        _colActivityLabelBuilderDict[subjectId].Append(item.ActivityLabel);
                        _colTimestampBuilderDict[subjectId].Append((DateTimeOffset)item.Timestamp);
                        _colXAxisBuilderDict[subjectId].Append((double)item.X_Axis);
                        _colYAxisBuilderDict[subjectId].Append((double)item.Y_Axis);
                        _colZAxisBuilderDict[subjectId].Append((double)item.Z_Axis);
                    }
                }
            }
            Console.WriteLine($"Read from channel task finished!");
        }

        public async Task<string> PersistData()
        {
            List<RecordBatch> recordBatches = new List<RecordBatch>();

            string currentPath = Directory.GetCurrentDirectory();
            string arrowDataPath = Path.Combine(currentPath, "arrow");
            if (!Directory.Exists(arrowDataPath))
                Directory.CreateDirectory(arrowDataPath);


            foreach (var key in _colSubjectIdBuilderDict.Keys)
            {
                var subjectId = key;

                Schema.Builder schemaBuilder = new Schema.Builder();

                schemaBuilder.Field(new Field("SubjectId", Int32Type.Default, nullable: false));
                schemaBuilder.Field(new Field("ActivityLabel", StringType.Default, nullable: false));
                schemaBuilder.Field(new Field("Timestamp", TimestampType.Default, nullable: false));
                schemaBuilder.Field(new Field("XAxis", DoubleType.Default, nullable: false));
                schemaBuilder.Field(new Field("YAxis", DoubleType.Default, nullable: false));
                schemaBuilder.Field(new Field("ZAxis", DoubleType.Default, nullable: false));

                schemaBuilder.Metadata("SubjectId", subjectId.ToString());
                Schema schema = schemaBuilder.Build();

                int fieldCount = schema.Fields.Count;
                List<IArrowArray> arrays = new List<IArrowArray>(fieldCount);

                arrays.Add(_colSubjectIdBuilderDict[subjectId].Build());
                arrays.Add(_colActivityLabelBuilderDict[subjectId].Build());
                arrays.Add(_colTimestampBuilderDict[subjectId].Build());
                arrays.Add(_colXAxisBuilderDict[subjectId].Build());
                arrays.Add(_colYAxisBuilderDict[subjectId].Build());
                arrays.Add(_colZAxisBuilderDict[subjectId].Build());

                var recordBatch = new RecordBatch(schema, arrays, _colSubjectIdBuilderDict[subjectId].Length);

                recordBatches.Add(recordBatch);
            }

            Schema fileSchema = recordBatches[0].Schema;

            using (var stream = File.OpenWrite(arrowDataPath + @"\iotbigdata_" + DateTime.UtcNow.ToString("yyyy-MM-dd--HH-mm-ss") + ".arrow"))
            using (var writer = new ArrowFileWriter(stream, fileSchema))
            {
                foreach (RecordBatch recordBatch in recordBatches)
                {
                    await writer.WriteRecordBatchAsync(recordBatch);
                }
                await writer.WriteEndAsync();
            }

            return arrowDataPath;
        }
    }
}
