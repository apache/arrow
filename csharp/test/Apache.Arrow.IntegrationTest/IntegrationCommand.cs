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
using Apache.Arrow.Compression;
using Apache.Arrow.Ipc;
using Apache.Arrow.Tests;
using Apache.Arrow.Types;

namespace Apache.Arrow.IntegrationTest
{
    public class IntegrationCommand
    {
        public string Mode { get; set; }
        public FileInfo JsonFileInfo { get; set; }
        public FileInfo ArrowFileInfo { get; set; }

        public IntegrationCommand(string mode, FileInfo jsonFileInfo, FileInfo arrowFileInfo)
        {
            Mode = mode;
            JsonFileInfo = jsonFileInfo;
            ArrowFileInfo = arrowFileInfo;
        }

        public async Task<int> Execute()
        {
            Func<Task<int>> commandDelegate = Mode switch
            {
                "validate" => Validate,
                "json-to-arrow" => JsonToArrow,
                "stream-to-file" => StreamToFile,
                "file-to-stream" => FileToStream,
                "round-trip-json-arrow" => RoundTripJsonArrow,
                _ => () =>
                {
                    Console.WriteLine($"Mode '{Mode}' is not supported.");
                    return Task.FromResult(-1);
                }
            };
            return await commandDelegate();
        }

        private async Task<int> RoundTripJsonArrow()
        {
            int status = await JsonToArrow();
            if (status != 0) { return status; }

            return await Validate();
        }

        private async Task<int> Validate()
        {
            JsonFile jsonFile = await ParseJsonFile();

            var compressionFactory = new CompressionCodecFactory();
            using FileStream arrowFileStream = ArrowFileInfo.OpenRead();
            using ArrowFileReader reader = new ArrowFileReader(arrowFileStream, compressionCodecFactory: compressionFactory);
            int batchCount = await reader.RecordBatchCountAsync();

            if (batchCount != jsonFile.Batches.Count)
            {
                Console.WriteLine($"Incorrect batch count. JsonFile: {jsonFile.Batches.Count}, ArrowFile: {batchCount}");
                return -1;
            }

            Schema jsonFileSchema = jsonFile.GetSchemaAndDictionaries(out Func<DictionaryType, IArrowArray> dictionaries);
            Schema arrowFileSchema = reader.Schema;

            SchemaComparer.Compare(jsonFileSchema, arrowFileSchema);

            for (int i = 0; i < batchCount; i++)
            {
                RecordBatch arrowFileRecordBatch = reader.ReadNextRecordBatch();
                RecordBatch jsonFileRecordBatch = jsonFile.Batches[i].ToArrow(jsonFileSchema, dictionaries);

                ArrowReaderVerifier.CompareBatches(jsonFileRecordBatch, arrowFileRecordBatch, strictCompare: false);
            }

            // ensure there are no more batches in the file
            if (reader.ReadNextRecordBatch() != null)
            {
                Console.WriteLine($"The ArrowFile has more RecordBatches than it should.");
                return -1;
            }

            return 0;
        }

        private async Task<int> JsonToArrow()
        {
            JsonFile jsonFile = await ParseJsonFile();
            Schema schema = jsonFile.GetSchemaAndDictionaries(out Func<DictionaryType, IArrowArray> dictionaries);

            using (FileStream fs = ArrowFileInfo.Create())
            {
                ArrowFileWriter writer = new ArrowFileWriter(fs, schema);
                await writer.WriteStartAsync();

                foreach (var jsonRecordBatch in jsonFile.Batches)
                {
                    RecordBatch batch = jsonRecordBatch.ToArrow(schema, dictionaries);
                    await writer.WriteRecordBatchAsync(batch);
                }
                await writer.WriteEndAsync();
                await fs.FlushAsync();
            }

            return 0;
        }

        private async Task<int> StreamToFile()
        {
            var compressionFactory = new CompressionCodecFactory();
            using ArrowStreamReader reader = new ArrowStreamReader(Console.OpenStandardInput(), compressionCodecFactory: compressionFactory);

            RecordBatch batch = await reader.ReadNextRecordBatchAsync();

            using FileStream fileStream = ArrowFileInfo.OpenWrite();
            using ArrowFileWriter writer = new ArrowFileWriter(fileStream, reader.Schema);
            await writer.WriteStartAsync();

            while (batch != null)
            {
                await writer.WriteRecordBatchAsync(batch);

                batch = await reader.ReadNextRecordBatchAsync();
            }

            await writer.WriteEndAsync();

            return 0;
        }

        private async Task<int> FileToStream()
        {
            using FileStream fileStream = ArrowFileInfo.OpenRead();
            var compressionFactory = new CompressionCodecFactory();
            using ArrowFileReader fileReader = new ArrowFileReader(fileStream, compressionCodecFactory: compressionFactory);

            // read the record batch count to initialize the Schema
            await fileReader.RecordBatchCountAsync();

            using ArrowStreamWriter writer = new ArrowStreamWriter(Console.OpenStandardOutput(), fileReader.Schema);
            await writer.WriteStartAsync();

            RecordBatch batch;
            while ((batch = fileReader.ReadNextRecordBatch()) != null)
            {
                await writer.WriteRecordBatchAsync(batch);
            }

            await writer.WriteEndAsync();

            return 0;
        }

        private async ValueTask<JsonFile> ParseJsonFile()
        {
            return await JsonFile.ParseAsync(JsonFileInfo);
        }
    }
}
