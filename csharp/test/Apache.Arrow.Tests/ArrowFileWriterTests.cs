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

using Apache.Arrow.Ipc;
using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowFileWriterTests
    {
        [Fact]
        public void Ctor_LeaveOpenDefault_StreamClosedOnDispose()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var stream = new MemoryStream();
            new ArrowFileWriter(stream, originalBatch.Schema).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenFalse_StreamClosedOnDispose()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var stream = new MemoryStream();
            new ArrowFileWriter(stream, originalBatch.Schema, leaveOpen: false).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenTrue_StreamValidOnDispose()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var stream = new MemoryStream();
            new ArrowFileWriter(stream, originalBatch.Schema, leaveOpen: true).Dispose();
            Assert.Equal(0, stream.Position);
        }

        /// <summary>
        /// Tests that writing an arrow file will always align the Block lengths
        /// to 8 bytes. There are asserts in both the reader and writer which will fail
        /// if this isn't the case.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task WritesFooterAlignedMultipleOf8()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            var stream = new MemoryStream();
            var writer = new ArrowFileWriter(
                stream,
                originalBatch.Schema,
                leaveOpen: true,
                // use WriteLegacyIpcFormat, which only uses a 4-byte length prefix
                // which causes the length prefix to not be 8-byte aligned by default
                new IpcOptions() { WriteLegacyIpcFormat = true });

            writer.WriteRecordBatch(originalBatch);
            writer.WriteEnd();

            stream.Position = 0;

            await ValidateRecordBatchFile(stream, originalBatch);
        }

        /// <summary>
        /// Tests that writing an arrow file will always align the Block lengths
        /// to 8 bytes. There are asserts in both the reader and writer which will fail
        /// if this isn't the case.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task WritesFooterAlignedMultipleOf8Async()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            var stream = new MemoryStream();
            var writer = new ArrowFileWriter(
                stream,
                originalBatch.Schema,
                leaveOpen: true,
                // use WriteLegacyIpcFormat, which only uses a 4-byte length prefix
                // which causes the length prefix to not be 8-byte aligned by default
                new IpcOptions() { WriteLegacyIpcFormat = true });

            await writer.WriteRecordBatchAsync(originalBatch);
            await writer.WriteEndAsync();

            stream.Position = 0;

            await ValidateRecordBatchFile(stream, originalBatch);
        }

        private async Task ValidateRecordBatchFile(Stream stream, RecordBatch recordBatch)
        {
            var reader = new ArrowFileReader(stream);
            int count = await reader.RecordBatchCountAsync();
            Assert.Equal(1, count);
            RecordBatch readBatch = await reader.ReadRecordBatchAsync(0);
            ArrowReaderVerifier.CompareBatches(recordBatch, readBatch);
        }

        /// <summary>
        /// Tests that writing an arrow file with no RecordBatches produces the correct
        /// file.
        /// </summary>
        [Fact]
        public async Task WritesEmptyFile()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 1);

            var stream = new MemoryStream();
            var writer = new ArrowFileWriter(stream, originalBatch.Schema);

            writer.WriteStart();
            writer.WriteEnd();

            stream.Position = 0;

            var reader = new ArrowFileReader(stream);
            int count = await reader.RecordBatchCountAsync();
            Assert.Equal(0, count);
            RecordBatch readBatch = reader.ReadNextRecordBatch();
            Assert.Null(readBatch);
            SchemaComparer.Compare(originalBatch.Schema, reader.Schema);
        }

        /// <summary>
        /// Tests that writing an arrow file with no RecordBatches produces the correct
        /// file when using WriteStartAsync and WriteEndAsync.
        /// </summary>
        [Fact]
        public async Task WritesEmptyFileAsync()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 1);

            var stream = new MemoryStream();
            var writer = new ArrowFileWriter(stream, originalBatch.Schema);

            await writer.WriteStartAsync();
            await writer.WriteEndAsync();

            stream.Position = 0;

            var reader = new ArrowFileReader(stream);
            int count = await reader.RecordBatchCountAsync();
            Assert.Equal(0, count);
            RecordBatch readBatch = reader.ReadNextRecordBatch();
            Assert.Null(readBatch);
            SchemaComparer.Compare(originalBatch.Schema, reader.Schema);
        }
    }
}
