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
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowFileReaderTests
    {
        [Fact]
        public void Ctor_LeaveOpenDefault_StreamClosedOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowFileReader(stream).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenFalse_StreamClosedOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowFileReader(stream, leaveOpen: false).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenTrue_StreamValidOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowFileReader(stream, leaveOpen: true).Dispose();
            Assert.Equal(0, stream.Position);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Ctor_MemoryPool_AllocatesFromPool(bool shouldLeaveOpen)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowFileWriter writer = new ArrowFileWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();
                stream.Position = 0;

                var memoryPool = new TestMemoryAllocator();
                ArrowFileReader reader = new ArrowFileReader(stream, memoryPool, leaveOpen: shouldLeaveOpen);
                reader.ReadNextRecordBatch();

                Assert.Equal(1, memoryPool.Statistics.Allocations);
                Assert.True(memoryPool.Statistics.BytesAllocated > 0);

                reader.Dispose();

                if (shouldLeaveOpen)
                {
                    Assert.True(stream.Position > 0);
                }
                else
                {
                    Assert.Throws<ObjectDisposedException>(() => stream.Position);
                }
            }
        }

        [Fact]
        public async Task TestReadNextRecordBatch()
        {
            await TestReadRecordBatchHelper((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task TestReadNextRecordBatchAsync()
        {
            await TestReadRecordBatchHelper(ArrowReaderVerifier.VerifyReaderAsync);
        }

        [Fact]
        public async Task TestReadRecordBatchAsync()
        {
            await TestReadRecordBatchHelper(async (reader, originalBatch) =>
            {
                RecordBatch readBatch = await reader.ReadRecordBatchAsync(0);
                ArrowReaderVerifier.CompareBatches(originalBatch, readBatch);

                // You should be able to read the same record batch again
                RecordBatch readBatch2 = await reader.ReadRecordBatchAsync(0);
                ArrowReaderVerifier.CompareBatches(originalBatch, readBatch2);
            });
        }

        private static async Task TestReadRecordBatchHelper(
            Func<ArrowFileReader, RecordBatch, Task> verificationFunc)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowFileWriter writer = new ArrowFileWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();
                stream.Position = 0;

                ArrowFileReader reader = new ArrowFileReader(stream);
                await verificationFunc(reader, originalBatch);
            }
        }

        [Fact]
        public async Task TestReadMultipleRecordBatchAsync()
        {
            RecordBatch originalBatch1 = TestData.CreateSampleRecordBatch(length: 100);
            RecordBatch originalBatch2 = TestData.CreateSampleRecordBatch(length: 50);

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowFileWriter writer = new ArrowFileWriter(stream, originalBatch1.Schema);
                await writer.WriteRecordBatchAsync(originalBatch1);
                await writer.WriteRecordBatchAsync(originalBatch2);
                await writer.WriteEndAsync();
                stream.Position = 0;

                ArrowFileReader reader = new ArrowFileReader(stream);
                RecordBatch readBatch1 = await reader.ReadRecordBatchAsync(0);
                ArrowReaderVerifier.CompareBatches(originalBatch1, readBatch1);

                RecordBatch readBatch2 = await reader.ReadRecordBatchAsync(1);
                ArrowReaderVerifier.CompareBatches(originalBatch2, readBatch2);

                // now read the first again, for random access
                RecordBatch readBatch3 = await reader.ReadRecordBatchAsync(0);
                ArrowReaderVerifier.CompareBatches(originalBatch1, readBatch3);
            }
        }

        [Fact]
        public void TestRecordBatchBasics()
        {
            RecordBatch recordBatch = TestData.CreateSampleRecordBatch(length: 1);
            Assert.Throws<ArgumentOutOfRangeException>(() => new RecordBatch(recordBatch.Schema, recordBatch.Arrays, -1));

            var col1 = recordBatch.Column(0);
            var col2 = recordBatch.Column("list0");
            ArrowReaderVerifier.CompareArrays(col1, col2);

            recordBatch.Dispose();
        }

        [Theory]
        [InlineData("ipc_lz4_compression.arrow")]
        [InlineData("ipc_zstd_compression.arrow")]
        public void CanReadCompressedIpcFile(string fileName)
        {
            var assembly = Assembly.GetExecutingAssembly();
            using var stream = assembly.GetManifestResourceStream($"Apache.Arrow.Tests.Resources.{fileName}");
            var codecFactory = new Compression.CompressionCodecFactory();
            using var reader = new ArrowFileReader(stream, codecFactory);

            var batch = reader.ReadNextRecordBatch();

            var intArray = (Int32Array) batch.Column("integers");
            var floatArray = (FloatArray) batch.Column("floats");

            const int numRows = 100;
            Assert.Equal(numRows, intArray.Length);
            Assert.Equal(numRows, floatArray.Length);

            for (var i = 0; i < numRows; ++i)
            {
                Assert.Equal(i, intArray.GetValue(i));
                Assert.True(Math.Abs(floatArray.GetValue(i).Value - 0.1f * i) < 1.0e-6);
            }
        }

        [Fact]
        public void ErrorReadingCompressedFileWithoutCodecFactory()
        {
            var assembly = Assembly.GetExecutingAssembly();
            using var stream = assembly.GetManifestResourceStream("Apache.Arrow.Tests.Resources.ipc_lz4_compression.arrow");
            using var reader = new ArrowFileReader(stream);

            var exception = Assert.Throws<Exception>(() => reader.ReadNextRecordBatch());
            Assert.Contains("no ICompressionCodecFactory has been configured", exception.Message);
        }
    }
}
