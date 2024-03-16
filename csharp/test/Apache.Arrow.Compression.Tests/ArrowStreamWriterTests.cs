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
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Tests;
using K4os.Compression.LZ4;
using Xunit;

namespace Apache.Arrow.Compression.Tests
{
    public class ArrowStreamWriterTests
    {
        [Fact]
        public void ThrowsWhenNoCompressionFactoryProvided()
        {
            var batch = TestData.CreateSampleRecordBatch(length: 100);
            var options = new IpcOptions
            {
                CompressionCodec = CompressionCodecType.Zstd,
            };

            using var stream = new MemoryStream();
            var exception = Assert.Throws<ArgumentException>(
                () => new ArrowStreamWriter(stream, batch.Schema, leaveOpen: true, options));

            Assert.Contains("A CompressionCodecFactory must be provided", exception.Message);
        }

        [Theory]
        [InlineData(CompressionCodecType.Zstd, null)]
        [InlineData(CompressionCodecType.Zstd, 2)]
        [InlineData(CompressionCodecType.Lz4Frame, null)]
        [InlineData(CompressionCodecType.Lz4Frame, (int)LZ4Level.L03_HC)]
        public void CanWriteCompressedIpcStream(CompressionCodecType codec, int? compressionLevel)
        {
            var batch = TestData.CreateSampleRecordBatch(length: 100);
            var codecFactory = new CompressionCodecFactory();
            var options = new IpcOptions
            {
                CompressionCodecFactory = codecFactory,
                CompressionCodec = codec,
                CompressionLevel = compressionLevel,
            };
            TestRoundTripRecordBatches(new [] {batch}, options, codecFactory);
        }

        [Theory]
        [InlineData(CompressionCodecType.Zstd)]
        [InlineData(CompressionCodecType.Lz4Frame)]
        public async Task CanWriteCompressedIpcStreamAsync(CompressionCodecType codec)
        {
            var batch = TestData.CreateSampleRecordBatch(length: 100);
            var codecFactory = new CompressionCodecFactory();
            var options = new IpcOptions
            {
                CompressionCodecFactory = codecFactory,
                CompressionCodec = codec,
            };
            await TestRoundTripRecordBatchesAsync(new [] {batch}, options, codecFactory);
        }

        [Fact]
        public void CanWriteEmptyBatches()
        {
            var batch = TestData.CreateSampleRecordBatch(length: 0);
            var codecFactory = new CompressionCodecFactory();
            var options = new IpcOptions
            {
                CompressionCodecFactory = codecFactory,
                CompressionCodec = CompressionCodecType.Lz4Frame,
            };
            TestRoundTripRecordBatches(new [] {batch}, options, codecFactory);
        }

        [Theory]
        [InlineData(CompressionCodecType.Zstd)]
        [InlineData(CompressionCodecType.Lz4Frame)]
        public void ThrowsForInvalidCompressionLevel(CompressionCodecType codec)
        {
            var batch = TestData.CreateSampleRecordBatch(length: 100);
            var codecFactory = new CompressionCodecFactory();
            var options = new IpcOptions
            {
                CompressionCodecFactory = codecFactory,
                CompressionCodec = codec,
                CompressionLevel = 12345,
            };

            using var stream = new MemoryStream();

            Assert.Throws<ArgumentException>(() =>
            {
                using var writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen: false, options);
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
            });
        }

        private static void TestRoundTripRecordBatches(
            IReadOnlyList<RecordBatch> originalBatches, IpcOptions options, ICompressionCodecFactory codecFactory)
        {
            using var stream = new MemoryStream();

            using (var writer = new ArrowStreamWriter(stream, originalBatches[0].Schema, leaveOpen: true, options))
            {
                foreach (var originalBatch in originalBatches)
                {
                    writer.WriteRecordBatch(originalBatch);
                }
                writer.WriteEnd();
            }

            // Should throw if trying to read without an ICompressionCodecFactory
            stream.Position = 0;
            var exception = Assert.Throws<Exception>(() =>
            {
                using var reader = new ArrowStreamReader(stream, leaveOpen: true);
                reader.ReadNextRecordBatch();
            });
            Assert.Contains(nameof(ICompressionCodecFactory), exception.Message);

            stream.Position = 0;
            using (var reader = new ArrowStreamReader(stream, codecFactory))
            {
                foreach (var originalBatch in originalBatches)
                {
                    var newBatch = reader.ReadNextRecordBatch();
                    ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                }
            }
        }

        private static async Task TestRoundTripRecordBatchesAsync(
            IReadOnlyList<RecordBatch> originalBatches, IpcOptions options, ICompressionCodecFactory codecFactory)
        {
            using var stream = new MemoryStream();

            using (var writer = new ArrowStreamWriter(stream, originalBatches[0].Schema, leaveOpen: true, options))
            {
                foreach (var originalBatch in originalBatches)
                {
                    await writer.WriteRecordBatchAsync(originalBatch);
                }
                await writer.WriteEndAsync();
            }

            // Should throw if trying to read without an ICompressionCodecFactory
            stream.Position = 0;
            var exception = await Assert.ThrowsAsync<Exception>(async () =>
            {
                using var reader = new ArrowStreamReader(stream, leaveOpen: true);
                await reader.ReadNextRecordBatchAsync();
            });
            Assert.Contains(nameof(ICompressionCodecFactory), exception.Message);

            stream.Position = 0;
            using (var reader = new ArrowStreamReader(stream, codecFactory))
            {
                foreach (var originalBatch in originalBatches)
                {
                    var newBatch = await reader.ReadNextRecordBatchAsync();
                    ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                }
            }
        }
    }
}

