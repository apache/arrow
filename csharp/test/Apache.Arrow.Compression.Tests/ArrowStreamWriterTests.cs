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
using Apache.Arrow.Ipc;
using System.IO;
using Apache.Arrow.Tests;
using Xunit;

namespace Apache.Arrow.Compression.Tests
{
    public class ArrowStreamWriterTests
    {
        [Theory]
        [InlineData(CompressionCodecType.Zstd)]
        [InlineData(CompressionCodecType.Lz4Frame)]
        public void CanWriteCompressedIpcStream(CompressionCodecType codec)
        {
            var batch = TestData.CreateSampleRecordBatch(length: 100);
            var codecFactory = new CompressionCodecFactory();
            var options = new IpcOptions
            {
                CompressionCodecFactory = codecFactory,
                CompressionCodec = codec,
            };
            TestRoundTripRecordBatches(new [] {batch}, options, codecFactory);
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
    }
}

