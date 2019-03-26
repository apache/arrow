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
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowStreamReaderTests
    {
        [Fact]
        public void Ctor_LeaveOpenDefault_StreamClosedOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowStreamReader(stream).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenFalse_StreamClosedOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowStreamReader(stream, leaveOpen: false).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenTrue_StreamValidOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowStreamReader(stream, leaveOpen: true).Dispose();
            Assert.Equal(0, stream.Position);
        }

        [Fact]
        public async Task ReadRecordBatch_Memory()
        {
            await TestReaderFromMemory((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task ReadRecordBatchAsync_Memory()
        {
            await TestReaderFromMemory(ArrowReaderVerifier.VerifyReaderAsync);
        }

        private static async Task TestReaderFromMemory(Func<ArrowStreamReader, RecordBatch, Task> verificationFunc)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            byte[] buffer;
            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                buffer = stream.GetBuffer();
            }

            ArrowStreamReader reader = new ArrowStreamReader(buffer);
            await verificationFunc(reader, originalBatch);
        }

        [Fact]
        public async Task ReadRecordBatch_Stream()
        {
            await TestReaderFromStream((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task ReadRecordBatchAsync_Stream()
        {
            await TestReaderFromStream(ArrowReaderVerifier.VerifyReaderAsync);
        }

        private static async Task TestReaderFromStream(Func<ArrowStreamReader, RecordBatch, Task> verificationFunc)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);

                stream.Position = 0;

                ArrowStreamReader reader = new ArrowStreamReader(stream);
                await verificationFunc(reader, originalBatch);
            }
        }

        [Fact]
        public async Task ReadRecordBatch_PartialReadStream()
        {
            await TestReaderFromPartialReadStream((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task ReadRecordBatchAsync_PartialReadStream()
        {
            await TestReaderFromPartialReadStream(ArrowReaderVerifier.VerifyReaderAsync);
        }

        /// <summary>
        /// Verifies that the stream reader reads multiple times when a stream
        /// only returns a subset of the data from each Read.
        /// </summary>
        private static async Task TestReaderFromPartialReadStream(Func<ArrowStreamReader, RecordBatch, Task> verificationFunc)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            using (PartialReadStream stream = new PartialReadStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);

                stream.Position = 0;

                ArrowStreamReader reader = new ArrowStreamReader(stream);
                await verificationFunc(reader, originalBatch);
            }
        }

        /// <summary>
        /// A stream class that only returns a part of the data at a time.
        /// </summary>
        private class PartialReadStream : MemoryStream
        {
            // by default return 20 bytes at a time
            public int PartialReadLength { get; set; } = 20;

            public override int Read(Span<byte> destination)
            {
                if (destination.Length > PartialReadLength)
                {
                    destination = destination.Slice(0, PartialReadLength);
                }

                return base.Read(destination);
            }

            public override ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                if (destination.Length > PartialReadLength)
                {
                    destination = destination.Slice(0, PartialReadLength);
                }

                return base.ReadAsync(destination, cancellationToken);
            }
        }
    }
}

