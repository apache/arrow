﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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
using Apache.Arrow.Memory;
using System;
using System.IO;
using System.Reflection;
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

        [Theory]
        [InlineData(true, true, 2)]
        [InlineData(true, false, 1)]
        [InlineData(false, true, 2)]
        [InlineData(false, false, 1)]
        public async Task Ctor_MemoryPool_AllocatesFromPool(bool shouldLeaveOpen, bool createDictionaryArray, int expectedAllocations)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();

                stream.Position = 0;

                var memoryPool = new TestMemoryAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(stream, memoryPool, shouldLeaveOpen);
                reader.ReadNextRecordBatch();

                Assert.Equal(expectedAllocations, memoryPool.Statistics.Allocations);
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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatch_Memory(bool writeEnd)
        {
            await TestReaderFromMemory((reader, originalBatch) =>
            {
                Assert.NotNull(reader.Schema);

                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            }, writeEnd);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatchAsync_Memory(bool writeEnd)
        {
            await TestReaderFromMemory(ArrowReaderVerifier.VerifyReaderAsync, writeEnd);
        }

        private static async Task TestReaderFromMemory(
            Func<ArrowStreamReader, RecordBatch, Task> verificationFunc,
            bool writeEnd)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            byte[] buffer;
            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                if (writeEnd)
                {
                    await writer.WriteEndAsync();
                }
                buffer = stream.GetBuffer();
            }

            ArrowStreamReader reader = new ArrowStreamReader(buffer);
            await verificationFunc(reader, originalBatch);
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ReadRecordBatch_Stream(bool writeEnd, bool createDictionaryArray)
        {
            await TestReaderFromStream((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            }, writeEnd, createDictionaryArray);
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ReadRecordBatchAsync_Stream(bool writeEnd, bool createDictionaryArray)
        {
            await TestReaderFromStream(ArrowReaderVerifier.VerifyReaderAsync, writeEnd, createDictionaryArray);
        }

        private static async Task TestReaderFromStream(
            Func<ArrowStreamReader, RecordBatch, Task> verificationFunc,
            bool writeEnd, bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                if (writeEnd)
                {
                    await writer.WriteEndAsync();
                }

                stream.Position = 0;

                ArrowStreamReader reader = new ArrowStreamReader(stream);
                await verificationFunc(reader, originalBatch);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatch_PartialReadStream(bool createDictionaryArray)
        {
            await TestReaderFromPartialReadStream((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            }, createDictionaryArray);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatchAsync_PartialReadStream(bool createDictionaryArray)
        {
            await TestReaderFromPartialReadStream(ArrowReaderVerifier.VerifyReaderAsync, createDictionaryArray);
        }

        /// <summary>
        /// Verifies that the stream reader reads multiple times when a stream
        /// only returns a subset of the data from each Read.
        /// </summary>
        private static async Task TestReaderFromPartialReadStream(Func<ArrowStreamReader, RecordBatch, Task> verificationFunc, bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

            using (PartialReadStream stream = new PartialReadStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();

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

#if NET5_0_OR_GREATER
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
#else
            public override int Read(byte[] buffer, int offset, int length)
            {
                return base.Read(buffer, offset, Math.Min(length, PartialReadLength));
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
            {
                return base.ReadAsync(buffer, offset, Math.Min(length, PartialReadLength), cancellationToken);
            }
#endif
        }
    }
}

