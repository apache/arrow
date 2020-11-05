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
using Apache.Arrow.Types;
using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowStreamWriterTests
    {
        [Fact]
        public void Ctor_LeaveOpenDefault_StreamClosedOnDispose()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var stream = new MemoryStream();
            new ArrowStreamWriter(stream, originalBatch.Schema).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenFalse_StreamClosedOnDispose()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var stream = new MemoryStream();
            new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: false).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenTrue_StreamValidOnDispose()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var stream = new MemoryStream();
            new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true).Dispose();
            Assert.Equal(0, stream.Position);
        }

        [Fact]
        public void CanWriteToNetworkStream()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            const int port = 32153;
            TcpListener listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();

            using (TcpClient sender = new TcpClient())
            {
                sender.Connect(IPAddress.Loopback, port);
                NetworkStream stream = sender.GetStream();

                using (var writer = new ArrowStreamWriter(stream, originalBatch.Schema))
                {
                    writer.WriteRecordBatch(originalBatch);
                    writer.WriteEnd();

                    stream.Flush();
                }
            }

            using (TcpClient receiver = listener.AcceptTcpClient())
            {
                NetworkStream stream = receiver.GetStream();
                using (var reader = new ArrowStreamReader(stream))
                {
                    RecordBatch newBatch = reader.ReadNextRecordBatch();
                    ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                }
            }
        }

        [Fact]
        public async Task CanWriteToNetworkStreamAsync()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            const int port = 32154;
            TcpListener listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();

            using (TcpClient sender = new TcpClient())
            {
                sender.Connect(IPAddress.Loopback, port);
                NetworkStream stream = sender.GetStream();

                using (var writer = new ArrowStreamWriter(stream, originalBatch.Schema))
                {
                    await writer.WriteRecordBatchAsync(originalBatch);
                    await writer.WriteEndAsync();

                    stream.Flush();
                }
            }

            using (TcpClient receiver = listener.AcceptTcpClient())
            {
                NetworkStream stream = receiver.GetStream();
                using (var reader = new ArrowStreamReader(stream))
                {
                    RecordBatch newBatch = reader.ReadNextRecordBatch();
                    ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                }
            }
        }

        [Fact]
        public void WriteEmptyBatch()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 0);

            TestRoundTripRecordBatch(originalBatch);
        }

        [Fact]
        public async Task WriteEmptyBatchAsync()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 0);

            await TestRoundTripRecordBatchAsync(originalBatch);
        }

        [Fact]
        public void WriteBatchWithNulls()
        {
            RecordBatch originalBatch = new RecordBatch.Builder()
                .Append("Column1", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(0, 10))))
                .Append("Column2", true, new Int32Array(
                    valueBuffer: new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, 10)).Build(),
                    nullBitmapBuffer: new ArrowBuffer.Builder<byte>().Append(0xfd).Append(0xff).Build(),
                    length: 10,
                    nullCount: 2,
                    offset: 0))
                .Append("Column3", true, new Int32Array(
                    valueBuffer: new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, 10)).Build(),
                    nullBitmapBuffer: new ArrowBuffer.Builder<byte>().Append(0x00).Append(0x00).Build(),
                    length: 10,
                    nullCount: 10,
                    offset: 0))
                .Append("NullableBooleanColumn", true, new BooleanArray(
                    valueBuffer: new ArrowBuffer.Builder<byte>().Append(0xfd).Append(0xff).Build(),
                    nullBitmapBuffer: new ArrowBuffer.Builder<byte>().Append(0xed).Append(0xff).Build(),
                    length: 10,
                    nullCount: 3,
                    offset: 0))
                .Build();

            TestRoundTripRecordBatch(originalBatch);
        }

        [Fact]
        public async Task WriteBatchWithNullsAsync()
        {
            RecordBatch originalBatch = new RecordBatch.Builder()
                .Append("Column1", false, col => col.Int32(array => array.AppendRange(Enumerable.Range(0, 10))))
                .Append("Column2", true, new Int32Array(
                    valueBuffer: new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, 10)).Build(),
                    nullBitmapBuffer: new ArrowBuffer.Builder<byte>().Append(0xfd).Append(0xff).Build(),
                    length: 10,
                    nullCount: 2,
                    offset: 0))
                .Append("Column3", true, new Int32Array(
                    valueBuffer: new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, 10)).Build(),
                    nullBitmapBuffer: new ArrowBuffer.Builder<byte>().Append(0x00).Append(0x00).Build(),
                    length: 10,
                    nullCount: 10,
                    offset: 0))
                .Append("NullableBooleanColumn", true, new BooleanArray(
                    valueBuffer: new ArrowBuffer.Builder<byte>().Append(0xfd).Append(0xff).Build(),
                    nullBitmapBuffer: new ArrowBuffer.Builder<byte>().Append(0xed).Append(0xff).Build(),
                    length: 10,
                    nullCount: 3,
                    offset: 0))
                .Build();

            await TestRoundTripRecordBatchAsync(originalBatch);
        }

        private static void TestRoundTripRecordBatch(RecordBatch originalBatch, IpcOptions options = null)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true, options))
                {
                    writer.WriteRecordBatch(originalBatch);
                    writer.WriteEnd();
                }

                stream.Position = 0;

                using (var reader = new ArrowStreamReader(stream))
                {
                    RecordBatch newBatch = reader.ReadNextRecordBatch();
                    ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                }
            }
        }


        private static async Task TestRoundTripRecordBatchAsync(RecordBatch originalBatch, IpcOptions options = null)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true, options))
                {
                    await writer.WriteRecordBatchAsync(originalBatch);
                    await writer.WriteEndAsync();
                }

                stream.Position = 0;

                using (var reader = new ArrowStreamReader(stream))
                {
                    RecordBatch newBatch = reader.ReadNextRecordBatch();
                    ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                }
            }
        }

        [Fact]
        public void WriteBatchWithCorrectPadding()
        {
            byte value1 = 0x04;
            byte value2 = 0x14;
            var batch = new RecordBatch(
                new Schema.Builder()
                    .Field(f => f.Name("age").DataType(Int32Type.Default))
                    .Field(f => f.Name("characterCount").DataType(Int32Type.Default))
                    .Build(),
                new IArrowArray[]
                {
                    new Int32Array(
                        new ArrowBuffer(new byte[] { value1, value1, 0x00, 0x00 }),
                        ArrowBuffer.Empty,
                        length: 1,
                        nullCount: 0,
                        offset: 0),
                    new Int32Array(
                        new ArrowBuffer(new byte[] { value2, value2, 0x00, 0x00 }),
                        ArrowBuffer.Empty,
                        length: 1,
                        nullCount: 0,
                        offset: 0)
                },
                length: 1);

            TestRoundTripRecordBatch(batch);

            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen: true))
                {
                    writer.WriteRecordBatch(batch);
                    writer.WriteEnd();
                }

                byte[] writtenBytes = stream.ToArray();

                // ensure that the data buffers at the end are 8-byte aligned
                Assert.Equal(value1, writtenBytes[writtenBytes.Length - 24]);
                Assert.Equal(value1, writtenBytes[writtenBytes.Length - 23]);
                for (int i = 22; i > 16; i--)
                {
                    Assert.Equal(0, writtenBytes[writtenBytes.Length - i]);
                }

                Assert.Equal(value2, writtenBytes[writtenBytes.Length - 16]);
                Assert.Equal(value2, writtenBytes[writtenBytes.Length - 15]);
                for (int i = 14; i > 8; i--)
                {
                    Assert.Equal(0, writtenBytes[writtenBytes.Length - i]);
                }

                // verify the EOS is written correctly
                for (int i = 8; i > 4; i--)
                {
                    Assert.Equal(0xFF, writtenBytes[writtenBytes.Length - i]);
                }
                for (int i = 4; i > 0; i--)
                {
                    Assert.Equal(0x00, writtenBytes[writtenBytes.Length - i]);
                }
            }
        }

        [Fact]
        public async Task WriteBatchWithCorrectPaddingAsync()
        {
            byte value1 = 0x04;
            byte value2 = 0x14;
            var batch = new RecordBatch(
                new Schema.Builder()
                    .Field(f => f.Name("age").DataType(Int32Type.Default))
                    .Field(f => f.Name("characterCount").DataType(Int32Type.Default))
                    .Build(),
                new IArrowArray[]
                {
                    new Int32Array(
                        new ArrowBuffer(new byte[] { value1, value1, 0x00, 0x00 }),
                        ArrowBuffer.Empty,
                        length: 1,
                        nullCount: 0,
                        offset: 0),
                    new Int32Array(
                        new ArrowBuffer(new byte[] { value2, value2, 0x00, 0x00 }),
                        ArrowBuffer.Empty,
                        length: 1,
                        nullCount: 0,
                        offset: 0)
                },
                length: 1);

            await TestRoundTripRecordBatchAsync(batch);

            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen: true))
                {
                    await writer.WriteRecordBatchAsync(batch);
                    await writer.WriteEndAsync();
                }

                byte[] writtenBytes = stream.ToArray();

                // ensure that the data buffers at the end are 8-byte aligned
                Assert.Equal(value1, writtenBytes[writtenBytes.Length - 24]);
                Assert.Equal(value1, writtenBytes[writtenBytes.Length - 23]);
                for (int i = 22; i > 16; i--)
                {
                    Assert.Equal(0, writtenBytes[writtenBytes.Length - i]);
                }

                Assert.Equal(value2, writtenBytes[writtenBytes.Length - 16]);
                Assert.Equal(value2, writtenBytes[writtenBytes.Length - 15]);
                for (int i = 14; i > 8; i--)
                {
                    Assert.Equal(0, writtenBytes[writtenBytes.Length - i]);
                }

                // verify the EOS is written correctly
                for (int i = 8; i > 4; i--)
                {
                    Assert.Equal(0xFF, writtenBytes[writtenBytes.Length - i]);
                }
                for (int i = 4; i > 0; i--)
                {
                    Assert.Equal(0x00, writtenBytes[writtenBytes.Length - i]);
                }
            }
        }

        [Fact]
        public void LegacyIpcFormatRoundTrips()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            TestRoundTripRecordBatch(originalBatch, new IpcOptions() { WriteLegacyIpcFormat = true });
        }


        [Fact]
        public async Task LegacyIpcFormatRoundTripsAsync()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            await TestRoundTripRecordBatchAsync(originalBatch, new IpcOptions() { WriteLegacyIpcFormat = true });
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteLegacyIpcFormat(bool writeLegacyIpcFormat)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var options = new IpcOptions() { WriteLegacyIpcFormat = writeLegacyIpcFormat };

            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true, options))
                {
                    writer.WriteRecordBatch(originalBatch);
                    writer.WriteEnd();
                }

                stream.Position = 0;

                // ensure the continuation is written correctly
                byte[] buffer = stream.ToArray();
                int messageLength = BinaryPrimitives.ReadInt32LittleEndian(buffer);
                int endOfBuffer1 = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(buffer.Length - 8));
                int endOfBuffer2 = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(buffer.Length - 4));
                if (writeLegacyIpcFormat)
                {
                    // the legacy IPC format doesn't have a continuation token at the start
                    Assert.NotEqual(-1, messageLength);
                    Assert.NotEqual(-1, endOfBuffer1);
                }
                else
                {
                    // the latest IPC format has a continuation token at the start
                    Assert.Equal(-1, messageLength);
                    Assert.Equal(-1, endOfBuffer1);
                }

                Assert.Equal(0, endOfBuffer2);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task WriteLegacyIpcFormatAsync(bool writeLegacyIpcFormat)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var options = new IpcOptions() { WriteLegacyIpcFormat = writeLegacyIpcFormat };

            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true, options))
                {
                    await writer.WriteRecordBatchAsync(originalBatch);
                    await writer.WriteEndAsync();
                }

                stream.Position = 0;

                // ensure the continuation is written correctly
                byte[] buffer = stream.ToArray();
                int messageLength = BinaryPrimitives.ReadInt32LittleEndian(buffer);
                int endOfBuffer1 = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(buffer.Length - 8));
                int endOfBuffer2 = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(buffer.Length - 4));
                if (writeLegacyIpcFormat)
                {
                    // the legacy IPC format doesn't have a continuation token at the start
                    Assert.NotEqual(-1, messageLength);
                    Assert.NotEqual(-1, endOfBuffer1);
                }
                else
                {
                    // the latest IPC format has a continuation token at the start
                    Assert.Equal(-1, messageLength);
                    Assert.Equal(-1, endOfBuffer1);
                }

                Assert.Equal(0, endOfBuffer2);
            }
        }
    }
}
