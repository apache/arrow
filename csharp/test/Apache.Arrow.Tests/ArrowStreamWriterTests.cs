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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
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

        [Theory]
        [InlineData(true, 32153)]
        [InlineData(false, 32154)]
        public void CanWriteToNetworkStream(bool createDictionaryArray, int port)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

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

        [Theory]
        [InlineData(true, 32155)]
        [InlineData(false, 32156)]
        public async Task CanWriteToNetworkStreamAsync(bool createDictionaryArray, int port)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void WriteEmptyBatch(bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 0, createDictionaryArray: createDictionaryArray);

            TestRoundTripRecordBatch(originalBatch);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task WriteEmptyBatchAsync(bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 0, createDictionaryArray: createDictionaryArray);

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
                .Append("NullColumn", true, new NullArray(10))
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

        private static void TestRoundTripRecordBatches(List<RecordBatch> originalBatches, IpcOptions options = null)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, originalBatches[0].Schema, leaveOpen: true, options))
                {
                    foreach (RecordBatch originalBatch in originalBatches)
                    {
                        writer.WriteRecordBatch(originalBatch);
                    }
                    writer.WriteEnd();
                }

                stream.Position = 0;

                using (var reader = new ArrowStreamReader(stream))
                {
                    foreach (RecordBatch originalBatch in originalBatches)
                    {
                        RecordBatch newBatch = reader.ReadNextRecordBatch();
                        ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                    }
                }
            }
        }

        private static async Task TestRoundTripRecordBatchesAsync(List<RecordBatch> originalBatches, IpcOptions options = null)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                using (var writer = new ArrowStreamWriter(stream, originalBatches[0].Schema, leaveOpen: true, options))
                {
                    foreach (RecordBatch originalBatch in originalBatches)
                    {
                        await writer.WriteRecordBatchAsync(originalBatch);
                    }
                    await writer.WriteEndAsync();
                }

                stream.Position = 0;

                using (var reader = new ArrowStreamReader(stream))
                {
                    foreach (RecordBatch originalBatch in originalBatches)
                    {
                        RecordBatch newBatch = reader.ReadNextRecordBatch();
                        ArrowReaderVerifier.CompareBatches(originalBatch, newBatch);
                    }
                }
            }
        }

        private static void TestRoundTripRecordBatch(RecordBatch originalBatch, IpcOptions options = null)
        {
            TestRoundTripRecordBatches(new List<RecordBatch> { originalBatch }, options);
        }

        private static async Task TestRoundTripRecordBatchAsync(RecordBatch originalBatch, IpcOptions options = null)
        {
            await TestRoundTripRecordBatchesAsync(new List<RecordBatch> { originalBatch }, options);
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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void LegacyIpcFormatRoundTrips(bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);
            TestRoundTripRecordBatch(originalBatch, new IpcOptions() { WriteLegacyIpcFormat = true });
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LegacyIpcFormatRoundTripsAsync(bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);
            await TestRoundTripRecordBatchAsync(originalBatch, new IpcOptions() { WriteLegacyIpcFormat = true });
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public void WriteLegacyIpcFormat(bool writeLegacyIpcFormat, bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);
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
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task WriteLegacyIpcFormatAsync(bool writeLegacyIpcFormat, bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);
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

        [Fact]
        public void WritesMetadataCorrectly()
        {
            Schema.Builder schemaBuilder = new Schema.Builder()
                .Metadata("index", "1, 2, 3, 4, 5")
                .Metadata("reverseIndex", "5, 4, 3, 2, 1")
                .Field(f => f
                    .Name("IntCol")
                    .DataType(UInt32Type.Default)
                    .Metadata("custom1", "false")
                    .Metadata("custom2", "true"))
                .Field(f => f
                    .Name("StringCol")
                    .DataType(StringType.Default)
                    .Metadata("custom2", "false")
                    .Metadata("custom3", "4"))
                .Field(f => f
                    .Name("StructCol")
                    .DataType(new StructType(new[] {
                        new Field("Inner1", FloatType.Default, nullable: false),
                        new Field("Inner2", DoubleType.Default, nullable: true, new Dictionary<string, string>() { { "customInner", "1" }, { "customInner2", "3" } })
                    }))
                    .Metadata("custom4", "6.4")
                    .Metadata("custom1", "true"));

            var schema = schemaBuilder.Build();
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(schema, length: 10);

            TestRoundTripRecordBatch(originalBatch);
        }

        [Fact]
        public async Task WriteMultipleDictionaryArraysAsync()
        {
            List<RecordBatch> originalRecordBatches = CreateMultipleDictionaryArraysTestData();
            await TestRoundTripRecordBatchesAsync(originalRecordBatches);
        }

        [Fact]
        public void WriteMultipleDictionaryArrays()
        {
            List<RecordBatch> originalRecordBatches = CreateMultipleDictionaryArraysTestData();
            TestRoundTripRecordBatches(originalRecordBatches);
        }

        private List<RecordBatch> CreateMultipleDictionaryArraysTestData()
        {
            var dictionaryData = new List<string> { "a", "b", "c" };
            int length = dictionaryData.Count;

            var schemaForSimpleCase = new Schema(new List<Field> {
                new Field("int8", Int8Type.Default, true),
                new Field("uint8", UInt8Type.Default, true),
                new Field("int16", Int16Type.Default, true),
                new Field("uint16", UInt16Type.Default, true),
                new Field("int32", Int32Type.Default, true),
                new Field("uint32", UInt32Type.Default, true),
                new Field("int64", Int64Type.Default, true),
                new Field("uint64", UInt64Type.Default, true)
            }, null);

            StringArray dictionary = new StringArray.Builder().AppendRange(dictionaryData).Build();
            IEnumerable<IArrowArray> indicesArraysForSimpleCase = TestData.CreateArrays(schemaForSimpleCase, length);

            var fields = new List<Field>(capacity: length + 1);
            var testTargetArrays = new List<IArrowArray>(capacity: length + 1);

            foreach (IArrowArray indices in indicesArraysForSimpleCase)
            {
                var dictionaryArray = new DictionaryArray(
                    new DictionaryType(indices.Data.DataType, StringType.Default, false),
                    indices, dictionary);
                testTargetArrays.Add(dictionaryArray);
                fields.Add(new Field($"dictionaryField_{indices.Data.DataType.Name}", dictionaryArray.Data.DataType, false));
            }

            (Field dictionaryTypeListArrayField, ListArray dictionaryTypeListArray) = CreateDictionaryTypeListArrayTestData(dictionary);

            fields.Add(dictionaryTypeListArrayField);
            testTargetArrays.Add(dictionaryTypeListArray);

            (Field listTypeDictionaryArrayField, DictionaryArray listTypeDictionaryArray) = CreateListTypeDictionaryArrayTestData(dictionaryData);

            fields.Add(listTypeDictionaryArrayField);
            testTargetArrays.Add(listTypeDictionaryArray);

            var schema = new Schema(fields, null);

            return new List<RecordBatch> {
                new RecordBatch(schema, testTargetArrays, length),
                new RecordBatch(schema, testTargetArrays, length),
            };
        }

        private Tuple<Field, ListArray> CreateDictionaryTypeListArrayTestData(StringArray dictionary)
        {
            Int32Array indiceArray = new Int32Array.Builder().AppendRange(Enumerable.Range(0, dictionary.Length)).Build();

            //DictionaryArray has no Builder for now, so creating ListArray directly.
            var dictionaryType = new DictionaryType(Int32Type.Default, StringType.Default, false);
            var dictionaryArray = new DictionaryArray(dictionaryType, indiceArray, dictionary);

            var valueOffsetsBufferBuilder = new ArrowBuffer.Builder<int>();
            var validityBufferBuilder = new ArrowBuffer.BitmapBuilder();

            foreach (int i in Enumerable.Range(0, dictionary.Length + 1))
            {
                valueOffsetsBufferBuilder.Append(i);
                validityBufferBuilder.Append(true);
            }

            var dictionaryField = new Field("dictionaryField_list", dictionaryType, false);
            var listType = new ListType(dictionaryField);
            var listArray = new ListArray(listType, valueOffsetsBufferBuilder.Length - 1, valueOffsetsBufferBuilder.Build(), dictionaryArray, valueOffsetsBufferBuilder.Build());

            return Tuple.Create(new Field($"listField_{listType.ValueDataType.Name}", listType, false), listArray);
        }

        private Tuple<Field, DictionaryArray> CreateListTypeDictionaryArrayTestData(List<string> dictionaryDataBase)
        {
            var listBuilder = new ListArray.Builder(StringType.Default);
            var valueBuilder = listBuilder.ValueBuilder as StringArray.Builder;

            foreach(string data in dictionaryDataBase) {
                listBuilder.Append();
                valueBuilder.Append(data);
            }

            ListArray dictionary = listBuilder.Build();
            Int32Array indiceArray = new Int32Array.Builder().AppendRange(Enumerable.Range(0, dictionary.Length)).Build();
            var dictionaryArrayType = new DictionaryType(Int32Type.Default, dictionary.Data.DataType, false);
            var dictionaryArray = new DictionaryArray(dictionaryArrayType, indiceArray, dictionary);

            return Tuple.Create(new Field($"dictionaryField_{dictionaryArray.Data.DataType.Name}", dictionaryArrayType, false), dictionaryArray);
        }

        /// <summary>
        /// Tests that writing an arrow stream with no RecordBatches produces the correct result.
        /// </summary>
        [Fact]
        public void WritesEmptyFile()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 1);

            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, originalBatch.Schema);

            writer.WriteStart();
            writer.WriteEnd();

            stream.Position = 0;

            var reader = new ArrowStreamReader(stream);
            RecordBatch readBatch = reader.ReadNextRecordBatch();
            Assert.Null(readBatch);
            SchemaComparer.Compare(originalBatch.Schema, reader.Schema);
        }

        /// <summary>
        /// Tests that writing an arrow stream with no RecordBatches produces the correct
        /// result when using WriteStartAsync and WriteEndAsync.
        /// </summary>
        [Fact]
        public async Task WritesEmptyFileAsync()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 1);

            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, originalBatch.Schema);

            await writer.WriteStartAsync();
            await writer.WriteEndAsync();

            stream.Position = 0;

            var reader = new ArrowStreamReader(stream);
            RecordBatch readBatch = reader.ReadNextRecordBatch();
            Assert.Null(readBatch);
            SchemaComparer.Compare(originalBatch.Schema, reader.Schema);
        }
    }
}
