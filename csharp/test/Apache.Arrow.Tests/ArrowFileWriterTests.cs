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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Types;
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

        [Theory]
        [InlineData(0, 45)]
        [InlineData(3, 45)]
        [InlineData(16, 45)]
        [InlineData(10, 0)]
        public async Task WriteSlicedArrays(int sliceOffset, int sliceLength)
        {
            var originalBatch = TestData.CreateSampleRecordBatch(length: 100);
            var slicedArrays = originalBatch.Arrays
                .Select(array => ArrowArrayFactory.Slice(array, sliceOffset, sliceLength))
                .ToList();
            var slicedBatch = new RecordBatch(originalBatch.Schema, slicedArrays, sliceLength);

            var stream = new MemoryStream();
            var writer = new ArrowFileWriter(stream, slicedBatch.Schema, leaveOpen: true);

            await writer.WriteRecordBatchAsync(slicedBatch);
            await writer.WriteEndAsync();

            stream.Position = 0;

            // Disable strict comparison because we don't expect buffers to match exactly
            // due to writing slices of buffers, and instead need to compare array values
            await ValidateRecordBatchFile(stream, slicedBatch, strictCompare: false);
        }

        [Theory]
        [InlineData(0, 100)]
        [InlineData(0, 50)]
        [InlineData(50, 50)]
        [InlineData(25, 50)]
        public async Task WriteListViewDataWithUnorderedOffsets(int sliceOffset, int sliceLength)
        {
            // A list-view array doesn't require that offsets are ordered,
            // so verify that we can round trip a list-view array with out-of-order offsets.
            const int length = 100;
            var random = new Random();

            var randomizedIndices = Enumerable.Range(0, length).ToArray();
            Shuffle(randomizedIndices, random);

            var offsetsBuilder = new ArrowBuffer.Builder<int>().Resize(length);
            var sizesBuilder = new ArrowBuffer.Builder<int>().Resize(length);
            var validityBuilder = new ArrowBuffer.BitmapBuilder().Reserve(length);

            var valuesLength = 0;
            for (int i = 0; i < length; ++i)
            {
                var index = randomizedIndices[i];
                var listLength = random.Next(0, 10);
                offsetsBuilder.Span[index] = valuesLength;
                sizesBuilder.Span[index] = listLength;
                valuesLength += listLength;

                validityBuilder.Append(random.NextDouble() < 0.9);
            }

            var valuesBuilder = new Int64Array.Builder().Reserve(valuesLength);
            for (int i = 0; i < valuesLength; ++i)
            {
                valuesBuilder.Append(random.Next(0, 1_000));
            }

            var type = new ListViewType(new Int64Type());
            var offsets = offsetsBuilder.Build();
            var sizes = sizesBuilder.Build();
            var values = valuesBuilder.Build();
            var nullCount = validityBuilder.UnsetBitCount;
            var validityBuffer = validityBuilder.Build();

            IArrowArray listViewArray = new ListViewArray(
                type, length, offsets, sizes, values, validityBuffer, nullCount);

            if (sliceOffset != 0 || sliceLength != length)
            {
                listViewArray = ArrowArrayFactory.Slice(listViewArray, sliceOffset, sliceLength);
            }

            var recordBatch = new RecordBatch.Builder().Append("x", true, listViewArray).Build();

            var stream = new MemoryStream();
            var writer = new ArrowFileWriter(stream, recordBatch.Schema, leaveOpen: true);

            await writer.WriteRecordBatchAsync(recordBatch);
            await writer.WriteEndAsync();

            stream.Position = 0;

            await ValidateRecordBatchFile(stream, recordBatch, strictCompare: false);
        }

        private async Task ValidateRecordBatchFile(Stream stream, RecordBatch recordBatch, bool strictCompare = true)
        {
            var reader = new ArrowFileReader(stream);
            int count = await reader.RecordBatchCountAsync();
            Assert.Equal(1, count);
            RecordBatch readBatch = await reader.ReadRecordBatchAsync(0);
            ArrowReaderVerifier.CompareBatches(recordBatch, readBatch, strictCompare);
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

        [Fact]
        public async Task WriteBinaryArrayWithEmptyOffsets()
        {
            // Empty binary arrays generated by the C# builder have a single offset,
            // but some implementations may produce an empty offsets buffer.

            var array = new BinaryArray(
                new BinaryType(),
                length: 0,
                valueOffsetsBuffer: ArrowBuffer.Empty,
                dataBuffer: ArrowBuffer.Empty,
                nullBitmapBuffer: ArrowBuffer.Empty,
                nullCount: 0);

            var recordBatch = new RecordBatch.Builder().Append("x", true, array).Build();

            var stream = new MemoryStream();
            var writer = new ArrowFileWriter(stream, recordBatch.Schema, leaveOpen: true);

            await writer.WriteRecordBatchAsync(recordBatch);
            await writer.WriteEndAsync();

            stream.Position = 0;

            await ValidateRecordBatchFile(stream, recordBatch, strictCompare: false);
        }

        [Fact]
        public async Task WriteListArrayWithEmptyOffsets()
        {
            var values = new Int32Array.Builder().Build();
            var array = new ListArray(
                new ListType(new Int32Type()),
                length: 0,
                valueOffsetsBuffer: ArrowBuffer.Empty,
                values: values,
                nullBitmapBuffer: ArrowBuffer.Empty,
                nullCount: 0);

            var recordBatch = new RecordBatch.Builder().Append("x", true, array).Build();

            var stream = new MemoryStream();
            var writer = new ArrowFileWriter(stream, recordBatch.Schema, leaveOpen: true);

            await writer.WriteRecordBatchAsync(recordBatch);
            await writer.WriteEndAsync();

            stream.Position = 0;

            await ValidateRecordBatchFile(stream, recordBatch, strictCompare: false);
        }

        private static void Shuffle(int[] values, Random random)
        {
            var length = values.Length;
            for (int i = 0; i < length - 1; ++i)
            {
                var j = random.Next(i, length);
                var tmp = values[i];
                values[i] = values[j];
                values[j] = tmp;
            }
        }
    }
}
