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

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Google.FlatBuffers;

namespace Apache.Arrow.Ipc
{
    public class ArrowStreamWriter : IDisposable
    {
        private class ArrowRecordBatchFlatBufferBuilder :
            IArrowArrayVisitor<Int8Array>,
            IArrowArrayVisitor<Int16Array>,
            IArrowArrayVisitor<Int32Array>,
            IArrowArrayVisitor<Int64Array>,
            IArrowArrayVisitor<UInt8Array>,
            IArrowArrayVisitor<UInt16Array>,
            IArrowArrayVisitor<UInt32Array>,
            IArrowArrayVisitor<UInt64Array>,
#if NET5_0_OR_GREATER
            IArrowArrayVisitor<HalfFloatArray>,
#endif
            IArrowArrayVisitor<FloatArray>,
            IArrowArrayVisitor<DoubleArray>,
            IArrowArrayVisitor<BooleanArray>,
            IArrowArrayVisitor<TimestampArray>,
            IArrowArrayVisitor<Date32Array>,
            IArrowArrayVisitor<Date64Array>,
            IArrowArrayVisitor<Time32Array>,
            IArrowArrayVisitor<Time64Array>,
            IArrowArrayVisitor<DurationArray>,
            IArrowArrayVisitor<YearMonthIntervalArray>,
            IArrowArrayVisitor<DayTimeIntervalArray>,
            IArrowArrayVisitor<MonthDayNanosecondIntervalArray>,
            IArrowArrayVisitor<ListArray>,
            IArrowArrayVisitor<ListViewArray>,
            IArrowArrayVisitor<FixedSizeListArray>,
            IArrowArrayVisitor<StringArray>,
            IArrowArrayVisitor<StringViewArray>,
            IArrowArrayVisitor<BinaryArray>,
            IArrowArrayVisitor<BinaryViewArray>,
            IArrowArrayVisitor<FixedSizeBinaryArray>,
            IArrowArrayVisitor<StructArray>,
            IArrowArrayVisitor<UnionArray>,
            IArrowArrayVisitor<Decimal128Array>,
            IArrowArrayVisitor<Decimal256Array>,
            IArrowArrayVisitor<DictionaryArray>,
            IArrowArrayVisitor<NullArray>
        {
            public readonly struct FieldNode
            {
                public readonly int Length;
                public readonly int NullCount;

                public FieldNode(int length, int nullCount)
                {
                    Length = length;
                    NullCount = nullCount;
                }
            }

            public readonly struct Buffer
            {
                public readonly ReadOnlyMemory<byte> DataBuffer;
                public readonly int Offset;

                public Buffer(ReadOnlyMemory<byte> buffer, int offset)
                {
                    DataBuffer = buffer;
                    Offset = offset;
                }
            }

            private readonly List<FieldNode> _fieldNodes;
            private readonly List<Buffer> _buffers;
            private readonly ICompressionCodec _compressionCodec;
            private readonly MemoryAllocator _allocator;
            private readonly MemoryStream _compressionStream;

            public IReadOnlyList<FieldNode> FieldNodes => _fieldNodes;
            public IReadOnlyList<Buffer> Buffers => _buffers;

            public List<long> VariadicCounts { get; private set; } 
            public int TotalLength { get; private set; }

            public ArrowRecordBatchFlatBufferBuilder(
                ICompressionCodec compressionCodec, MemoryAllocator allocator, MemoryStream compressionStream)
            {
                _compressionCodec = compressionCodec;
                _compressionStream = compressionStream;
                _allocator = allocator;
                _fieldNodes = new List<FieldNode>();
                _buffers = new List<Buffer>();
                TotalLength = 0;
            }

            public void VisitArray(IArrowArray array)
            {
                _fieldNodes.Add(new FieldNode(array.Length, array.NullCount));

                array.Accept(this);
            }

            public void Visit(Int8Array array) => VisitPrimitiveArray(array);
            public void Visit(Int16Array array) => VisitPrimitiveArray(array);
            public void Visit(Int32Array array) => VisitPrimitiveArray(array);
            public void Visit(Int64Array array) => VisitPrimitiveArray(array);
            public void Visit(UInt8Array array) => VisitPrimitiveArray(array);
            public void Visit(UInt16Array array) => VisitPrimitiveArray(array);
            public void Visit(UInt32Array array) => VisitPrimitiveArray(array);
            public void Visit(UInt64Array array) => VisitPrimitiveArray(array);
#if NET5_0_OR_GREATER
            public void Visit(HalfFloatArray array) => VisitPrimitiveArray(array);
#endif
            public void Visit(FloatArray array) => VisitPrimitiveArray(array);
            public void Visit(DoubleArray array) => VisitPrimitiveArray(array);
            public void Visit(TimestampArray array) => VisitPrimitiveArray(array);
            public void Visit(Date32Array array) => VisitPrimitiveArray(array);
            public void Visit(Date64Array array) => VisitPrimitiveArray(array);
            public void Visit(Time32Array array) => VisitPrimitiveArray(array);
            public void Visit(Time64Array array) => VisitPrimitiveArray(array);
            public void Visit(DurationArray array) => VisitPrimitiveArray(array);
            public void Visit(YearMonthIntervalArray array) => VisitPrimitiveArray(array);
            public void Visit(DayTimeIntervalArray array) => VisitPrimitiveArray(array);
            public void Visit(MonthDayNanosecondIntervalArray array) => VisitPrimitiveArray(array);

            private void VisitPrimitiveArray<T>(PrimitiveArray<T> array)
                where T : struct
            {
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));
                _buffers.Add(CreateSlicedBuffer<T>(array.ValueBuffer, array.Offset, array.Length));
            }

            public void Visit(BooleanArray array)
            {
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));
                _buffers.Add(CreateBitmapBuffer(array.ValueBuffer, array.Offset, array.Length));
            }

            public void Visit(ListArray array)
            {
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));
                _buffers.Add(CreateBuffer(GetZeroBasedValueOffsets(array.ValueOffsetsBuffer, array.Offset, array.Length)));

                int valuesOffset = array.ValueOffsets[0];
                int valuesLength = array.ValueOffsets[array.Length] - valuesOffset;

                var values = array.Values;
                if (valuesOffset > 0 || valuesLength < values.Length)
                {
                    values = ArrowArrayFactory.Slice(values, valuesOffset, valuesLength);
                }

                VisitArray(values);
            }

            public void Visit(ListViewArray array)
            {
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));
                _buffers.Add(CreateSlicedBuffer<int>(array.ValueOffsetsBuffer, array.Offset, array.Length));
                _buffers.Add(CreateSlicedBuffer<int>(array.SizesBuffer, array.Offset, array.Length));

                VisitArray(array.Values);
            }

            public void Visit(FixedSizeListArray array)
            {
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));

                var listSize = ((FixedSizeListType)array.Data.DataType).ListSize;
                var valuesSlice =
                    ArrowArrayFactory.Slice(array.Values, array.Offset * listSize, array.Length * listSize);

                VisitArray(valuesSlice);
            }

            public void Visit(StringArray array) => Visit(array as BinaryArray);

            public void Visit(StringViewArray array) => Visit(array as BinaryViewArray);

            public void Visit(BinaryArray array)
            {
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));
                _buffers.Add(CreateBuffer(GetZeroBasedValueOffsets(array.ValueOffsetsBuffer, array.Offset, array.Length)));

                int valuesOffset = array.ValueOffsets[0];
                int valuesLength = array.ValueOffsets[array.Length] - valuesOffset;

                _buffers.Add(CreateSlicedBuffer<byte>(array.ValueBuffer, valuesOffset, valuesLength));
            }

            public void Visit(BinaryViewArray array)
            {
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));
                _buffers.Add(CreateSlicedBuffer<Scalars.BinaryView>(array.ViewsBuffer, array.Offset, array.Length));
                for (int i = 0; i < array.DataBufferCount; i++)
                {
                    _buffers.Add(CreateBuffer(array.DataBuffer(i)));
                }
                VariadicCounts = VariadicCounts ?? new List<long>();
                VariadicCounts.Add(array.DataBufferCount);
            }

            public void Visit(FixedSizeBinaryArray array)
            {
                var itemSize = ((FixedSizeBinaryType)array.Data.DataType).ByteWidth;
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));
                _buffers.Add(CreateSlicedBuffer(array.ValueBuffer, itemSize, array.Offset, array.Length));
            }

            public void Visit(Decimal128Array array) => Visit(array as FixedSizeBinaryArray);

            public void Visit(Decimal256Array array) => Visit(array as FixedSizeBinaryArray);

            public void Visit(StructArray array)
            {
                _buffers.Add(CreateBitmapBuffer(array.NullBitmapBuffer, array.Offset, array.Length));

                for (int i = 0; i < array.Fields.Count; i++)
                {
                    // Fields property accessor handles slicing field arrays if required
                    VisitArray(array.Fields[i]);
                }
            }

            public void Visit(UnionArray array)
            {
                _buffers.Add(CreateSlicedBuffer<byte>(array.TypeBuffer, array.Offset, array.Length));

                ArrowBuffer? offsets = (array as DenseUnionArray)?.ValueOffsetBuffer;
                if (offsets != null)
                {
                    _buffers.Add(CreateSlicedBuffer<int>(offsets.Value, array.Offset, array.Length));
                }

                for (int i = 0; i < array.Fields.Count; i++)
                {
                    // Fields property accessor handles slicing field arrays for sparse union arrays if required
                    VisitArray(array.Fields[i]);
                }
            }

            public void Visit(DictionaryArray array)
            {
                // Dictionary is serialized separately in Dictionary serialization.
                // We are only interested in indices at this context.

                array.Indices.Accept(this);
            }

            public void Visit(NullArray array)
            {
                // There are no buffers for a NullArray
            }

            private ArrowBuffer GetZeroBasedValueOffsets(ArrowBuffer valueOffsetsBuffer, int arrayOffset, int arrayLength)
            {
                var requiredBytes = CalculatePaddedBufferLength(sizeof(int) * (arrayLength + 1));

                if (arrayOffset != 0)
                {
                    // Array has been sliced, so we need to shift and adjust the offsets
                    var originalOffsets = valueOffsetsBuffer.Span.CastTo<int>().Slice(arrayOffset, arrayLength + 1);
                    var firstOffset = arrayLength > 0 ? originalOffsets[0] : 0;

                    var newValueOffsetsBuffer = _allocator.Allocate(requiredBytes);
                    var newValueOffsets = newValueOffsetsBuffer.Memory.Span.CastTo<int>();

                    for (int i = 0; i < arrayLength + 1; ++i)
                    {
                        newValueOffsets[i] = originalOffsets[i] - firstOffset;
                    }

                    return new ArrowBuffer(newValueOffsetsBuffer);
                }
                else if (valueOffsetsBuffer.Length > requiredBytes)
                {
                    // Array may have been sliced but the offset is zero,
                    // so we can truncate the existing offsets
                    return new ArrowBuffer(valueOffsetsBuffer.Memory.Slice(0, requiredBytes));
                }
                else
                {
                    // Use the full buffer
                    return valueOffsetsBuffer;
                }
            }

            private Buffer CreateBitmapBuffer(ArrowBuffer buffer, int offset, int length)
            {
                if (buffer.IsEmpty)
                {
                    return CreateBuffer(buffer.Memory);
                }

                var paddedLength = CalculatePaddedBufferLength(BitUtility.ByteCount(length));
                if (offset % 8 == 0)
                {
                    var byteOffset = offset / 8;
                    var sliceLength = Math.Min(paddedLength, buffer.Length - byteOffset);

                    return CreateBuffer(buffer.Memory.Slice(byteOffset, sliceLength));
                }
                else
                {
                    // Need to copy bitmap so the first bit is aligned with the first byte
                    var memoryOwner = _allocator.Allocate(paddedLength);
                    var outputSpan = memoryOwner.Memory.Span;
                    var inputSpan = buffer.Span;
                    for (var i = 0; i < length; ++i)
                    {
                        BitUtility.SetBit(outputSpan, i, BitUtility.GetBit(inputSpan, offset + i));
                    }

                    return CreateBuffer(memoryOwner.Memory);
                }
            }

            private Buffer CreateSlicedBuffer<T>(ArrowBuffer buffer, int offset, int length)
                where T : struct
            {
                return CreateSlicedBuffer(buffer, Unsafe.SizeOf<T>(), offset, length);
            }

            private Buffer CreateSlicedBuffer(ArrowBuffer buffer, int itemSize, int offset, int length)
            {
                var byteLength = length * itemSize;
                var paddedLength = CalculatePaddedBufferLength(byteLength);
                if (offset != 0 || paddedLength < buffer.Length)
                {
                    var byteOffset = offset * itemSize;
                    var sliceLength = Math.Min(paddedLength, buffer.Length - byteOffset);
                    return CreateBuffer(buffer.Memory.Slice(byteOffset, sliceLength));
                }

                return CreateBuffer(buffer.Memory);
            }

            private Buffer CreateBuffer(ArrowBuffer buffer)
            {
                return CreateBuffer(buffer.Memory);
            }

            private Buffer CreateBuffer(ReadOnlyMemory<byte> buffer)
            {
                int offset = TotalLength;
                const int UncompressedLengthSize = 8;

                ReadOnlyMemory<byte> bufferToWrite;
                if (_compressionCodec == null)
                {
                    bufferToWrite = buffer;
                }
                else if (buffer.Length == 0)
                {
                    // Write zero length and skip compression
                    var uncompressedLengthBytes = _allocator.Allocate(UncompressedLengthSize);
                    BinaryPrimitives.WriteInt64LittleEndian(uncompressedLengthBytes.Memory.Span, 0);
                    bufferToWrite = uncompressedLengthBytes.Memory;
                }
                else
                {
                    // See format/Message.fbs, and the BUFFER BodyCompressionMethod for documentation on how
                    // compressed buffers are stored.
                    _compressionStream.Seek(0, SeekOrigin.Begin);
                    _compressionStream.SetLength(0);
                    _compressionCodec.Compress(buffer, _compressionStream);
                    if (_compressionStream.Length < buffer.Length)
                    {
                        var newBuffer = _allocator.Allocate((int) _compressionStream.Length + UncompressedLengthSize);
                        BinaryPrimitives.WriteInt64LittleEndian(newBuffer.Memory.Span, buffer.Length);
                        _compressionStream.Seek(0, SeekOrigin.Begin);
                        _compressionStream.ReadFullBuffer(newBuffer.Memory.Slice(UncompressedLengthSize));
                        bufferToWrite = newBuffer.Memory;
                    }
                    else
                    {
                        // If the compressed buffer is larger than the uncompressed buffer, use the uncompressed
                        // buffer instead, and indicate this by setting the uncompressed length to -1
                        var newBuffer = _allocator.Allocate(buffer.Length + UncompressedLengthSize);
                        BinaryPrimitives.WriteInt64LittleEndian(newBuffer.Memory.Span, -1);
                        buffer.CopyTo(newBuffer.Memory.Slice(UncompressedLengthSize));
                        bufferToWrite = newBuffer.Memory;
                    }
                }

                int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(bufferToWrite.Length));
                TotalLength += paddedLength;

                return new Buffer(bufferToWrite, offset);
            }

            public void Visit(IArrowArray array)
            {
                throw new NotImplementedException();
            }
        }

        protected Stream BaseStream { get; }

        protected ArrayPool<byte> Buffers { get; }

        private protected FlatBufferBuilder Builder { get; }

        protected bool HasWrittenSchema { get; set; }

        private bool HasWrittenDictionaryBatch { get; set; }

        private bool HasWrittenStart { get; set; }

        private bool HasWrittenEnd { get; set; }

        protected Schema Schema { get; }

        private readonly bool _leaveOpen;
        private readonly IpcOptions _options;
        private readonly MemoryAllocator _allocator;
        // Reuse a single memory stream for writing compressed data to, to reduce memory allocations
        private readonly MemoryStream _compressionStream = new MemoryStream();

        private protected const Flatbuf.MetadataVersion CurrentMetadataVersion = Flatbuf.MetadataVersion.V5;

        private static readonly byte[] s_padding = new byte[64];

        private readonly ArrowTypeFlatbufferBuilder _fieldTypeBuilder;

        private DictionaryMemo _dictionaryMemo;
        private DictionaryMemo DictionaryMemo => _dictionaryMemo ??= new DictionaryMemo();

        public ArrowStreamWriter(Stream baseStream, Schema schema)
            : this(baseStream, schema, leaveOpen: false)
        {
        }

        public ArrowStreamWriter(Stream baseStream, Schema schema, bool leaveOpen)
            : this(baseStream, schema, leaveOpen, options: null, allocator: null)
        {
        }

        public ArrowStreamWriter(Stream baseStream, Schema schema, bool leaveOpen, IpcOptions options)
            : this(baseStream, schema, leaveOpen, options, allocator: null)
        {
        }

        public ArrowStreamWriter(Stream baseStream, Schema schema, bool leaveOpen, IpcOptions options, MemoryAllocator allocator)
        {
            BaseStream = baseStream ?? throw new ArgumentNullException(nameof(baseStream));
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _leaveOpen = leaveOpen;
            _allocator = allocator ?? MemoryAllocator.Default.Value;

            Buffers = ArrayPool<byte>.Create();
            Builder = new FlatBufferBuilder(1024);
            HasWrittenSchema = false;

            _fieldTypeBuilder = new ArrowTypeFlatbufferBuilder(Builder);
            _options = options ?? IpcOptions.Default;

            if (_options.CompressionCodec.HasValue && _options.CompressionCodecFactory == null)
            {
                throw new ArgumentException(
                    $"A {nameof(_options.CompressionCodecFactory)} must be provided when a {nameof(_options.CompressionCodec)} is specified",
                    nameof(options));
            }
        }

        private Offset<Flatbuf.BodyCompression> GetBodyCompression()
        {
            if (_options.CompressionCodec == null)
            {
                return default;
            }

            var compressionType = _options.CompressionCodec.Value switch
            {
                CompressionCodecType.Lz4Frame => Flatbuf.CompressionType.LZ4_FRAME,
                CompressionCodecType.Zstd => Flatbuf.CompressionType.ZSTD,
                _ => throw new ArgumentOutOfRangeException()
            };
            return Flatbuf.BodyCompression.CreateBodyCompression(
                Builder, compressionType, Flatbuf.BodyCompressionMethod.BUFFER);
        }

        private protected void WriteRecordBatchInternal(RecordBatch recordBatch)
        {
            // TODO: Truncate buffers with extraneous padding / unused capacity

            if (!HasWrittenSchema)
            {
                WriteSchema(Schema);
                HasWrittenSchema = true;
            }

            if (!HasWrittenDictionaryBatch)
            {
                DictionaryCollector.Collect(recordBatch, ref _dictionaryMemo);
                WriteDictionaries(_dictionaryMemo);
                HasWrittenDictionaryBatch = true;
            }

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, VectorOffset fieldNodesVectorOffset, VectorOffset variadicCountsOffset) =
                PrepareWritingRecordBatch(recordBatch);

            VectorOffset buffersVectorOffset = Builder.EndVector();

            // Serialize record batch

            StartingWritingRecordBatch();

            Offset<Flatbuf.RecordBatch> recordBatchOffset = Flatbuf.RecordBatch.CreateRecordBatch(Builder, recordBatch.Length,
                fieldNodesVectorOffset,
                buffersVectorOffset,
                GetBodyCompression(),
                variadicCountsOffset);

            long metadataLength = WriteMessage(Flatbuf.MessageHeader.RecordBatch,
                recordBatchOffset, recordBatchBuilder.TotalLength);

            long bufferLength = WriteBufferData(recordBatchBuilder.Buffers);

            FinishedWritingRecordBatch(bufferLength, metadataLength);
        }

        private protected async Task WriteRecordBatchInternalAsync(RecordBatch recordBatch,
            CancellationToken cancellationToken = default)
        {
            if (!HasWrittenSchema)
            {
                await WriteSchemaAsync(Schema, cancellationToken).ConfigureAwait(false);
                HasWrittenSchema = true;
            }

            if (!HasWrittenDictionaryBatch)
            {
                DictionaryCollector.Collect(recordBatch, ref _dictionaryMemo);
                await WriteDictionariesAsync(_dictionaryMemo, cancellationToken).ConfigureAwait(false);
                HasWrittenDictionaryBatch = true;
            }

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, VectorOffset fieldNodesVectorOffset, VectorOffset variadicCountsOffset) =
                PrepareWritingRecordBatch(recordBatch);

            VectorOffset buffersVectorOffset = Builder.EndVector();

            // Serialize record batch

            StartingWritingRecordBatch();

            Offset<Flatbuf.RecordBatch> recordBatchOffset = Flatbuf.RecordBatch.CreateRecordBatch(Builder, recordBatch.Length,
                fieldNodesVectorOffset,
                buffersVectorOffset,
                GetBodyCompression(),
                variadicCountsOffset);

            long metadataLength = await WriteMessageAsync(Flatbuf.MessageHeader.RecordBatch,
                recordBatchOffset, recordBatchBuilder.TotalLength,
                cancellationToken).ConfigureAwait(false);

            long bufferLength = await WriteBufferDataAsync(recordBatchBuilder.Buffers, cancellationToken).ConfigureAwait(false);

            FinishedWritingRecordBatch(bufferLength, metadataLength);
        }

        private long WriteBufferData(IReadOnlyList<ArrowRecordBatchFlatBufferBuilder.Buffer> buffers)
        {
            long bodyLength = 0;

            for (int i = 0; i < buffers.Count; i++)
            {
                ReadOnlyMemory<byte> buffer = buffers[i].DataBuffer;
                if (buffer.IsEmpty)
                    continue;

                BaseStream.Write(buffer);

                int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(buffer.Length));
                int padding = paddedLength - buffer.Length;
                if (padding > 0)
                {
                    WritePadding(padding);
                }

                bodyLength += paddedLength;
            }

            // Write padding so the record batch message body length is a multiple of 8 bytes

            int bodyPaddingLength = CalculatePadding(bodyLength);

            WritePadding(bodyPaddingLength);

            return bodyLength + bodyPaddingLength;
        }

        private async ValueTask<long> WriteBufferDataAsync(IReadOnlyList<ArrowRecordBatchFlatBufferBuilder.Buffer> buffers, CancellationToken cancellationToken = default)
        {
            long bodyLength = 0;

            for (int i = 0; i < buffers.Count; i++)
            {
                ReadOnlyMemory<byte> buffer = buffers[i].DataBuffer;
                if (buffer.IsEmpty)
                    continue;

                await BaseStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);

                int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(buffer.Length));
                int padding = paddedLength - buffer.Length;
                if (padding > 0)
                {
                    await WritePaddingAsync(padding).ConfigureAwait(false);
                }

                bodyLength += paddedLength;
            }

            // Write padding so the record batch message body length is a multiple of 8 bytes

            int bodyPaddingLength = CalculatePadding(bodyLength);

            await WritePaddingAsync(bodyPaddingLength).ConfigureAwait(false);

            return bodyLength + bodyPaddingLength;
        }

        private Tuple<ArrowRecordBatchFlatBufferBuilder, VectorOffset, VectorOffset> PrepareWritingRecordBatch(RecordBatch recordBatch)
        {
            return PrepareWritingRecordBatch(recordBatch.Schema.FieldsList, recordBatch.ArrayList);
        }

        private Tuple<ArrowRecordBatchFlatBufferBuilder, VectorOffset, VectorOffset> PrepareWritingRecordBatch(IReadOnlyList<Field> fields, IReadOnlyList<IArrowArray> arrays)
        {
            Builder.Clear();

            // CompressionCodec can be disposed after all data is visited by the builder,
            // and doesn't need to be alive for the full lifetime of the ArrowRecordBatchFlatBufferBuilder
            using var compressionCodec = _options.CompressionCodec.HasValue
                ? _options.CompressionCodecFactory.CreateCodec(_options.CompressionCodec.Value, _options.CompressionLevel)
                : null;

            var recordBatchBuilder = new ArrowRecordBatchFlatBufferBuilder(compressionCodec, _allocator, _compressionStream);

            // Visit all arrays recursively
            for (int i = 0; i < fields.Count; i++)
            {
                IArrowArray fieldArray = arrays[i];
                recordBatchBuilder.VisitArray(fieldArray);
            }

            // Serialize field nodes
            IReadOnlyList<ArrowRecordBatchFlatBufferBuilder.FieldNode> fieldNodes = recordBatchBuilder.FieldNodes;
            Flatbuf.RecordBatch.StartNodesVector(Builder, fieldNodes.Count);

            // flatbuffer struct vectors have to be created in reverse order
            for (int i = fieldNodes.Count - 1; i >= 0; i--)
            {
                Flatbuf.FieldNode.CreateFieldNode(Builder, fieldNodes[i].Length, fieldNodes[i].NullCount);
            }

            VectorOffset fieldNodesVectorOffset = Builder.EndVector();

            VectorOffset variadicCountOffset = default;
            if (recordBatchBuilder.VariadicCounts != null)
            {
                variadicCountOffset = Flatbuf.RecordBatch.CreateVariadicCountsVectorBlock(Builder, recordBatchBuilder.VariadicCounts.ToArray());
            }

            // Serialize buffers
            IReadOnlyList<ArrowRecordBatchFlatBufferBuilder.Buffer> buffers = recordBatchBuilder.Buffers;
            Flatbuf.RecordBatch.StartBuffersVector(Builder, buffers.Count);

            // flatbuffer struct vectors have to be created in reverse order
            for (int i = buffers.Count - 1; i >= 0; i--)
            {
                Flatbuf.Buffer.CreateBuffer(Builder,
                    buffers[i].Offset, buffers[i].DataBuffer.Length);
            }

            return Tuple.Create(recordBatchBuilder, fieldNodesVectorOffset, variadicCountOffset);
        }

        private protected virtual void StartingWritingDictionary()
        {
        }

        private protected virtual void FinishedWritingDictionary(long bodyLength, long metadataLength)
        {
        }

        private protected void WriteDictionaries(DictionaryMemo dictionaryMemo)
        {
            int fieldCount = dictionaryMemo?.DictionaryCount ?? 0;
            for (int i = 0; i < fieldCount; i++)
            {
                WriteDictionary(i, dictionaryMemo.GetDictionaryType(i), dictionaryMemo.GetDictionary(i));
            }
        }

        private protected void WriteDictionary(long id, IArrowType valueType, IArrowArray dictionary)
        {
            StartingWritingDictionary();

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, Offset<Flatbuf.DictionaryBatch> dictionaryBatchOffset) =
                CreateDictionaryBatchOffset(id, valueType, dictionary);

            long metadataLength = WriteMessage(Flatbuf.MessageHeader.DictionaryBatch,
                dictionaryBatchOffset, recordBatchBuilder.TotalLength);

            long bufferLength = WriteBufferData(recordBatchBuilder.Buffers);

            FinishedWritingDictionary(bufferLength, metadataLength);
        }

        private protected async Task WriteDictionariesAsync(DictionaryMemo dictionaryMemo, CancellationToken cancellationToken)
        {
            int fieldCount = dictionaryMemo?.DictionaryCount ?? 0;
            for (int i = 0; i < fieldCount; i++)
            {
                await WriteDictionaryAsync(i, dictionaryMemo.GetDictionaryType(i), dictionaryMemo.GetDictionary(i), cancellationToken).ConfigureAwait(false);
            }
        }

        private protected async Task WriteDictionaryAsync(long id, IArrowType valueType, IArrowArray dictionary, CancellationToken cancellationToken)
        {
            StartingWritingDictionary();

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, Offset<Flatbuf.DictionaryBatch> dictionaryBatchOffset) =
                CreateDictionaryBatchOffset(id, valueType, dictionary);

            long metadataLength = await WriteMessageAsync(Flatbuf.MessageHeader.DictionaryBatch,
                dictionaryBatchOffset, recordBatchBuilder.TotalLength, cancellationToken).ConfigureAwait(false);

            long bufferLength = await WriteBufferDataAsync(recordBatchBuilder.Buffers, cancellationToken).ConfigureAwait(false);

            FinishedWritingDictionary(bufferLength, metadataLength);
        }

        private Tuple<ArrowRecordBatchFlatBufferBuilder, Offset<Flatbuf.DictionaryBatch>> CreateDictionaryBatchOffset(long id, IArrowType valueType, IArrowArray dictionary)
        {
            Field dictionaryField = new Field("dummy", valueType, false);

            var fields = new Field[] { dictionaryField };

            var arrays = new List<IArrowArray> { dictionary };

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, VectorOffset fieldNodesVectorOffset, VectorOffset variadicCountsOffset) =
                PrepareWritingRecordBatch(fields, arrays);

            VectorOffset buffersVectorOffset = Builder.EndVector();

            // Serialize record batch
            Offset<Flatbuf.RecordBatch> recordBatchOffset = Flatbuf.RecordBatch.CreateRecordBatch(Builder, dictionary.Length,
                fieldNodesVectorOffset,
                buffersVectorOffset,
                GetBodyCompression(),
                variadicCountsOffset);

            // TODO: Support delta.
            Offset<Flatbuf.DictionaryBatch> dictionaryBatchOffset = Flatbuf.DictionaryBatch.CreateDictionaryBatch(Builder, id, recordBatchOffset, false);
            return Tuple.Create(recordBatchBuilder, dictionaryBatchOffset);
        }

        private protected virtual void WriteStartInternal()
        {
            if (!HasWrittenSchema)
            {
                WriteSchema(Schema);
                HasWrittenSchema = true;
            }
        }

        private protected async virtual ValueTask WriteStartInternalAsync(CancellationToken cancellationToken)
        {
            if (!HasWrittenSchema)
            {
                await WriteSchemaAsync(Schema, cancellationToken).ConfigureAwait(false);
                HasWrittenSchema = true;
            }
        }

        private protected virtual void WriteEndInternal()
        {
            WriteIpcMessageLength(length: 0);
        }

        private protected virtual ValueTask WriteEndInternalAsync(CancellationToken cancellationToken)
        {
            return WriteIpcMessageLengthAsync(length: 0, cancellationToken);
        }

        private protected virtual void StartingWritingRecordBatch()
        {
        }

        private protected virtual void FinishedWritingRecordBatch(long bodyLength, long metadataLength)
        {
        }

        public virtual void WriteRecordBatch(RecordBatch recordBatch)
        {
            WriteRecordBatchInternal(recordBatch);
        }

        public virtual Task WriteRecordBatchAsync(RecordBatch recordBatch, CancellationToken cancellationToken = default)
        {
            return WriteRecordBatchInternalAsync(recordBatch, cancellationToken);
        }

        public void WriteStart()
        {
            if (!HasWrittenStart)
            {
                WriteStartInternal();
                HasWrittenStart = true;
            }
        }

        public async Task WriteStartAsync(CancellationToken cancellationToken = default)
        {
            if (!HasWrittenStart)
            {
                await WriteStartInternalAsync(cancellationToken);
                HasWrittenStart = true;
            }
        }

        public void WriteEnd()
        {
            if (!HasWrittenEnd)
            {
                WriteEndInternal();
                HasWrittenEnd = true;
            }
        }

        public async Task WriteEndAsync(CancellationToken cancellationToken = default)
        {
            if (!HasWrittenEnd)
            {
                await WriteEndInternalAsync(cancellationToken);
                HasWrittenEnd = true;
            }
        }

        private protected Offset<Flatbuf.Schema> SerializeSchema(Schema schema)
        {
            // Build metadata
            VectorOffset metadataVectorOffset = default;
            if (schema.HasMetadata)
            {
                Offset<Flatbuf.KeyValue>[] metadataOffsets = GetMetadataOffsets(schema.Metadata);
                metadataVectorOffset = Flatbuf.Schema.CreateCustomMetadataVector(Builder, metadataOffsets);
            }

            // Build fields
            var fieldOffsets = new Offset<Flatbuf.Field>[schema.FieldsList.Count];
            for (int i = 0; i < fieldOffsets.Length; i++)
            {
                Field field = schema.GetFieldByIndex(i);
                StringOffset fieldNameOffset = Builder.CreateString(field.Name);
                ArrowTypeFlatbufferBuilder.FieldType fieldType = _fieldTypeBuilder.BuildFieldType(field);

                VectorOffset fieldChildrenVectorOffset = GetChildrenFieldOffset(field);
                VectorOffset fieldMetadataVectorOffset = GetFieldMetadataOffset(field);
                Offset<Flatbuf.DictionaryEncoding> dictionaryOffset = GetDictionaryOffset(field);

                fieldOffsets[i] = Flatbuf.Field.CreateField(Builder,
                    fieldNameOffset, field.IsNullable, fieldType.Type, fieldType.Offset,
                    dictionaryOffset, fieldChildrenVectorOffset, fieldMetadataVectorOffset);
            }

            VectorOffset fieldsVectorOffset = Flatbuf.Schema.CreateFieldsVector(Builder, fieldOffsets);

            // Build schema

            Flatbuf.Endianness endianness = BitConverter.IsLittleEndian ? Flatbuf.Endianness.Little : Flatbuf.Endianness.Big;

            return Flatbuf.Schema.CreateSchema(
                Builder, endianness, fieldsVectorOffset, metadataVectorOffset);
        }

        private VectorOffset GetChildrenFieldOffset(Field field)
        {
            IArrowType targetDataType = field.DataType is DictionaryType dictionaryType ?
                dictionaryType.ValueType :
                field.DataType;

            if (!(targetDataType is NestedType type))
            {
                return default;
            }

            int childrenCount = type.Fields.Count;
            var children = new Offset<Flatbuf.Field>[childrenCount];

            for (int i = 0; i < childrenCount; i++)
            {
                Field childField = type.Fields[i];
                StringOffset childFieldNameOffset = Builder.CreateString(childField.Name);
                ArrowTypeFlatbufferBuilder.FieldType childFieldType = _fieldTypeBuilder.BuildFieldType(childField);

                VectorOffset childFieldChildrenVectorOffset = GetChildrenFieldOffset(childField);
                VectorOffset childFieldMetadataVectorOffset = GetFieldMetadataOffset(childField);
                Offset<Flatbuf.DictionaryEncoding> dictionaryOffset = GetDictionaryOffset(childField);

                children[i] = Flatbuf.Field.CreateField(Builder,
                    childFieldNameOffset, childField.IsNullable, childFieldType.Type, childFieldType.Offset,
                    dictionaryOffset, childFieldChildrenVectorOffset, childFieldMetadataVectorOffset);
            }

            return Builder.CreateVectorOfTables(children);
        }

        private VectorOffset GetFieldMetadataOffset(Field field)
        {
            if (!field.HasMetadata)
            {
                return default;
            }

            Offset<Flatbuf.KeyValue>[] metadataOffsets = GetMetadataOffsets(field.Metadata);
            return Flatbuf.Field.CreateCustomMetadataVector(Builder, metadataOffsets);
        }

        private Offset<Flatbuf.DictionaryEncoding> GetDictionaryOffset(Field field)
        {
            if (field.DataType.TypeId != ArrowTypeId.Dictionary)
            {
                return default;
            }

            long id = DictionaryMemo.GetOrAssignId(field);
            var dicType = field.DataType as DictionaryType;
            var indexType = dicType.IndexType as NumberType;

            Offset<Flatbuf.Int> indexOffset = Flatbuf.Int.CreateInt(Builder, indexType.BitWidth, indexType.IsSigned);
            return Flatbuf.DictionaryEncoding.CreateDictionaryEncoding(Builder, id, indexOffset, dicType.Ordered);
        }

        private Offset<Flatbuf.KeyValue>[] GetMetadataOffsets(IReadOnlyDictionary<string, string> metadata)
        {
            Debug.Assert(metadata != null);
            Debug.Assert(metadata.Count > 0);

            Offset<Flatbuf.KeyValue>[] metadataOffsets = new Offset<Flatbuf.KeyValue>[metadata.Count];
            int index = 0;
            foreach (KeyValuePair<string, string> metadatum in metadata)
            {
                StringOffset keyOffset = Builder.CreateString(metadatum.Key);
                StringOffset valueOffset = Builder.CreateString(metadatum.Value);

                metadataOffsets[index++] = Flatbuf.KeyValue.CreateKeyValue(Builder, keyOffset, valueOffset);
            }

            return metadataOffsets;
        }

        private Offset<Flatbuf.Schema> WriteSchema(Schema schema)
        {
            Builder.Clear();

            // Build schema

            Offset<Flatbuf.Schema> schemaOffset = SerializeSchema(schema);

            // Build message

            WriteMessage(Flatbuf.MessageHeader.Schema, schemaOffset, 0);

            return schemaOffset;
        }

        private async ValueTask<Offset<Flatbuf.Schema>> WriteSchemaAsync(Schema schema, CancellationToken cancellationToken)
        {
            Builder.Clear();

            // Build schema

            Offset<Flatbuf.Schema> schemaOffset = SerializeSchema(schema);

            // Build message

            await WriteMessageAsync(Flatbuf.MessageHeader.Schema, schemaOffset, 0, cancellationToken)
                .ConfigureAwait(false);

            return schemaOffset;
        }

        /// <summary>
        /// Writes the message to the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>
        /// The number of bytes written to the stream.
        /// </returns>
        private protected long WriteMessage<T>(
            Flatbuf.MessageHeader headerType, Offset<T> headerOffset, int bodyLength)
            where T : struct
        {
            Offset<Flatbuf.Message> messageOffset = Flatbuf.Message.CreateMessage(
                Builder, CurrentMetadataVersion, headerType, headerOffset.Value,
                bodyLength);

            Builder.Finish(messageOffset.Value);

            ReadOnlyMemory<byte> messageData = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);
            int messagePaddingLength = CalculatePadding(_options.SizeOfIpcLength + messageData.Length);

            WriteIpcMessageLength(messageData.Length + messagePaddingLength);

            BaseStream.Write(messageData);
            WritePadding(messagePaddingLength);

            checked
            {
                return _options.SizeOfIpcLength + messageData.Length + messagePaddingLength;
            }
        }

        /// <summary>
        /// Writes the message to the <see cref="BaseStream"/>.
        /// </summary>
        /// <returns>
        /// The number of bytes written to the stream.
        /// </returns>
        private protected virtual async ValueTask<long> WriteMessageAsync<T>(
            Flatbuf.MessageHeader headerType, Offset<T> headerOffset, int bodyLength,
            CancellationToken cancellationToken)
            where T : struct
        {
            Offset<Flatbuf.Message> messageOffset = Flatbuf.Message.CreateMessage(
                Builder, CurrentMetadataVersion, headerType, headerOffset.Value,
                bodyLength);

            Builder.Finish(messageOffset.Value);

            ReadOnlyMemory<byte> messageData = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);
            int messagePaddingLength = CalculatePadding(_options.SizeOfIpcLength + messageData.Length);

            await WriteIpcMessageLengthAsync(messageData.Length + messagePaddingLength, cancellationToken)
                .ConfigureAwait(false);

            await BaseStream.WriteAsync(messageData, cancellationToken).ConfigureAwait(false);
            await WritePaddingAsync(messagePaddingLength).ConfigureAwait(false);

            checked
            {
                return _options.SizeOfIpcLength + messageData.Length + messagePaddingLength;
            }
        }

        private protected void WriteFlatBuffer()
        {
            ReadOnlyMemory<byte> segment = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);

            BaseStream.Write(segment);
        }

        private protected async ValueTask WriteFlatBufferAsync(CancellationToken cancellationToken = default)
        {
            ReadOnlyMemory<byte> segment = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);

            await BaseStream.WriteAsync(segment, cancellationToken).ConfigureAwait(false);
        }

        private void WriteIpcMessageLength(int length)
        {
            using (Buffers.RentReturn(_options.SizeOfIpcLength, out Memory<byte> buffer))
            {
                Memory<byte> currentBufferPosition = buffer;
                if (!_options.WriteLegacyIpcFormat)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(
                        currentBufferPosition.Span, MessageSerializer.IpcContinuationToken);
                    currentBufferPosition = currentBufferPosition.Slice(sizeof(int));
                }

                BinaryPrimitives.WriteInt32LittleEndian(currentBufferPosition.Span, length);
                BaseStream.Write(buffer);
            }
        }

        private async ValueTask WriteIpcMessageLengthAsync(int length, CancellationToken cancellationToken)
        {
            using (Buffers.RentReturn(_options.SizeOfIpcLength, out Memory<byte> buffer))
            {
                Memory<byte> currentBufferPosition = buffer;
                if (!_options.WriteLegacyIpcFormat)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(
                        currentBufferPosition.Span, MessageSerializer.IpcContinuationToken);
                    currentBufferPosition = currentBufferPosition.Slice(sizeof(int));
                }

                BinaryPrimitives.WriteInt32LittleEndian(currentBufferPosition.Span, length);
                await BaseStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            }
        }

        protected int CalculatePadding(long offset, int alignment = 8)
        {
            long result = BitUtility.RoundUpToMultiplePowerOfTwo(offset, alignment) - offset;
            checked
            {
                return (int)result;
            }
        }

        private static int CalculatePaddedBufferLength(int length)
        {
            long result = BitUtility.RoundUpToMultiplePowerOfTwo(length, MemoryAllocator.DefaultAlignment);
            checked
            {
                return (int)result;
            }
        }

        private protected void WritePadding(int length)
        {
            if (length > 0)
            {
                BaseStream.Write(s_padding.AsMemory(0, Math.Min(s_padding.Length, length)));
            }
        }

        private protected ValueTask WritePaddingAsync(int length)
        {
            if (length > 0)
            {
                return BaseStream.WriteAsync(s_padding.AsMemory(0, Math.Min(s_padding.Length, length)));
            }

            return default;
        }

        public virtual void Dispose()
        {
            if (!_leaveOpen)
            {
                BaseStream.Dispose();
            }
            _compressionStream.Dispose();
        }
    }

    internal static class DictionaryCollector
    {
        internal static void Collect(RecordBatch recordBatch, ref DictionaryMemo dictionaryMemo)
        {
            Schema schema = recordBatch.Schema;
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Field field = schema.GetFieldByIndex(i);
                IArrowArray array = recordBatch.Column(i);

                CollectDictionary(field, array.Data, ref dictionaryMemo);
            }
        }

        private static void CollectDictionary(Field field, ArrayData arrayData, ref DictionaryMemo dictionaryMemo)
        {
            if (field.DataType is DictionaryType dictionaryType)
            {
                if (arrayData.Dictionary == null)
                {
                    throw new ArgumentException($"{nameof(arrayData.Dictionary)} must not be null");
                }
                arrayData.Dictionary.EnsureDataType(dictionaryType.ValueType.TypeId);

                IArrowArray dictionary = ArrowArrayFactory.BuildArray(arrayData.Dictionary);
                WalkChildren(dictionary.Data, ref dictionaryMemo);

                dictionaryMemo ??= new DictionaryMemo();
                long id = dictionaryMemo.GetOrAssignId(field);

                dictionaryMemo.AddOrReplaceDictionary(id, dictionary);
            }
            else
            {
                WalkChildren(arrayData, ref dictionaryMemo);
            }
        }

        private static void WalkChildren(ArrayData arrayData, ref DictionaryMemo dictionaryMemo)
        {
            ArrayData[] children = arrayData.Children;

            if (children == null)
            {
                return;
            }

            if (arrayData.DataType is NestedType nestedType)
            {
                for (int i = 0; i < nestedType.Fields.Count; i++)
                {
                    Field childField = nestedType.Fields[i];
                    ArrayData child = children[i];

                    CollectDictionary(childField, child, ref dictionaryMemo);
                }
            }
        }
    }
}
