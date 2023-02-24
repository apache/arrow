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
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using FlatBuffers;

namespace Apache.Arrow.Ipc
{
    public class ArrowStreamWriter : IDisposable
    {
        internal class ArrowRecordBatchFlatBufferBuilder :
            IArrowArrayVisitor<Int8Array>,
            IArrowArrayVisitor<Int16Array>,
            IArrowArrayVisitor<Int32Array>,
            IArrowArrayVisitor<Int64Array>,
            IArrowArrayVisitor<UInt8Array>,
            IArrowArrayVisitor<UInt16Array>,
            IArrowArrayVisitor<UInt32Array>,
            IArrowArrayVisitor<UInt64Array>,
            IArrowArrayVisitor<FloatArray>,
            IArrowArrayVisitor<DoubleArray>,
            IArrowArrayVisitor<BooleanArray>,
            IArrowArrayVisitor<TimestampArray>,
            IArrowArrayVisitor<Date32Array>,
            IArrowArrayVisitor<Date64Array>,
            IArrowArrayVisitor<Time32Array>,
            IArrowArrayVisitor<Time64Array>,
            IArrowArrayVisitor<ListArray>,
            IArrowArrayVisitor<StringArray>,
            IArrowArrayVisitor<BinaryArray>,
            IArrowArrayVisitor<FixedSizeBinaryArray>,
            IArrowArrayVisitor<StructArray>,
            IArrowArrayVisitor<Decimal128Array>,
            IArrowArrayVisitor<Decimal256Array>,
            IArrowArrayVisitor<DictionaryArray>
        {
            public readonly struct Buffer
            {
                public readonly ArrowBuffer DataBuffer;
                public readonly int Offset;

                public Buffer(ArrowBuffer buffer, int offset)
                {
                    DataBuffer = buffer;
                    Offset = offset;
                }
            }

            private readonly List<Buffer> _buffers;

            public IReadOnlyList<Buffer> Buffers => _buffers;

            public int TotalLength { get; private set; }

            public ArrowRecordBatchFlatBufferBuilder()
            {
                _buffers = new List<Buffer>();
                TotalLength = 0;
            }

            public void Visit(Int8Array array) => CreateBuffers(array);
            public void Visit(Int16Array array) => CreateBuffers(array);
            public void Visit(Int32Array array) => CreateBuffers(array);
            public void Visit(Int64Array array) => CreateBuffers(array);
            public void Visit(UInt8Array array) => CreateBuffers(array);
            public void Visit(UInt16Array array) => CreateBuffers(array);
            public void Visit(UInt32Array array) => CreateBuffers(array);
            public void Visit(UInt64Array array) => CreateBuffers(array);
            public void Visit(FloatArray array) => CreateBuffers(array);
            public void Visit(DoubleArray array) => CreateBuffers(array);
            public void Visit(TimestampArray array) => CreateBuffers(array);
            public void Visit(BooleanArray array) => CreateBuffers(array);
            public void Visit(Date32Array array) => CreateBuffers(array);
            public void Visit(Date64Array array) => CreateBuffers(array);
            public void Visit(Time32Array array) => CreateBuffers(array);
            public void Visit(Time64Array array) => CreateBuffers(array);

            public void Visit(ListArray array)
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.ValueOffsetsBuffer));

                array.Values.Accept(this);
            }

            public void Visit(StringArray array) => Visit(array as BinaryArray);

            public void Visit(BinaryArray array)
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.ValueOffsetsBuffer));
                _buffers.Add(CreateBuffer(array.ValueBuffer));
            }

            public void Visit(FixedSizeBinaryArray array)
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.ValueBuffer));
            }

            public void Visit(Decimal128Array array)
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.ValueBuffer));
            }

            public void Visit(Decimal256Array array)
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.ValueBuffer));
            }

            public void Visit(StructArray array)
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));

                for (int i = 0; i < array.Fields.Count; i++)
                {
                    array.Fields[i].Accept(this);
                }
            }

            public void Visit(DictionaryArray array)
            {
                // Dictionary is serialized separately in Dictionary serialization.
                // We are only interested in indices at this context.

                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.IndicesBuffer));
            }

            private void CreateBuffers(BooleanArray array)
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.ValueBuffer));
            }

            private void CreateBuffers<T>(PrimitiveArray<T> array)
                where T : struct
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.ValueBuffer));
            }

            private Buffer CreateBuffer(ArrowBuffer buffer)
            {
                int offset = TotalLength;

                int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(buffer.Length));
                TotalLength += paddedLength;

                return new Buffer(buffer, offset);
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

        private protected const Flatbuf.MetadataVersion CurrentMetadataVersion = Flatbuf.MetadataVersion.V4;

        private static readonly byte[] s_padding = new byte[64];

        private readonly ArrowTypeFlatbufferBuilder _fieldTypeBuilder;

        private DictionaryMemo _dictionaryMemo;
        private DictionaryMemo DictionaryMemo => _dictionaryMemo ??= new DictionaryMemo();

        public ArrowStreamWriter(Stream baseStream, Schema schema)
            : this(baseStream, schema, leaveOpen: false)
        {
        }

        public ArrowStreamWriter(Stream baseStream, Schema schema, bool leaveOpen)
            : this(baseStream, schema, leaveOpen, options: null)
        {
        }

        public ArrowStreamWriter(Stream baseStream, Schema schema, bool leaveOpen, IpcOptions options)
        {
            BaseStream = baseStream ?? throw new ArgumentNullException(nameof(baseStream));
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _leaveOpen = leaveOpen;

            Buffers = ArrayPool<byte>.Create();
            Builder = new FlatBufferBuilder(1024);
            HasWrittenSchema = false;

            _fieldTypeBuilder = new ArrowTypeFlatbufferBuilder(Builder);
            _options = options ?? IpcOptions.Default;
        }


        private void CreateSelfAndChildrenFieldNodes(ArrayData data)
        {
            if (data.DataType is NestedType)
            {
                // flatbuffer struct vectors have to be created in reverse order
                for (int i = data.Children.Length - 1; i >= 0; i--)
                {
                    CreateSelfAndChildrenFieldNodes(data.Children[i]);
                }
            }
            Flatbuf.FieldNode.CreateFieldNode(Builder, data.Length, data.NullCount);
        }

        private static int CountAllNodes(IReadOnlyList<Field> fields)
        {
            int count = 0;
            foreach (Field arrowArray in fields)
            {
                CountSelfAndChildrenNodes(arrowArray.DataType, ref count);
            }
            return count;
        }

        private static void CountSelfAndChildrenNodes(IArrowType type, ref int count)
        {
            if (type is NestedType nestedType)
            {
                foreach (Field childField in nestedType.Fields)
                {
                    CountSelfAndChildrenNodes(childField.DataType, ref count);
                }
            }
            count++;
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
                WriteDictionaries(recordBatch);
                HasWrittenDictionaryBatch = true;
            }

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, VectorOffset fieldNodesVectorOffset) =
                PreparingWritingRecordBatch(recordBatch);

            VectorOffset buffersVectorOffset = Builder.EndVector();

            // Serialize record batch

            StartingWritingRecordBatch();

            Offset<Flatbuf.RecordBatch> recordBatchOffset = Flatbuf.RecordBatch.CreateRecordBatch(Builder, recordBatch.Length,
                fieldNodesVectorOffset,
                buffersVectorOffset);

            long metadataLength = WriteMessage(Flatbuf.MessageHeader.RecordBatch,
                recordBatchOffset, recordBatchBuilder.TotalLength);

            long bufferLength = WriteBufferData(recordBatchBuilder.Buffers);

            FinishedWritingRecordBatch(bufferLength, metadataLength);
        }

        private protected async Task WriteRecordBatchInternalAsync(RecordBatch recordBatch,
            CancellationToken cancellationToken = default)
        {
            // TODO: Truncate buffers with extraneous padding / unused capacity

            if (!HasWrittenSchema)
            {
                await WriteSchemaAsync(Schema, cancellationToken).ConfigureAwait(false);
                HasWrittenSchema = true;
            }

            if (!HasWrittenDictionaryBatch)
            {
                DictionaryCollector.Collect(recordBatch, ref _dictionaryMemo);
                await WriteDictionariesAsync(recordBatch, cancellationToken).ConfigureAwait(false);
                HasWrittenDictionaryBatch = true;
            }

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, VectorOffset fieldNodesVectorOffset) =
                PreparingWritingRecordBatch(recordBatch);

            VectorOffset buffersVectorOffset = Builder.EndVector();

            // Serialize record batch

            StartingWritingRecordBatch();

            Offset<Flatbuf.RecordBatch> recordBatchOffset = Flatbuf.RecordBatch.CreateRecordBatch(Builder, recordBatch.Length,
                fieldNodesVectorOffset,
                buffersVectorOffset);

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
                ArrowBuffer buffer = buffers[i].DataBuffer;
                if (buffer.IsEmpty)
                    continue;

                WriteBuffer(buffer);

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
                ArrowBuffer buffer = buffers[i].DataBuffer;
                if (buffer.IsEmpty)
                    continue;

                await WriteBufferAsync(buffer, cancellationToken).ConfigureAwait(false);

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

        private Tuple<ArrowRecordBatchFlatBufferBuilder, VectorOffset> PreparingWritingRecordBatch(RecordBatch recordBatch)
        {
            return PreparingWritingRecordBatch(recordBatch.Schema.FieldsList, recordBatch.ArrayList);
        }

        private Tuple<ArrowRecordBatchFlatBufferBuilder, VectorOffset> PreparingWritingRecordBatch(IReadOnlyList<Field> fields, IReadOnlyList<IArrowArray> arrays)
        {
            Builder.Clear();

            // Serialize field nodes

            int fieldCount = fields.Count;

            Flatbuf.RecordBatch.StartNodesVector(Builder, CountAllNodes(fields));

            // flatbuffer struct vectors have to be created in reverse order
            for (int i = fieldCount - 1; i >= 0; i--)
            {
                CreateSelfAndChildrenFieldNodes(arrays[i].Data);
            }

            VectorOffset fieldNodesVectorOffset = Builder.EndVector();

            // Serialize buffers

            var recordBatchBuilder = new ArrowRecordBatchFlatBufferBuilder();
            for (int i = 0; i < fieldCount; i++)
            {
                IArrowArray fieldArray = arrays[i];
                fieldArray.Accept(recordBatchBuilder);
            }

            IReadOnlyList<ArrowRecordBatchFlatBufferBuilder.Buffer> buffers = recordBatchBuilder.Buffers;

            Flatbuf.RecordBatch.StartBuffersVector(Builder, buffers.Count);

            // flatbuffer struct vectors have to be created in reverse order
            for (int i = buffers.Count - 1; i >= 0; i--)
            {
                Flatbuf.Buffer.CreateBuffer(Builder,
                    buffers[i].Offset, buffers[i].DataBuffer.Length);
            }

            return Tuple.Create(recordBatchBuilder, fieldNodesVectorOffset);
        }


        private protected void WriteDictionaries(RecordBatch recordBatch)
        {
            foreach (Field field in recordBatch.Schema.FieldsList)
            {
                WriteDictionary(field);
            }
        }

        private protected void WriteDictionary(Field field)
        {
            if (field.DataType.TypeId != ArrowTypeId.Dictionary)
            {
                if (field.DataType is NestedType nestedType)
                {
                    foreach (Field child in nestedType.Fields)
                    {
                        WriteDictionary(child);
                    }
                }
                return;
            }

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, Offset<Flatbuf.DictionaryBatch> dictionaryBatchOffset) =
                CreateDictionaryBatchOffset(field);

            WriteMessage(Flatbuf.MessageHeader.DictionaryBatch,
                dictionaryBatchOffset, recordBatchBuilder.TotalLength);

            WriteBufferData(recordBatchBuilder.Buffers);
        }

        private protected async Task WriteDictionariesAsync(RecordBatch recordBatch, CancellationToken cancellationToken)
        {
            foreach (Field field in recordBatch.Schema.FieldsList)
            {
                await WriteDictionaryAsync(field, cancellationToken).ConfigureAwait(false);
            }
        }

        private protected async Task WriteDictionaryAsync(Field field, CancellationToken cancellationToken)
        {
            if (field.DataType.TypeId != ArrowTypeId.Dictionary)
            {
                if (field.DataType is NestedType nestedType)
                {
                    foreach (Field child in nestedType.Fields)
                    {
                        await WriteDictionaryAsync(child, cancellationToken).ConfigureAwait(false);
                    }
                }
                return;
            }

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, Offset<Flatbuf.DictionaryBatch> dictionaryBatchOffset) =
                CreateDictionaryBatchOffset(field);

            await WriteMessageAsync(Flatbuf.MessageHeader.DictionaryBatch,
                dictionaryBatchOffset, recordBatchBuilder.TotalLength, cancellationToken).ConfigureAwait(false);

            await WriteBufferDataAsync(recordBatchBuilder.Buffers, cancellationToken).ConfigureAwait(false);
        }

        private Tuple<ArrowRecordBatchFlatBufferBuilder, Offset<Flatbuf.DictionaryBatch>> CreateDictionaryBatchOffset(Field field)
        {
            Field dictionaryField = new Field("dummy", ((DictionaryType)field.DataType).ValueType, false);
            long id = DictionaryMemo.GetId(field);
            IArrowArray dictionary = DictionaryMemo.GetDictionary(id);

            var fields = new Field[] { dictionaryField };

            var arrays = new List<IArrowArray> { dictionary };

            (ArrowRecordBatchFlatBufferBuilder recordBatchBuilder, VectorOffset fieldNodesVectorOffset) =
                PreparingWritingRecordBatch(fields, arrays);

            VectorOffset buffersVectorOffset = Builder.EndVector();

            // Serialize record batch
            Offset<Flatbuf.RecordBatch> recordBatchOffset = Flatbuf.RecordBatch.CreateRecordBatch(Builder, dictionary.Length,
                fieldNodesVectorOffset,
                buffersVectorOffset);

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

        private void WriteBuffer(ArrowBuffer arrowBuffer)
        {
            BaseStream.Write(arrowBuffer.Memory);
        }

        private ValueTask WriteBufferAsync(ArrowBuffer arrowBuffer, CancellationToken cancellationToken = default)
        {
            return BaseStream.WriteAsync(arrowBuffer.Memory, cancellationToken);
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
            Buffers.RentReturn(_options.SizeOfIpcLength, (buffer) =>
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
            });
        }

        private async ValueTask WriteIpcMessageLengthAsync(int length, CancellationToken cancellationToken)
        {
            await Buffers.RentReturnAsync(_options.SizeOfIpcLength, async (buffer) =>
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
            }).ConfigureAwait(false);
        }

        protected int CalculatePadding(long offset, int alignment = 8)
        {
            long result = BitUtility.RoundUpToMultiplePowerOfTwo(offset, alignment) - offset;
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

                dictionaryMemo ??= new DictionaryMemo();
                long id = dictionaryMemo.GetOrAssignId(field);

                dictionaryMemo.AddOrReplaceDictionary(id, dictionary);
                WalkChildren(dictionary.Data, ref dictionaryMemo);
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
