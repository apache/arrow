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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
            IArrowArrayVisitor<ListArray>,
            IArrowArrayVisitor<StringArray>,
            IArrowArrayVisitor<BinaryArray>
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

        private bool HasWrittenEnd { get; set; }

        protected Schema Schema { get; }

        private readonly bool _leaveOpen;
        private readonly IpcOptions _options;

        private protected const Flatbuf.MetadataVersion CurrentMetadataVersion = Flatbuf.MetadataVersion.V4;

        private static readonly byte[] s_padding = new byte[64];

        private readonly ArrowTypeFlatbufferBuilder _fieldTypeBuilder;

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

        private int CountAllNodes()
        {
            int count = 0;
            foreach (Field arrowArray in Schema.Fields.Values)
            {
                CountSelfAndChildrenNodes(arrowArray.DataType, ref count);
            }
            return count;
        }

        private void CountSelfAndChildrenNodes(IArrowType type, ref int count)
        {
            if (type is NestedType nestedType)
            {
                foreach (Field childField in nestedType.Children)
                {
                    CountSelfAndChildrenNodes(childField.DataType, ref count);
                }
            }
            count++;
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

            Builder.Clear();

            // Serialize field nodes

            int fieldCount = Schema.Fields.Count;

            Flatbuf.RecordBatch.StartNodesVector(Builder, CountAllNodes());

            // flatbuffer struct vectors have to be created in reverse order
            for (int i = fieldCount - 1; i >= 0; i--)
            {
                CreateSelfAndChildrenFieldNodes(recordBatch.Column(i).Data);
            }

            VectorOffset fieldNodesVectorOffset = Builder.EndVector();

            // Serialize buffers

            var recordBatchBuilder = new ArrowRecordBatchFlatBufferBuilder();
            for (int i = 0; i < fieldCount; i++)
            {
                IArrowArray fieldArray = recordBatch.Column(i);
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

            VectorOffset buffersVectorOffset = Builder.EndVector();

            // Serialize record batch

            StartingWritingRecordBatch();

            Offset<Flatbuf.RecordBatch> recordBatchOffset = Flatbuf.RecordBatch.CreateRecordBatch(Builder, recordBatch.Length,
                fieldNodesVectorOffset,
                buffersVectorOffset);

            long metadataLength = await WriteMessageAsync(Flatbuf.MessageHeader.RecordBatch,
                recordBatchOffset, recordBatchBuilder.TotalLength,
                cancellationToken).ConfigureAwait(false);

            // Write buffer data

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

            FinishedWritingRecordBatch(bodyLength + bodyPaddingLength, metadataLength);
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

        public virtual Task WriteRecordBatchAsync(RecordBatch recordBatch, CancellationToken cancellationToken = default)
        {
            return WriteRecordBatchInternalAsync(recordBatch, cancellationToken);
        }

        public async Task WriteEndAsync(CancellationToken cancellationToken = default)
        {
            if (!HasWrittenEnd)
            {
                await WriteEndInternalAsync(cancellationToken);
                HasWrittenEnd = true;
            }
        }

        private ValueTask WriteBufferAsync(ArrowBuffer arrowBuffer, CancellationToken cancellationToken = default)
        {
            return BaseStream.WriteAsync(arrowBuffer.Memory, cancellationToken);
        }

        private protected Offset<Flatbuf.Schema> SerializeSchema(Schema schema)
        {
            // TODO: Serialize schema metadata

            // Build fields

            var fieldOffsets = new Offset<Flatbuf.Field>[schema.Fields.Count];

            for (int i = 0; i < fieldOffsets.Length; i++)
            {
                Field field = schema.GetFieldByIndex(i);
                StringOffset fieldNameOffset = Builder.CreateString(field.Name);
                ArrowTypeFlatbufferBuilder.FieldType fieldType = _fieldTypeBuilder.BuildFieldType(field);

                VectorOffset fieldChildrenVectorOffset = Builder.CreateVectorOfTables(GetChildrenFieldOffsets(field));

                fieldOffsets[i] = Flatbuf.Field.CreateField(Builder,
                    fieldNameOffset, field.IsNullable, fieldType.Type, fieldType.Offset,
                    default, fieldChildrenVectorOffset, default);
            }

            VectorOffset fieldsVectorOffset = Flatbuf.Schema.CreateFieldsVector(Builder, fieldOffsets);

            // Build schema

            Flatbuf.Endianness endianness = BitConverter.IsLittleEndian ? Flatbuf.Endianness.Little : Flatbuf.Endianness.Big;

            return Flatbuf.Schema.CreateSchema(
                Builder, endianness, fieldsVectorOffset);
        }

        private protected Offset<Flatbuf.Field>[] GetChildrenFieldOffsets(Field field)
        {
            if (!(field.DataType is NestedType type))
            {
                return System.Array.Empty<Offset<Flatbuf.Field>>();
            }

            int childrenCount = type.Children.Count;
            var children = new Offset<Flatbuf.Field>[childrenCount];

            for (int i = 0; i < childrenCount; i++)
            {
                Field childField = type.Children[i];
                StringOffset childFieldNameOffset = Builder.CreateString(childField.Name);
                ArrowTypeFlatbufferBuilder.FieldType childFieldType = _fieldTypeBuilder.BuildFieldType(childField);
                VectorOffset childFieldChildrenVectorOffset = Builder.CreateVectorOfTables(GetChildrenFieldOffsets(childField));

                children[i] = Flatbuf.Field.CreateField(Builder,
                    childFieldNameOffset, childField.IsNullable, childFieldType.Type, childFieldType.Offset,
                    default, childFieldChildrenVectorOffset, default);
            }
            return children;
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
        private async ValueTask<long> WriteMessageAsync<T>(
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

        private protected async ValueTask WriteFlatBufferAsync(CancellationToken cancellationToken = default)
        {
            ReadOnlyMemory<byte> segment = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);

            await BaseStream.WriteAsync(segment, cancellationToken).ConfigureAwait(false);
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
}
