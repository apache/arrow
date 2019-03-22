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
            public struct Buffer
            {
                public readonly ArrowBuffer DataBuffer;
                public readonly int Offset;
                public readonly int Length;

                public Buffer(ArrowBuffer buffer, int offset, int length)
                {
                    DataBuffer = buffer;
                    Offset = offset;
                    Length = length;
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

            private void CreateBuffers<T>(PrimitiveArray<T> array)
                where T: struct
            {
                _buffers.Add(CreateBuffer(array.NullBitmapBuffer));
                _buffers.Add(CreateBuffer(array.ValueBuffer));
            }

            private Buffer CreateBuffer(ArrowBuffer buffer)
            {
                var offset = TotalLength;

                TotalLength += buffer.Length;

                return new Buffer(buffer, offset, buffer.Length);
            }

            public void Visit(IArrowArray array)
            {
                throw new NotImplementedException();
            }
        }

        protected struct Block
        {
            public readonly int Offset;
            public readonly int Length;
            public readonly int MetadataLength;

            public Block(int offset, int length, int metadataLength)
            {
                Offset = offset;
                Length = length;
                MetadataLength = metadataLength;
            }
        }

        protected Stream BaseStream { get; }

        protected ArrayPool<byte> Buffers { get; }

        private protected FlatBufferBuilder Builder { get; }

        protected bool HasWrittenSchema { get; set; }

        protected Schema Schema { get; }

        private readonly bool _leaveOpen;

        private protected const Flatbuf.MetadataVersion CurrentMetadataVersion = Flatbuf.MetadataVersion.V4;

        private static readonly byte[] Padding = new byte[64];

        private readonly ArrowTypeFlatbufferBuilder _fieldTypeBuilder;

        public ArrowStreamWriter(Stream baseStream, Schema schema) : this(baseStream, schema, leaveOpen: false)
        {
        }

        public ArrowStreamWriter(Stream baseStream, Schema schema, bool leaveOpen)
        {
            BaseStream = baseStream ?? throw new ArgumentNullException(nameof(baseStream));
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _leaveOpen = leaveOpen;

            Buffers = ArrayPool<byte>.Create();
            Builder = new FlatBufferBuilder(1024);
            HasWrittenSchema = false;

            _fieldTypeBuilder = new ArrowTypeFlatbufferBuilder(Builder);
        }

        protected virtual async Task<Block> WriteRecordBatchInternalAsync(RecordBatch recordBatch,
            CancellationToken cancellationToken = default)
        {
            // TODO: Truncate buffers with extraneous padding / unused capacity

            if (!HasWrittenSchema)
            {
                await WriteSchemaAsync(Schema, cancellationToken).ConfigureAwait(false);
                HasWrittenSchema = true;
            }

            var recordBatchBuilder = new ArrowRecordBatchFlatBufferBuilder();

            Builder.Clear();

            // Serialize field nodes

            var fieldCount = Schema.Fields.Count;
            var fieldNodeOffsets = new Offset<Flatbuf.FieldNode>[fieldCount];

            Flatbuf.RecordBatch.StartNodesVector(Builder, fieldCount);

            for (var i = 0; i < fieldCount; i++)
            {
                var fieldArray = recordBatch.Column(i);
                fieldNodeOffsets[i] =
                    Flatbuf.FieldNode.CreateFieldNode(Builder, fieldArray.Length, fieldArray.NullCount);
            }

            var fieldNodesVectorOffset = Builder.EndVector();

            // Serialize buffers

            for (var i = 0; i < fieldCount; i++)
            {
                var fieldArray = recordBatch.Column(i);
                fieldArray.Accept(recordBatchBuilder);
            }

            var buffers = recordBatchBuilder.Buffers;
            var bufferOffsets = new Offset<Flatbuf.Buffer>[buffers.Count];

            Flatbuf.RecordBatch.StartBuffersVector(Builder, buffers.Count);

            for (var i = buffers.Count - 1; i >= 0; i--)
            {
                bufferOffsets[i] = Flatbuf.Buffer.CreateBuffer(Builder,
                    buffers[i].Offset, buffers[i].Length);
            }

            var buffersVectorOffset = Builder.EndVector();

            // Serialize record batch

            var recordBatchOffset = Flatbuf.RecordBatch.CreateRecordBatch(Builder, recordBatch.Length,
                fieldNodesVectorOffset,
                buffersVectorOffset);

            var metadataOffset = BaseStream.Position;

            await WriteMessageAsync(Flatbuf.MessageHeader.RecordBatch,
                recordBatchOffset, recordBatchBuilder.TotalLength,
                cancellationToken).ConfigureAwait(false);

            var metadataLength = BaseStream.Position - metadataOffset;

            // Write buffer data

            var lengthOffset = BaseStream.Position;

            for (var i = 0; i < buffers.Count; i++)
            {
                if (buffers[i].DataBuffer.IsEmpty)
                    continue;

                
                await WriteBufferAsync(buffers[i].DataBuffer, cancellationToken).ConfigureAwait(false);
            }

            // Write padding so the record batch message body length is a multiple of 8 bytes

            var bodyLength = Convert.ToInt32(BaseStream.Position - lengthOffset);
            var bodyPaddingLength = CalculatePadding(bodyLength);

            await WritePaddingAsync(bodyPaddingLength).ConfigureAwait(false);

            return new Block(
                offset: Convert.ToInt32(metadataOffset),
                length: bodyLength + bodyPaddingLength, 
                metadataLength: Convert.ToInt32(metadataLength));
        }

        public virtual Task WriteRecordBatchAsync(RecordBatch recordBatch, CancellationToken cancellationToken = default)
        {
            return WriteRecordBatchInternalAsync(recordBatch, cancellationToken);
        }
    
        public Task WriteBufferAsync(ArrowBuffer arrowBuffer, CancellationToken cancellationToken = default)
        {
            byte[] buffer = null;
            try
            {
                var span = arrowBuffer.Span;
                buffer = ArrayPool<byte>.Shared.Rent(span.Length);
                span.CopyTo(buffer);
                return BaseStream.WriteAsync(buffer, 0, span.Length, cancellationToken);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private protected Offset<Flatbuf.Schema> SerializeSchema(Schema schema)
        {
            // TODO: Serialize schema metadata

            // Build fields

            var fieldOffsets = new Offset<Flatbuf.Field>[schema.Fields.Count];
            var fieldChildren = new List<Offset<Flatbuf.Field>>();

            for (var i = 0; i < fieldOffsets.Length; i++)
            {
                var field = schema.GetFieldByIndex(i);
                var fieldNameOffset = Builder.CreateString(field.Name);
                var fieldType = _fieldTypeBuilder.BuildFieldType(field);

                var fieldChildrenOffsets = Builder.CreateVectorOfTables(fieldChildren.ToArray());

                fieldOffsets[i] = Flatbuf.Field.CreateField(Builder,
                    fieldNameOffset, field.IsNullable, fieldType.Type, fieldType.Offset,
                    default, fieldChildrenOffsets, default);
            }

            var fieldsVectorOffset = Flatbuf.Schema.CreateFieldsVector(Builder, fieldOffsets);

            // Build schema

            var endianness = BitConverter.IsLittleEndian ? Flatbuf.Endianness.Little : Flatbuf.Endianness.Big;

            return Flatbuf.Schema.CreateSchema(
                Builder, endianness, fieldsVectorOffset);
        }


        private async ValueTask<Offset<Flatbuf.Schema>> WriteSchemaAsync(Schema schema, CancellationToken cancellationToken)
        {
            Builder.Clear();

            // Build schema

            var schemaOffset = SerializeSchema(schema);

            // Build message

            await WriteMessageAsync(Flatbuf.MessageHeader.Schema, schemaOffset, 0, cancellationToken)
                .ConfigureAwait(false);

            return schemaOffset;
        }

        private async ValueTask WriteMessageAsync<T>(
            Flatbuf.MessageHeader headerType, Offset<T> headerOffset, int bodyLength,
            CancellationToken cancellationToken)
            where T: struct
        {
            var messageOffset = Flatbuf.Message.CreateMessage(
                Builder, CurrentMetadataVersion, headerType, headerOffset.Value, 
                bodyLength);

            Builder.Finish(messageOffset.Value);

            var messageData = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);
            var messagePaddingLength = CalculatePadding(messageData.Length);

            await Buffers.RentReturnAsync(4, async (buffer) =>
            {
                var metadataSize = messageData.Length + messagePaddingLength;
                BinaryPrimitives.WriteInt32LittleEndian(buffer.Span, metadataSize);
                await BaseStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            }).ConfigureAwait(false);

            await BaseStream.WriteAsync(messageData, cancellationToken).ConfigureAwait(false);
            await WritePaddingAsync(messagePaddingLength).ConfigureAwait(false);
        }

        private protected async ValueTask WriteFlatBufferAsync(CancellationToken cancellationToken = default)
        {
            var segment = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);

            await BaseStream.WriteAsync(segment, cancellationToken).ConfigureAwait(false);
        }

        protected int CalculatePadding(int offset, int alignment = 8) =>
            BitUtility.RoundUpToMultiplePowerOfTwo(offset, alignment) - offset;

        protected Task WritePaddingAsync(int length)
        {
            if (length <= 0) return Task.CompletedTask;

            return BaseStream.WriteAsync(Padding, 0, Math.Min(Padding.Length, length));
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
