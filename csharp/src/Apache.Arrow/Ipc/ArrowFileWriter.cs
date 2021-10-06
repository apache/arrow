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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    public class ArrowFileWriter: ArrowStreamWriter
    {
        private long _currentRecordBatchOffset = -1;

        private List<Block> RecordBatchBlocks { get; }

        public ArrowFileWriter(Stream stream, Schema schema)
            : this(stream, schema, leaveOpen: false)
        {
        }

        public ArrowFileWriter(Stream stream, Schema schema, bool leaveOpen)
            : this(stream, schema, leaveOpen, options: null)
        {
        }

        public ArrowFileWriter(Stream stream, Schema schema, bool leaveOpen, IpcOptions options)
            : base(stream, schema, leaveOpen, options)
        {
            if (!stream.CanWrite)
            {
                throw new ArgumentException("stream must be writable", nameof(stream));
            }

            // TODO: Remove seek requirement

            if (!stream.CanSeek)
            {
                throw new ArgumentException("stream must be seekable", nameof(stream));
            }

            RecordBatchBlocks = new List<Block>();
        }

        public override void WriteRecordBatch(RecordBatch recordBatch)
        {
            // TODO: Compare record batch schema

            WriteStart();

            WriteRecordBatchInternal(recordBatch);
        }

        public override async Task WriteRecordBatchAsync(RecordBatch recordBatch, CancellationToken cancellationToken = default)
        {
            // TODO: Compare record batch schema

            await WriteStartAsync(cancellationToken).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();

            await WriteRecordBatchInternalAsync(recordBatch, cancellationToken)
                .ConfigureAwait(false);
        }

        private protected override void StartingWritingRecordBatch()
        {
            _currentRecordBatchOffset = BaseStream.Position;
        }

        private protected override void FinishedWritingRecordBatch(long bodyLength, long metadataLength)
        {
            // Record batches only appear after a Schema is written, so the record batch offsets must
            // always be greater than 0.
            Debug.Assert(_currentRecordBatchOffset > 0, "_currentRecordBatchOffset must be positive.");

            int metadataLengthInt = checked((int)metadataLength);

            Debug.Assert(BitUtility.IsMultipleOf8(_currentRecordBatchOffset));
            Debug.Assert(BitUtility.IsMultipleOf8(metadataLengthInt));
            Debug.Assert(BitUtility.IsMultipleOf8(bodyLength));

            var block = new Block(
                offset: _currentRecordBatchOffset,
                length: bodyLength,
                metadataLength: metadataLengthInt);

            RecordBatchBlocks.Add(block);

            _currentRecordBatchOffset = -1;
        }

        private protected override void WriteEndInternal()
        {
            base.WriteEndInternal();

            WriteFooter(Schema);
        }

        private protected override async ValueTask WriteEndInternalAsync(CancellationToken cancellationToken)
        {
            await base.WriteEndInternalAsync(cancellationToken);

            await WriteFooterAsync(Schema, cancellationToken);
        }

        private protected override void WriteStartInternal()
        {
            // Write magic number and empty padding up to the 8-byte boundary

            WriteMagic();
            WritePadding(CalculatePadding(ArrowFileConstants.Magic.Length));
        }

        private protected async override ValueTask WriteStartInternalAsync(CancellationToken cancellationToken)
        {
            // Write magic number and empty padding up to the 8-byte boundary

            await WriteMagicAsync(cancellationToken).ConfigureAwait(false);
            await WritePaddingAsync(CalculatePadding(ArrowFileConstants.Magic.Length))
                .ConfigureAwait(false);
        }

        private void WriteFooter(Schema schema)
        {
            Builder.Clear();

            long offset = BaseStream.Position;

            // Serialize the schema

            FlatBuffers.Offset<Flatbuf.Schema> schemaOffset = SerializeSchema(schema);

            // Serialize all record batches

            Flatbuf.Footer.StartRecordBatchesVector(Builder, RecordBatchBlocks.Count);

            // flatbuffer struct vectors have to be created in reverse order
            for (int i = RecordBatchBlocks.Count - 1; i >= 0; i--)
            {
                Block recordBatch = RecordBatchBlocks[i];
                Flatbuf.Block.CreateBlock(
                    Builder, recordBatch.Offset, recordBatch.MetadataLength, recordBatch.BodyLength);
            }

            FlatBuffers.VectorOffset recordBatchesVectorOffset = Builder.EndVector();

            // Serialize all dictionaries
            // NOTE: Currently unsupported.

            Flatbuf.Footer.StartDictionariesVector(Builder, 0);

            FlatBuffers.VectorOffset dictionaryBatchesOffset = Builder.EndVector();

            // Serialize and write the footer flatbuffer

            FlatBuffers.Offset<Flatbuf.Footer> footerOffset = Flatbuf.Footer.CreateFooter(Builder, CurrentMetadataVersion,
                schemaOffset, dictionaryBatchesOffset, recordBatchesVectorOffset);

            Builder.Finish(footerOffset.Value);

            WriteFlatBuffer();

            // Write footer length

            Buffers.RentReturn(4, (buffer) =>
            {
                int footerLength;
                checked
                {
                    footerLength = (int)(BaseStream.Position - offset);
                }

                BinaryPrimitives.WriteInt32LittleEndian(buffer.Span, footerLength);

                BaseStream.Write(buffer);
            });

            // Write magic

            WriteMagic();
        }

        private async Task WriteFooterAsync(Schema schema, CancellationToken cancellationToken)
        {
            Builder.Clear();

            long offset = BaseStream.Position;

            // Serialize the schema

            FlatBuffers.Offset<Flatbuf.Schema> schemaOffset = SerializeSchema(schema);

            // Serialize all record batches

            Flatbuf.Footer.StartRecordBatchesVector(Builder, RecordBatchBlocks.Count);

            // flatbuffer struct vectors have to be created in reverse order
            for (int i = RecordBatchBlocks.Count - 1; i >= 0; i--)
            {
                Block recordBatch = RecordBatchBlocks[i];
                Flatbuf.Block.CreateBlock(
                    Builder, recordBatch.Offset, recordBatch.MetadataLength, recordBatch.BodyLength);
            }

            FlatBuffers.VectorOffset recordBatchesVectorOffset = Builder.EndVector();

            // Serialize all dictionaries
            // NOTE: Currently unsupported.

            Flatbuf.Footer.StartDictionariesVector(Builder, 0);

            FlatBuffers.VectorOffset dictionaryBatchesOffset = Builder.EndVector();

            // Serialize and write the footer flatbuffer

            FlatBuffers.Offset<Flatbuf.Footer> footerOffset = Flatbuf.Footer.CreateFooter(Builder, CurrentMetadataVersion,
                schemaOffset, dictionaryBatchesOffset, recordBatchesVectorOffset);

            Builder.Finish(footerOffset.Value);

            cancellationToken.ThrowIfCancellationRequested();

            await WriteFlatBufferAsync(cancellationToken).ConfigureAwait(false);

            // Write footer length

            cancellationToken.ThrowIfCancellationRequested();

            await Buffers.RentReturnAsync(4, async (buffer) =>
            {
                int footerLength;
                checked
                {
                    footerLength = (int)(BaseStream.Position - offset);
                }

                BinaryPrimitives.WriteInt32LittleEndian(buffer.Span, footerLength);

                await BaseStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            }).ConfigureAwait(false);

            // Write magic

            await WriteMagicAsync(cancellationToken).ConfigureAwait(false);
        }

        private void WriteMagic()
        {
            BaseStream.Write(ArrowFileConstants.Magic);
        }

        private ValueTask WriteMagicAsync(CancellationToken cancellationToken)
        {
            return BaseStream.WriteAsync(ArrowFileConstants.Magic, cancellationToken);
        }
    }
}
