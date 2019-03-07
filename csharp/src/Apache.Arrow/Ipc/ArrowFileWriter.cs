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
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    public class ArrowFileWriter: ArrowStreamWriter
    { 
        private bool HasWrittenHeader { get; set; }
        private bool HasWrittenFooter { get; set; }

        private List<Block> RecordBatchBlocks { get; }

        public ArrowFileWriter(Stream stream, Schema schema)
            : base(stream, schema)
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

            HasWrittenHeader = false;
            HasWrittenFooter = false;

            RecordBatchBlocks = new List<Block>();
        }

        public override async Task WriteRecordBatchAsync(RecordBatch recordBatch, CancellationToken cancellationToken = default)
        {
            // TODO: Compare record batch schema

            if (!HasWrittenHeader)
            {
                await WriteHeaderAsync(cancellationToken).ConfigureAwait(false);
                HasWrittenHeader = true;
            }

            cancellationToken.ThrowIfCancellationRequested();

            var block = await WriteRecordBatchInternalAsync(recordBatch, cancellationToken)
                .ConfigureAwait(false);

            RecordBatchBlocks.Add(block);
        }

        public async Task WriteFooterAsync(CancellationToken cancellationToken = default)
        {
            if (!HasWrittenFooter)
            {
                await WriteFooterAsync(Schema, cancellationToken).ConfigureAwait(false);
                HasWrittenFooter = true;
            }

            await BaseStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task WriteHeaderAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Write magic number and empty padding up to the 8-byte boundary

            await WriteMagicAsync().ConfigureAwait(false);
            await WritePaddingAsync(CalculatePadding(ArrowFileConstants.Magic.Length))
                .ConfigureAwait(false);
        }

        private async Task WriteFooterAsync(Schema schema, CancellationToken cancellationToken)
        {
            Builder.Clear();

            var offset = BaseStream.Position;

            // Serialize the schema

            var schemaOffset = SerializeSchema(schema);

            // Serialize all record batches

            Flatbuf.Footer.StartRecordBatchesVector(Builder, RecordBatchBlocks.Count);

            foreach (var recordBatch in RecordBatchBlocks)
            {
                Flatbuf.Block.CreateBlock(
                    Builder, recordBatch.Offset, recordBatch.MetadataLength, recordBatch.Length);
            }

            var recordBatchesVectorOffset = Builder.EndVector();

            // Serialize all dictionaries
            // NOTE: Currently unsupported.

            Flatbuf.Footer.StartDictionariesVector(Builder, 0);

            var dictionaryBatchesOffset = Builder.EndVector();

            // Serialize and write the footer flatbuffer

            var footerOffset = Flatbuf.Footer.CreateFooter(Builder, CurrentMetadataVersion,
                schemaOffset, dictionaryBatchesOffset, recordBatchesVectorOffset);

            Builder.Finish(footerOffset.Value);

            cancellationToken.ThrowIfCancellationRequested();

            await WriteFlatBufferAsync(cancellationToken).ConfigureAwait(false);

            // Write footer length

            cancellationToken.ThrowIfCancellationRequested();

            await Buffers.RentReturnAsync(4, async (buffer) =>
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer,
                    Convert.ToInt32(BaseStream.Position - offset));

                await BaseStream.WriteAsync(buffer, 0, 4, cancellationToken).ConfigureAwait(false);
            }).ConfigureAwait(false);

            // Write magic

            cancellationToken.ThrowIfCancellationRequested();

            await WriteMagicAsync().ConfigureAwait(false);
        }

        private Task WriteMagicAsync()
        {
            return BaseStream.WriteAsync(
                ArrowFileConstants.Magic, 0, ArrowFileConstants.Magic.Length);
        }

    }
}
