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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    internal class ArrowStreamReaderImplementation : ArrowReaderImplementation
    {
        public Stream BaseStream { get; }
        protected ArrayPool<byte> Buffers { get; }

        public ArrowStreamReaderImplementation(Stream stream)
        {
            BaseStream = stream;
            Buffers = ArrayPool<byte>.Create();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                BaseStream.Dispose();
            }
        }

        public override async Task<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            // TODO: Loop until a record batch is read.
            cancellationToken.ThrowIfCancellationRequested();
            return await ReadRecordBatchAsync(cancellationToken).ConfigureAwait(false);
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            throw new NotImplementedException();
        }

        protected async Task<RecordBatch> ReadRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            await ReadSchemaAsync().ConfigureAwait(false);

            var bytesRead = 0;

            byte[] lengthBuffer = null;
            byte[] messageBuff = null;
            byte[] bodyBuff = null;

            try
            {
                // Get Length of record batch for message header.

                lengthBuffer = Buffers.Rent(4);
                bytesRead += await BaseStream.ReadAsync(lengthBuffer, 0, 4, cancellationToken)
                    .ConfigureAwait(false);

                if (bytesRead != 4)
                {
                    //reached the end
                    return null;
                }

                var messageLength = BitConverter.ToInt32(lengthBuffer, 0);

                if (messageLength == 0)
                {
                    //reached the end
                    return null;
                }

                messageBuff = Buffers.Rent(messageLength);
                bytesRead += await BaseStream.ReadAsync(messageBuff, 0, messageLength, cancellationToken)
                    .ConfigureAwait(false);
                var message = Flatbuf.Message.GetRootAsMessage(new FlatBuffers.ByteBuffer(messageBuff));

                bodyBuff = Buffers.Rent((int)message.BodyLength);
                var bodybb = new FlatBuffers.ByteBuffer(bodyBuff);
                bytesRead += await BaseStream.ReadAsync(bodyBuff, 0, (int)message.BodyLength, cancellationToken)
                    .ConfigureAwait(false);

                return CreateArrowObjectFromMessage(message, bodybb);
            }
            finally
            {
                if (lengthBuffer != null)
                {
                    Buffers.Return(lengthBuffer);
                }

                if (messageBuff != null)
                {
                    Buffers.Return(messageBuff);
                }

                if (bodyBuff != null)
                {
                    Buffers.Return(bodyBuff);
                }
            }
        }

        protected virtual async Task ReadSchemaAsync()
        {
            if (HasReadSchema)
            {
                return;
            }

            byte[] buff = null;

            try
            {
                // Figure out length of schema

                buff = Buffers.Rent(4);
                await BaseStream.ReadAsync(buff, 0, 4).ConfigureAwait(false);
                var schemaMessageLength = BitConverter.ToInt32(buff, 0);
                Buffers.Return(buff);

                // Allocate byte array for schema flat buffer

                buff = Buffers.Rent(schemaMessageLength);
                var schemabb = new FlatBuffers.ByteBuffer(buff);

                // Read in schema

                await BaseStream.ReadAsync(buff, 0, schemaMessageLength).ConfigureAwait(false);
                Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb));
            }
            finally
            {
                if (buff != null)
                {
                    Buffers.Return(buff);
                }
            }
        }

        protected override ArrowBuffer CreateArrowBuffer(ReadOnlyMemory<byte> data)
        {
            // need to use the Buffer.Builder because we are currently renting the memory to
            // read messages
            return new ArrowBuffer.Builder<byte>(data.Length)
                .Append(data.Span)
                .Build();
        }
    }
}
