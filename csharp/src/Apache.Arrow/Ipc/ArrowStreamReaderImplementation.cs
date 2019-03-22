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
        private readonly bool _leaveOpen;

        public ArrowStreamReaderImplementation(Stream stream, bool leaveOpen)
        {
            BaseStream = stream;
            _leaveOpen = leaveOpen;
            Buffers = ArrayPool<byte>.Create();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_leaveOpen)
            {
                BaseStream.Dispose();
            }
        }

        public override async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            // TODO: Loop until a record batch is read.
            cancellationToken.ThrowIfCancellationRequested();
            return await ReadRecordBatchAsync(cancellationToken).ConfigureAwait(false);
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            return ReadRecordBatch();
        }

        protected async ValueTask<RecordBatch> ReadRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            await ReadSchemaAsync().ConfigureAwait(false);

            int messageLength = 0;
            await Buffers.RentReturnAsync(4, async (lengthBuffer) =>
            {
                // Get Length of record batch for message header.
                int bytesRead = await BaseStream.ReadFullBufferAsync(lengthBuffer, cancellationToken)
                    .ConfigureAwait(false);

                if (bytesRead == 4)
                {
                    messageLength = BitUtility.ReadInt32(lengthBuffer);
                }
            }).ConfigureAwait(false);

            if (messageLength == 0)
            {
                // reached end
                return null;
            }

            RecordBatch result = null;
            await Buffers.RentReturnAsync(messageLength, async (messageBuff) =>
            {
                int bytesRead = await BaseStream.ReadFullBufferAsync(messageBuff, cancellationToken)
                    .ConfigureAwait(false);
                EnsureFullRead(messageBuff, bytesRead);

                Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(CreateByteBuffer(messageBuff));

                await Buffers.RentReturnAsync((int)message.BodyLength, async (bodyBuff) =>
                {
                    int bodyBytesRead = await BaseStream.ReadFullBufferAsync(bodyBuff, cancellationToken)
                        .ConfigureAwait(false);
                    EnsureFullRead(bodyBuff, bodyBytesRead);

                    FlatBuffers.ByteBuffer bodybb = CreateByteBuffer(bodyBuff);
                    result = CreateArrowObjectFromMessage(message, bodybb);
                }).ConfigureAwait(false);
            }).ConfigureAwait(false);

            return result;
        }

        protected RecordBatch ReadRecordBatch()
        {
            ReadSchema();

            int messageLength = 0;
            Buffers.RentReturn(4, lengthBuffer =>
            {
                int bytesRead = BaseStream.ReadFullBuffer(lengthBuffer);

                if (bytesRead == 4)
                {
                    messageLength = BitUtility.ReadInt32(lengthBuffer);
                }
            });

            if (messageLength == 0)
            {
                // reached end
                return null;
            }

            RecordBatch result = null;
            Buffers.RentReturn(messageLength, messageBuff =>
            {
                int bytesRead = BaseStream.ReadFullBuffer(messageBuff);
                EnsureFullRead(messageBuff, bytesRead);

                Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(CreateByteBuffer(messageBuff));

                Buffers.RentReturn((int)message.BodyLength, bodyBuff =>
                {
                    int bodyBytesRead = BaseStream.ReadFullBuffer(bodyBuff);
                    EnsureFullRead(bodyBuff, bodyBytesRead);

                    FlatBuffers.ByteBuffer bodybb = CreateByteBuffer(bodyBuff);
                    result = CreateArrowObjectFromMessage(message, bodybb);
                });
            });

            return result;
        }

        protected virtual async ValueTask ReadSchemaAsync()
        {
            if (HasReadSchema)
            {
                return;
            }

            // Figure out length of schema
            int schemaMessageLength = 0;
            await Buffers.RentReturnAsync(4, async (lengthBuffer) =>
            {
                int bytesRead = await BaseStream.ReadFullBufferAsync(lengthBuffer).ConfigureAwait(false);
                EnsureFullRead(lengthBuffer, bytesRead);

                schemaMessageLength = BitUtility.ReadInt32(lengthBuffer);
            }).ConfigureAwait(false);

            await Buffers.RentReturnAsync(schemaMessageLength, async (buff) =>
            {
                // Read in schema
                int bytesRead = await BaseStream.ReadFullBufferAsync(buff).ConfigureAwait(false);
                EnsureFullRead(buff, bytesRead);

                var schemabb = CreateByteBuffer(buff);
                Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb));
            }).ConfigureAwait(false);
        }

        protected virtual void ReadSchema()
        {
            if (HasReadSchema)
            {
                return;
            }

            // Figure out length of schema
            int schemaMessageLength = 0;
            Buffers.RentReturn(4, lengthBuffer =>
            {
                int bytesRead = BaseStream.ReadFullBuffer(lengthBuffer);
                EnsureFullRead(lengthBuffer, bytesRead);

                schemaMessageLength = BitUtility.ReadInt32(lengthBuffer);
            });

            Buffers.RentReturn(schemaMessageLength, buff =>
            {
                int bytesRead = BaseStream.ReadFullBuffer(buff);
                EnsureFullRead(buff, bytesRead);

                var schemabb = CreateByteBuffer(buff);
                Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb));
            });
        }

        protected override ArrowBuffer CreateArrowBuffer(ReadOnlyMemory<byte> data)
        {
            // need to use the Buffer.Builder because we are currently renting the memory to
            // read messages
            return new ArrowBuffer.Builder<byte>(data.Length)
                .Append(data.Span)
                .Build();
        }

        /// <summary>
        /// Ensures the number of bytes read matches the buffer length
        /// and throws an exception it if doesn't. This ensures we have read
        /// a full buffer from the stream.
        /// </summary>
        internal static void EnsureFullRead(Memory<byte> buffer, int bytesRead)
        {
            if (bytesRead != buffer.Length)
            {
                throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
            }
        }
    }
}
