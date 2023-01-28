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

using Apache.Arrow.Memory;
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
        private readonly bool _leaveOpen;

        public ArrowStreamReaderImplementation(Stream stream, MemoryAllocator allocator, ICompressionCodecFactory compressionCodecFactory, bool leaveOpen) : base(allocator, compressionCodecFactory)
        {
            BaseStream = stream;
            _leaveOpen = leaveOpen;
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

            RecordBatch result = null;

            while (result == null)
            {
                int messageLength = await ReadMessageLengthAsync(throwOnFullRead: false, cancellationToken)
                    .ConfigureAwait(false);

                if (messageLength == 0)
                {
                    // reached end
                    return null;
                }

                await ArrayPool<byte>.Shared.RentReturnAsync(messageLength, async (messageBuff) =>
                {
                    int bytesRead = await BaseStream.ReadFullBufferAsync(messageBuff, cancellationToken)
                        .ConfigureAwait(false);
                    EnsureFullRead(messageBuff, bytesRead);

                    Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(CreateByteBuffer(messageBuff));

                    int bodyLength = checked((int)message.BodyLength);

                    IMemoryOwner<byte> bodyBuffOwner = _allocator.Allocate(bodyLength);
                    Memory<byte> bodyBuff = bodyBuffOwner.Memory.Slice(0, bodyLength);
                    bytesRead = await BaseStream.ReadFullBufferAsync(bodyBuff, cancellationToken)
                        .ConfigureAwait(false);
                    EnsureFullRead(bodyBuff, bytesRead);

                    FlatBuffers.ByteBuffer bodybb = CreateByteBuffer(bodyBuff);
                    result = CreateArrowObjectFromMessage(message, bodybb, bodyBuffOwner);
                }).ConfigureAwait(false);
            }

            return result;
        }

        protected RecordBatch ReadRecordBatch()
        {
            ReadSchema();

            RecordBatch result = null;

            while (result == null)
            {
                int messageLength = ReadMessageLength(throwOnFullRead: false);

                if (messageLength == 0)
                {
                    // reached end
                    return null;
                }

                ArrayPool<byte>.Shared.RentReturn(messageLength, messageBuff =>
                {
                    int bytesRead = BaseStream.ReadFullBuffer(messageBuff);
                    EnsureFullRead(messageBuff, bytesRead);

                    Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(CreateByteBuffer(messageBuff));

                    int bodyLength = checked((int)message.BodyLength);

                    IMemoryOwner<byte> bodyBuffOwner = _allocator.Allocate(bodyLength);
                    Memory<byte> bodyBuff = bodyBuffOwner.Memory.Slice(0, bodyLength);
                    bytesRead = BaseStream.ReadFullBuffer(bodyBuff);
                    EnsureFullRead(bodyBuff, bytesRead);

                    FlatBuffers.ByteBuffer bodybb = CreateByteBuffer(bodyBuff);
                    result = CreateArrowObjectFromMessage(message, bodybb, bodyBuffOwner);
                });
            }

            return result;
        }

        protected virtual async ValueTask ReadSchemaAsync()
        {
            if (HasReadSchema)
            {
                return;
            }

            // Figure out length of schema
            int schemaMessageLength = await ReadMessageLengthAsync(throwOnFullRead: true)
                .ConfigureAwait(false);

            await ArrayPool<byte>.Shared.RentReturnAsync(schemaMessageLength, async (buff) =>
            {
                // Read in schema
                int bytesRead = await BaseStream.ReadFullBufferAsync(buff).ConfigureAwait(false);
                EnsureFullRead(buff, bytesRead);

                FlatBuffers.ByteBuffer schemabb = CreateByteBuffer(buff);
                Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb), ref _dictionaryMemo);
            }).ConfigureAwait(false);
        }

        protected virtual void ReadSchema()
        {
            if (HasReadSchema)
            {
                return;
            }

            // Figure out length of schema
            int schemaMessageLength = ReadMessageLength(throwOnFullRead: true);

            ArrayPool<byte>.Shared.RentReturn(schemaMessageLength, buff =>
            {
                int bytesRead = BaseStream.ReadFullBuffer(buff);
                EnsureFullRead(buff, bytesRead);

                FlatBuffers.ByteBuffer schemabb = CreateByteBuffer(buff);
                Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb), ref _dictionaryMemo);
            });
        }

        private async ValueTask<int> ReadMessageLengthAsync(bool throwOnFullRead, CancellationToken cancellationToken = default)
        {
            int messageLength = 0;
            await ArrayPool<byte>.Shared.RentReturnAsync(4, async (lengthBuffer) =>
            {
                int bytesRead = await BaseStream.ReadFullBufferAsync(lengthBuffer, cancellationToken)
                    .ConfigureAwait(false);
                if (throwOnFullRead)
                {
                    EnsureFullRead(lengthBuffer, bytesRead);
                }
                else if (bytesRead != 4)
                {
                    return;
                }

                messageLength = BitUtility.ReadInt32(lengthBuffer);

                // ARROW-6313, if the first 4 bytes are continuation message, read the next 4 for the length
                if (messageLength == MessageSerializer.IpcContinuationToken)
                {
                    bytesRead = await BaseStream.ReadFullBufferAsync(lengthBuffer, cancellationToken)
                        .ConfigureAwait(false);
                    if (throwOnFullRead)
                    {
                        EnsureFullRead(lengthBuffer, bytesRead);
                    }
                    else if (bytesRead != 4)
                    {
                        messageLength = 0;
                        return;
                    }

                    messageLength = BitUtility.ReadInt32(lengthBuffer);
                }
            }).ConfigureAwait(false);

            return messageLength;
        }

        private int ReadMessageLength(bool throwOnFullRead)
        {
            int messageLength = 0;
            ArrayPool<byte>.Shared.RentReturn(4, lengthBuffer =>
            {
                int bytesRead = BaseStream.ReadFullBuffer(lengthBuffer);
                if (throwOnFullRead)
                {
                    EnsureFullRead(lengthBuffer, bytesRead);
                }
                else if (bytesRead != 4)
                {
                    return;
                }

                messageLength = BitUtility.ReadInt32(lengthBuffer);

                // ARROW-6313, if the first 4 bytes are continuation message, read the next 4 for the length
                if (messageLength == MessageSerializer.IpcContinuationToken)
                {
                    bytesRead = BaseStream.ReadFullBuffer(lengthBuffer);
                    if (throwOnFullRead)
                    {
                        EnsureFullRead(lengthBuffer, bytesRead);
                    }
                    else if (bytesRead != 4)
                    {
                        messageLength = 0;
                        return;
                    }

                    messageLength = BitUtility.ReadInt32(lengthBuffer);
                }
            });

            return messageLength;
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
