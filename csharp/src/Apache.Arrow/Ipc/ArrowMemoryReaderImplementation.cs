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

using Apache.Arrow.Flatbuf;
using FlatBuffers;
using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    internal sealed class ArrowMemoryReaderImplementation : ArrowReaderImplementation
    {
        private readonly ReadOnlyMemory<byte> _buffer;
        private int _bufferPosition;

        public ArrowMemoryReaderImplementation(ReadOnlyMemory<byte> buffer, ICompressionCodecFactory compressionCodecFactory) : base(null, compressionCodecFactory)
        {
            _buffer = buffer;
        }

        public override ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<RecordBatch>(ReadNextRecordBatch());
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            ReadSchema();

            if (_buffer.Length <= _bufferPosition + sizeof(int))
            {
                // reached the end
                return null;
            }

            // Get Length of record batch for message header.
            int messageLength = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Span.Slice(_bufferPosition));
            _bufferPosition += sizeof(int);

            if (messageLength == 0)
            {
                //reached the end
                return null;
            }
            else if (messageLength == MessageSerializer.IpcContinuationToken)
            {
                // ARROW-6313, if the first 4 bytes are continuation message, read the next 4 for the length
                if (_buffer.Length <= _bufferPosition + sizeof(int))
                {
                    throw new InvalidDataException("Corrupted IPC message. Received a continuation token at the end of the message.");
                }

                messageLength = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Span.Slice(_bufferPosition));
                _bufferPosition += sizeof(int);

                if (messageLength == 0)
                {
                    //reached the end
                    return null;
                }
            }

            Message message = Message.GetRootAsMessage(
                CreateByteBuffer(_buffer.Slice(_bufferPosition, messageLength)));
            _bufferPosition += messageLength;

            int bodyLength = (int)message.BodyLength;
            ByteBuffer bodybb = CreateByteBuffer(_buffer.Slice(_bufferPosition, bodyLength));
            _bufferPosition += bodyLength;

            return CreateArrowObjectFromMessage(message, bodybb, memoryOwner: null);
        }

        private void ReadSchema()
        {
            if (HasReadSchema)
            {
                return;
            }

            // Figure out length of schema
            int schemaMessageLength = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Span.Slice(_bufferPosition));
            _bufferPosition += sizeof(int);

            if (schemaMessageLength == MessageSerializer.IpcContinuationToken)
            {
                // ARROW-6313, if the first 4 bytes are continuation message, read the next 4 for the length
                if (_buffer.Length <= _bufferPosition + sizeof(int))
                {
                    throw new InvalidDataException("Corrupted IPC message. Received a continuation token at the end of the message.");
                }

                schemaMessageLength = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Span.Slice(_bufferPosition));
                _bufferPosition += sizeof(int);
            }

            ByteBuffer schemaBuffer = CreateByteBuffer(_buffer.Slice(_bufferPosition));
            Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemaBuffer), ref _dictionaryMemo);
            _bufferPosition += schemaMessageLength;
        }
    }
}
