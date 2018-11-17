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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    public class ArrowStreamReader : IArrowReader, IDisposable
    {
        public Schema Schema { get; protected set; }
        public Stream BaseStream { get; }

        protected ArrayPool<byte> Buffers { get; }

        public ArrowStreamReader(Stream stream)
        {
            BaseStream = stream ?? throw new ArgumentNullException(nameof(stream));
            Buffers = ArrayPool<byte>.Create();
            Schema = null;
        }

        protected bool HasReadSchema => Schema != null;

        public virtual async Task<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            // TODO: Loop until a record batch is read.
            cancellationToken.ThrowIfCancellationRequested();
            return await ReadRecordBatchAsync(cancellationToken);
        }

        protected async Task<RecordBatch> ReadRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            await ReadSchemaAsync();

            var bytesRead = 0;

            byte[] lengthBuffer = null;
            byte[] messageBuff = null;
            byte[] bodyBuff = null;

            try
            {
                // Get Length of record batch for message header.

                lengthBuffer = Buffers.Rent(4);
                bytesRead += await BaseStream.ReadAsync(lengthBuffer, 0, 4, cancellationToken);
                var messageLength = BitConverter.ToInt32(lengthBuffer, 0);

                messageBuff = Buffers.Rent(messageLength);
                bytesRead += await BaseStream.ReadAsync(messageBuff, 0, messageLength, cancellationToken);
                var message = Flatbuf.Message.GetRootAsMessage(new FlatBuffers.ByteBuffer(messageBuff));

                bodyBuff = Buffers.Rent((int)message.BodyLength);
                var bodybb = new FlatBuffers.ByteBuffer(bodyBuff);
                bytesRead += await BaseStream.ReadAsync(bodyBuff, 0, (int)message.BodyLength, cancellationToken);

                switch (message.HeaderType)
                {
                    case Flatbuf.MessageHeader.Schema:
                        // TODO: Read schema and verify equality?
                        break;
                    case Flatbuf.MessageHeader.DictionaryBatch:
                        // TODO: not supported currently
                        Debug.WriteLine("Dictionaries are not yet supported.");
                        break;
                    case Flatbuf.MessageHeader.RecordBatch:
                        var rb = message.Header<Flatbuf.RecordBatch>().Value;
                        var arrays = BuildArrays(Schema, bodybb, rb);
                        return new RecordBatch(Schema, arrays, (int)rb.Length);
                    default:
                        // NOTE: Skip unsupported message type
                        Debug.WriteLine($"Skipping unsupported message type '{message.HeaderType}'");
                        break;
                }
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

            return null;
        }

        protected virtual async Task<Schema> ReadSchemaAsync()
        {
            if (HasReadSchema)
            {
                return Schema;
            }

            byte[] buff = null;

            try
            {
                // Figure out length of schema

                buff = Buffers.Rent(4);
                await BaseStream.ReadAsync(buff, 0, 4);
                var schemaMessageLength = BitConverter.ToInt32(buff, 0);
                Buffers.Return(buff);

                // Allocate byte array for schema flat buffer

                buff = Buffers.Rent(schemaMessageLength);
                var schemabb = new FlatBuffers.ByteBuffer(buff);

                // Read in schema

                await BaseStream.ReadAsync(buff, 0, schemaMessageLength);
                Schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb));

                return Schema;
            }
            finally
            {
                if (buff != null)
                {
                    Buffers.Return(buff);
                }
            }
        }

        public void Dispose()
        {
            BaseStream.Dispose();
        }

        #region Static Helper Functions

        protected static IEnumerable<IArrowArray> BuildArrays(Schema schema,
            FlatBuffers.ByteBuffer messageBuffer,
            Flatbuf.RecordBatch recordBatchMessage)
        {
            var arrays = new List<ArrayData>();
            var bufferIndex = 0;

            for (var n = 0; n < recordBatchMessage.NodesLength; n++)
            {
                var field = schema.GetFieldByIndex(n);
                var fieldNode = recordBatchMessage.Nodes(n).GetValueOrDefault();

                if (field.DataType.IsFixedPrimitive())
                    arrays.Add(LoadPrimitiveField(field, fieldNode, recordBatchMessage, messageBuffer, ref bufferIndex));
                else
                    arrays.Add(LoadVariableField(field, fieldNode, recordBatchMessage, messageBuffer, ref bufferIndex));
            }

            return arrays.Select(ArrowArrayFactory.BuildArray);
        }

        protected static T ReadMessage<T>(FlatBuffers.ByteBuffer bb) where T : struct, FlatBuffers.IFlatbufferObject
        {
            var returnType = typeof(T);
            var msg = Flatbuf.Message.GetRootAsMessage(bb);

            if (MatchEnum(msg.HeaderType, returnType))
            {
                return msg.Header<T>().Value;
            }
            else
            {
                throw new Exception($"Requested type '{returnType.Name}' " +
                                    $"did not match type found at offset => '{msg.HeaderType}'");
            }
        }

        private static bool MatchEnum(Flatbuf.MessageHeader messageHeader, Type flatBuffType)
        {
            switch (messageHeader)
            {
                case Flatbuf.MessageHeader.RecordBatch:
                    return flatBuffType == typeof(Flatbuf.RecordBatch);
                case Flatbuf.MessageHeader.DictionaryBatch:
                    return flatBuffType == typeof(Flatbuf.DictionaryBatch);
                case Flatbuf.MessageHeader.Schema:
                    return flatBuffType == typeof(Flatbuf.Schema);
                case Flatbuf.MessageHeader.Tensor:
                    return flatBuffType == typeof(Flatbuf.Tensor);
                case Flatbuf.MessageHeader.NONE:
                    throw new ArgumentException("MessageHeader NONE has no matching flatbuf types", nameof(messageHeader));
                default:
                    throw new ArgumentException($"Unexpected MessageHeader value", nameof(messageHeader));
            }
        }

        private static ArrowBuffer BuildArrowBuffer(FlatBuffers.ByteBuffer bodyData, Flatbuf.Buffer buffer)
        {
            if (buffer.Length <= 0)
            {
                return null;
            }

            var segment = bodyData.ToArraySegment((int)buffer.Offset, (int)buffer.Length);
            return ArrowBuffer.FromMemory(segment);
        }

        private static ArrayData LoadPrimitiveField(Field field,
                                  Flatbuf.FieldNode fieldNode,
                                  Flatbuf.RecordBatch recordBatch,
                                  FlatBuffers.ByteBuffer bodyData,
                                  ref int bufferIndex)
        {
            var nullBitmapBuffer = recordBatch.Buffers(bufferIndex++).GetValueOrDefault();
            var valueBuffer = recordBatch.Buffers(bufferIndex++).GetValueOrDefault();

            ArrowBuffer nullArrowBuffer = BuildArrowBuffer(bodyData, nullBitmapBuffer);
            ArrowBuffer valueArrowBuffer = BuildArrowBuffer(bodyData, valueBuffer);
            
            var fieldLength = (int)fieldNode.Length;
            var fieldNullCount = (int)fieldNode.NullCount;

            if (fieldLength < 0)
            {
                throw new InvalidDataException("Field length must be >= 0"); // TODO:Localize exception message
            }

            if (fieldNullCount < 0)
            {
                throw new InvalidDataException("Null count length must be >= 0"); // TODO:Localize exception message
            }

            var arrowBuff = new[] { nullArrowBuffer, valueArrowBuffer };

            return new ArrayData(field.DataType, fieldLength, fieldNullCount, 0, arrowBuff);
        }

        private static ArrayData LoadVariableField(Field field,
                                  Flatbuf.FieldNode fieldNode,
                          Flatbuf.RecordBatch recordBatch,
                          FlatBuffers.ByteBuffer bodyData,
                          ref int bufferIndex)
        {
            var nullBitmapBuffer = recordBatch.Buffers(bufferIndex++).GetValueOrDefault();
            var offsetBuffer = recordBatch.Buffers(bufferIndex++).GetValueOrDefault();
            var valueBuffer = recordBatch.Buffers(bufferIndex++).GetValueOrDefault();

            ArrowBuffer nullArrowBuffer = BuildArrowBuffer(bodyData, nullBitmapBuffer);
            ArrowBuffer offsetArrowBuffer = BuildArrowBuffer(bodyData, offsetBuffer);
            ArrowBuffer valueArrowBuffer = BuildArrowBuffer(bodyData, valueBuffer);

            var fieldLength = (int)fieldNode.Length;
            var fieldNullCount = (int)fieldNode.NullCount;

            if (fieldLength < 0)
            {
                throw new InvalidDataException("Field length must be >= 0"); // TODO: Localize exception message
            }

            if (fieldNullCount < 0)
            { 
                throw new InvalidDataException("Null count length must be >= 0"); //TODO: Localize exception message
            }

            var arrowBuff = new[] { nullArrowBuffer, offsetArrowBuffer, valueArrowBuffer };

            return new ArrayData(field.DataType, fieldLength, fieldNullCount, 0, arrowBuff);
        }
        #endregion
    }
}
