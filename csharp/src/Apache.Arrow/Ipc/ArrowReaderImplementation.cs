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

using FlatBuffers;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Ipc
{
    internal abstract class ArrowReaderImplementation : IDisposable
    {
        public Schema Schema { get; protected set; }
        protected bool HasReadSchema => Schema != null;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        public abstract ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken);
        public abstract RecordBatch ReadNextRecordBatch();

        protected static T ReadMessage<T>(ByteBuffer bb)
            where T : struct, IFlatbufferObject
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

        protected RecordBatch CreateArrowObjectFromMessage(
            Flatbuf.Message message, ByteBuffer bodyByteBuffer, IMemoryOwner<byte> memoryOwner)
        {
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
                    List<IArrowArray> arrays = BuildArrays(Schema, bodyByteBuffer, rb);
                    return new RecordBatch(Schema, memoryOwner, arrays, (int)rb.Length);
                default:
                    // NOTE: Skip unsupported message type
                    Debug.WriteLine($"Skipping unsupported message type '{message.HeaderType}'");
                    break;
            }

            return null;
        }

        internal static ByteBuffer CreateByteBuffer(ReadOnlyMemory<byte> buffer)
        {
            return new ByteBuffer(new ReadOnlyMemoryBufferAllocator(buffer), 0);
        }

        private List<IArrowArray> BuildArrays(
            Schema schema,
            ByteBuffer messageBuffer,
            Flatbuf.RecordBatch recordBatchMessage)
        {
            var arrays = new List<IArrowArray>(recordBatchMessage.NodesLength);
            int bufferIndex = 0;

            for (var n = 0; n < recordBatchMessage.NodesLength; n++)
            {
                Field field = schema.GetFieldByIndex(n);
                Flatbuf.FieldNode fieldNode = recordBatchMessage.Nodes(n).GetValueOrDefault();

                ArrayData arrayData = field.DataType.IsFixedPrimitive() ?
                    LoadPrimitiveField(field, fieldNode, recordBatchMessage, messageBuffer, ref bufferIndex) :
                    LoadVariableField(field, fieldNode, recordBatchMessage, messageBuffer, ref bufferIndex);

                arrays.Add(ArrowArrayFactory.BuildArray(arrayData));
            }

            return arrays;
        }

        private ArrayData LoadPrimitiveField(
            Field field,
            Flatbuf.FieldNode fieldNode,
            Flatbuf.RecordBatch recordBatch,
            ByteBuffer bodyData,
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

        private ArrayData LoadVariableField(
            Field field,
            Flatbuf.FieldNode fieldNode,
            Flatbuf.RecordBatch recordBatch,
            ByteBuffer bodyData,
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

        private ArrowBuffer BuildArrowBuffer(ByteBuffer bodyData, Flatbuf.Buffer buffer)
        {
            if (buffer.Length <= 0)
            {
                return ArrowBuffer.Empty;
            }

            int offset = (int)buffer.Offset;
            int length = (int)buffer.Length;

            var data = bodyData.ToReadOnlyMemory(offset, length);
            return new ArrowBuffer(data);
        }
    }
}
