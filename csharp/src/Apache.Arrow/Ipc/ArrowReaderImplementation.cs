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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Types;
using Apache.Arrow.Memory;
using Google.FlatBuffers;
using Type = System.Type;

namespace Apache.Arrow.Ipc
{
    internal abstract class ArrowReaderImplementation : IDisposable
    {
        public Schema Schema { get; protected set; }
        protected bool HasReadSchema => Schema != null;

        private protected DictionaryMemo _dictionaryMemo;
        private protected DictionaryMemo DictionaryMemo => _dictionaryMemo ??= new DictionaryMemo();
        private protected readonly MemoryAllocator _allocator;
        private readonly ICompressionCodecFactory _compressionCodecFactory;

        private protected ArrowReaderImplementation() : this(null, null)
        { }

        private protected ArrowReaderImplementation(MemoryAllocator allocator, ICompressionCodecFactory compressionCodecFactory)
        {
            _allocator = allocator ?? MemoryAllocator.Default.Value;
            _compressionCodecFactory = compressionCodecFactory;
        }

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

        internal static T ReadMessage<T>(ByteBuffer bb)
            where T : struct, IFlatbufferObject
        {
            Type returnType = typeof(T);
            Flatbuf.Message msg = Flatbuf.Message.GetRootAsMessage(bb);

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

        /// <summary>
        /// Create a record batch or dictionary batch from Flatbuf.Message.
        /// </summary>
        /// <remarks>
        /// This method adds data to _dictionaryMemo and returns null when the message type is DictionaryBatch.
        /// </remarks>>
        /// <returns>
        /// The record batch when the message type is RecordBatch.
        /// Null when the message type is not RecordBatch.
        /// </returns>
        protected RecordBatch CreateArrowObjectFromMessage(
            Flatbuf.Message message, ByteBuffer bodyByteBuffer, IMemoryOwner<byte> memoryOwner)
        {
            switch (message.HeaderType)
            {
                case Flatbuf.MessageHeader.Schema:
                    // TODO: Read schema and verify equality?
                    break;
                case Flatbuf.MessageHeader.DictionaryBatch:
                    Flatbuf.DictionaryBatch dictionaryBatch = message.Header<Flatbuf.DictionaryBatch>().Value;
                    ReadDictionaryBatch(message.Version, dictionaryBatch, bodyByteBuffer, memoryOwner);
                    break;
                case Flatbuf.MessageHeader.RecordBatch:
                    Flatbuf.RecordBatch rb = message.Header<Flatbuf.RecordBatch>().Value;
                    List<IArrowArray> arrays = BuildArrays(message.Version, Schema, bodyByteBuffer, rb);
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

        private void ReadDictionaryBatch(
            MetadataVersion version,
            Flatbuf.DictionaryBatch dictionaryBatch,
            ByteBuffer bodyByteBuffer,
            IMemoryOwner<byte> memoryOwner)
        {
            long id = dictionaryBatch.Id;
            IArrowType valueType = DictionaryMemo.GetDictionaryType(id);
            Flatbuf.RecordBatch? recordBatch = dictionaryBatch.Data;

            if (!recordBatch.HasValue)
            {
                throw new InvalidDataException("Dictionary must contain RecordBatch");
            }

            Field valueField = new Field("dummy", valueType, true);
            var schema = new Schema(new[] { valueField }, default);
            IList<IArrowArray> arrays = BuildArrays(version, schema, bodyByteBuffer, recordBatch.Value);

            if (arrays.Count != 1)
            {
                throw new InvalidDataException("Dictionary record batch must contain only one field");
            }

            if (dictionaryBatch.IsDelta)
            {
                DictionaryMemo.AddDeltaDictionary(id, arrays[0], _allocator);
            }
            else
            {
                DictionaryMemo.AddOrReplaceDictionary(id, arrays[0]);
            }
        }

        private List<IArrowArray> BuildArrays(
            MetadataVersion version,
            Schema schema,
            ByteBuffer messageBuffer,
            Flatbuf.RecordBatch recordBatchMessage)
        {
            var arrays = new List<IArrowArray>(recordBatchMessage.NodesLength);

            if (recordBatchMessage.NodesLength == 0)
            {
                return arrays;
            }

            using var bufferCreator = GetBufferCreator(recordBatchMessage.Compression);
            var recordBatchEnumerator = new RecordBatchEnumerator(in recordBatchMessage);
            int schemaFieldIndex = 0;
            do
            {
                Field field = schema.GetFieldByIndex(schemaFieldIndex++);
                Flatbuf.FieldNode fieldNode = recordBatchEnumerator.CurrentNode;

                ArrayData arrayData = LoadField(version, ref recordBatchEnumerator, field, in fieldNode, messageBuffer, bufferCreator);

                arrays.Add(ArrowArrayFactory.BuildArray(arrayData));
            } while (recordBatchEnumerator.MoveNextNode());

            return arrays;
        }

        private IBufferCreator GetBufferCreator(BodyCompression? compression)
        {
            if (!compression.HasValue)
            {
                return NoOpBufferCreator.Instance;
            }

            var method = compression.Value.Method;
            if (method != BodyCompressionMethod.BUFFER)
            {
                throw new NotImplementedException($"Compression method {method} is not supported");
            }

            var codec = compression.Value.Codec;
            if (_compressionCodecFactory == null)
            {
                throw new Exception(
                    $"Body is compressed with codec {codec} but no {nameof(ICompressionCodecFactory)} has been configured to decompress buffers");
            }
            var decompressor = codec switch
            {
                Apache.Arrow.Flatbuf.CompressionType.LZ4_FRAME => _compressionCodecFactory.CreateCodec(CompressionCodecType.Lz4Frame),
                Apache.Arrow.Flatbuf.CompressionType.ZSTD => _compressionCodecFactory.CreateCodec(CompressionCodecType.Zstd),
                _ => throw new NotImplementedException($"Compression codec {codec} is not supported")
            };
            return new DecompressingBufferCreator(decompressor, _allocator);
        }

        private ArrayData LoadField(
            MetadataVersion version,
            ref RecordBatchEnumerator recordBatchEnumerator,
            Field field,
            in Flatbuf.FieldNode fieldNode,
            ByteBuffer bodyData,
            IBufferCreator bufferCreator)
        {

            int fieldLength = (int)fieldNode.Length;
            int fieldNullCount = (int)fieldNode.NullCount;

            if (fieldLength < 0)
            {
                throw new InvalidDataException("Field length must be >= 0"); // TODO:Localize exception message
            }

            if (fieldNullCount < 0)
            {
                throw new InvalidDataException("Null count length must be >= 0"); // TODO:Localize exception message
            }

            int buffers;
            switch (field.DataType.TypeId)
            {
                case ArrowTypeId.Null:
                    return new ArrayData(field.DataType, fieldLength, fieldNullCount, 0, System.Array.Empty<ArrowBuffer>());
                case ArrowTypeId.Union:
                    if (version < MetadataVersion.V5)
                    {
                        if (fieldNullCount > 0)
                        {
                            if (recordBatchEnumerator.CurrentBuffer.Length > 0)
                            {
                                // With older metadata we can get a validity bitmap. Fixing up union data is hard,
                                // so we will just quit.
                                throw new NotSupportedException("Cannot read pre-1.0.0 Union array with top-level validity bitmap");
                            }
                        }
                        recordBatchEnumerator.MoveNextBuffer();
                    }
                    buffers = ((UnionType)field.DataType).Mode == Types.UnionMode.Dense ? 2 : 1;
                    break;
                case ArrowTypeId.Struct:
                case ArrowTypeId.FixedSizeList:
                    buffers = 1;
                    break;
                case ArrowTypeId.String:
                case ArrowTypeId.Binary:
                case ArrowTypeId.ListView:
                    buffers = 3;
                    break;
                case ArrowTypeId.StringView:
                case ArrowTypeId.BinaryView:
                    buffers = checked((int)(2 + recordBatchEnumerator.CurrentVariadicCount));
                    recordBatchEnumerator.MoveNextVariadicCount();
                    break;
                default:
                    buffers = 2;
                    break;
            }

            ArrowBuffer[] arrowBuff = new ArrowBuffer[buffers];
            for (int i = 0; i < buffers; i++)
            {
                arrowBuff[i] = BuildArrowBuffer(bodyData, recordBatchEnumerator.CurrentBuffer, bufferCreator);
                recordBatchEnumerator.MoveNextBuffer();
            }

            ArrayData[] children = GetChildren(version, ref recordBatchEnumerator, field, bodyData, bufferCreator);

            IArrowArray dictionary = null;
            if (field.DataType.TypeId == ArrowTypeId.Dictionary)
            {
                long id = DictionaryMemo.GetId(field);
                dictionary = DictionaryMemo.GetDictionary(id);
            }

            return new ArrayData(field.DataType, fieldLength, fieldNullCount, 0, arrowBuff, children, dictionary?.Data);
        }

        private ArrayData[] GetChildren(
            MetadataVersion version,
            ref RecordBatchEnumerator recordBatchEnumerator,
            Field field,
            ByteBuffer bodyData,
            IBufferCreator bufferCreator)
        {
            if (!(field.DataType is NestedType type)) return null;

            int childrenCount = type.Fields.Count;
            var children = new ArrayData[childrenCount];
            for (int index = 0; index < childrenCount; index++)
            {
                recordBatchEnumerator.MoveNextNode();
                Flatbuf.FieldNode childFieldNode = recordBatchEnumerator.CurrentNode;

                Field childField = type.Fields[index];
                children[index] = LoadField(version, ref recordBatchEnumerator, childField, in childFieldNode, bodyData, bufferCreator);
            }
            return children;
        }

        private ArrowBuffer BuildArrowBuffer(ByteBuffer bodyData, Flatbuf.Buffer buffer, IBufferCreator bufferCreator)
        {
            if (buffer.Length <= 0)
            {
                return ArrowBuffer.Empty;
            }

            int offset = (int)buffer.Offset;
            int length = (int)buffer.Length;

            var data = bodyData.ToReadOnlyMemory(offset, length);
            return bufferCreator.CreateBuffer(data);
        }
    }

    internal struct RecordBatchEnumerator
    {
        private Flatbuf.RecordBatch RecordBatch { get; }
        internal int CurrentBufferIndex { get; private set; }
        internal int CurrentNodeIndex { get; private set; }
        internal int CurrentVariadicCountIndex { get; private set; }

        internal Flatbuf.Buffer CurrentBuffer => RecordBatch.Buffers(CurrentBufferIndex).GetValueOrDefault();

        internal Flatbuf.FieldNode CurrentNode => RecordBatch.Nodes(CurrentNodeIndex).GetValueOrDefault();

        internal long CurrentVariadicCount => RecordBatch.VariadicBufferCounts(CurrentVariadicCountIndex);

        internal bool MoveNextBuffer()
        {
            return ++CurrentBufferIndex < RecordBatch.BuffersLength;
        }

        internal bool MoveNextNode()
        {
            return ++CurrentNodeIndex < RecordBatch.NodesLength;
        }

        internal bool MoveNextVariadicCount()
        {
            return ++CurrentVariadicCountIndex < RecordBatch.VariadicBufferCountsLength;
        }

        internal RecordBatchEnumerator(in Flatbuf.RecordBatch recordBatch)
        {
            RecordBatch = recordBatch;
            CurrentBufferIndex = 0;
            CurrentNodeIndex = 0;
            CurrentVariadicCountIndex = 0;
        }
    }
}
