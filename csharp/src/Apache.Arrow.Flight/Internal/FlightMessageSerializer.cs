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
using System.Text;
using Apache.Arrow.Ipc;
using Google.FlatBuffers;

namespace Apache.Arrow.Flight
{
    internal static class FlightMessageSerializer
    {
        public static Schema DecodeSchema(ReadOnlyMemory<byte> buffer)
        {
            int bufferPosition = 0;
            int schemaMessageLength = BinaryPrimitives.ReadInt32LittleEndian(buffer.Span.Slice(bufferPosition));
            bufferPosition += sizeof(int);

            if (schemaMessageLength == MessageSerializer.IpcContinuationToken)
            {
                // ARROW-6313, if the first 4 bytes are continuation message, read the next 4 for the length
                if (buffer.Length <= bufferPosition + sizeof(int))
                {
                    throw new InvalidDataException("Corrupted IPC message. Received a continuation token at the end of the message.");
                }

                schemaMessageLength = BinaryPrimitives.ReadInt32LittleEndian(buffer.Span.Slice(bufferPosition));
                bufferPosition += sizeof(int);
            }

            ByteBuffer schemaBuffer = ArrowReaderImplementation.CreateByteBuffer(buffer.Slice(bufferPosition));
            //DictionaryBatch not supported for now
            DictionaryMemo dictionaryMemo = null;
            var schema = MessageSerializer.GetSchema(ArrowReaderImplementation.ReadMessage<Flatbuf.Schema>(schemaBuffer), ref dictionaryMemo);
            return schema;
        }

        internal static Schema DecodeSchema(ByteBuffer schemaBuffer)
        {
            //DictionaryBatch not supported for now
            DictionaryMemo dictionaryMemo = null;
            var schema = MessageSerializer.GetSchema(ArrowReaderImplementation.ReadMessage<Flatbuf.Schema>(schemaBuffer), ref dictionaryMemo);
            return schema;
        }
    }
}
