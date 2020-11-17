using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Apache.Arrow.Ipc;
using FlatBuffers;

namespace Apache.Arrow.Flight
{
    public static class FlightMessageSerializer
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
            var Schema = MessageSerializer.GetSchema(ArrowReaderImplementation.ReadMessage<Flatbuf.Schema>(schemaBuffer));
            return Schema;
        }

        internal static Schema DecodeSchema(ByteBuffer schemaBuffer)
        {
            var Schema = MessageSerializer.GetSchema(ArrowReaderImplementation.ReadMessage<Flatbuf.Schema>(schemaBuffer));
            return Schema;
        }
    }
}
