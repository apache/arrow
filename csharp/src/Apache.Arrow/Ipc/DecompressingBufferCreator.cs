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
using Apache.Arrow.Memory;

namespace Apache.Arrow.Ipc
{
    /// <summary>
    /// Creates Arrow buffers from compressed data
    /// </summary>
    internal sealed class DecompressingBufferCreator : IBufferCreator
    {
        private readonly ICompressionCodec _compressionCodec;
        private readonly MemoryAllocator _allocator;

        public DecompressingBufferCreator(ICompressionCodec compressionCodec, MemoryAllocator allocator)
        {
            _compressionCodec = compressionCodec;
            _allocator = allocator;
        }

        public ArrowBuffer CreateBuffer(ReadOnlyMemory<byte> source)
        {
            // See the BodyCompressionMethod enum in format/Message.fbs
            // for documentation on the Buffer compression method used here.

            if (source.Length < 8)
            {
                throw new Exception($"Invalid compressed data buffer size ({source.Length}), expected at least 8 bytes");
            }

            // First 8 bytes give the uncompressed data length
            var uncompressedLength = BinaryPrimitives.ReadInt64LittleEndian(source.Span.Slice(0, 8));
            if (uncompressedLength == -1)
            {
                // The buffer is not actually compressed
                return new ArrowBuffer(source.Slice(8));
            }

            var outputData = _allocator.Allocate(Convert.ToInt32(uncompressedLength));
            var decompressedLength = _compressionCodec.Decompress(source.Slice(8), outputData.Memory);
            if (decompressedLength != uncompressedLength)
            {
                throw new Exception($"Expected to decompress {uncompressedLength} bytes, but got {decompressedLength} bytes");
            }

            return new ArrowBuffer(outputData);
        }

        public void Dispose()
        {
            _compressionCodec.Dispose();
        }
    }
}
