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
using System.IO;
using Apache.Arrow.Ipc;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;

namespace Apache.Arrow.Compression
{
    internal sealed class Lz4CompressionCodec : ICompressionCodec
    {
        private readonly LZ4EncoderSettings _settings = null;

        public Lz4CompressionCodec(int? compressionLevel = null)
        {
            if (compressionLevel.HasValue)
            {
                if (Enum.IsDefined(typeof(LZ4Level), compressionLevel))
                {
                    _settings = new LZ4EncoderSettings
                    {
                        CompressionLevel = (LZ4Level) compressionLevel,
                    };
                }
                else
                {
                    throw new ArgumentException(
                        $"Invalid LZ4 compression level ({compressionLevel})", nameof(compressionLevel));
                }
            }
        }

        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
        {
            using var decoder = LZ4Frame.Decode(source);
            return decoder.ReadManyBytes(destination.Span);
        }

        public void Compress(ReadOnlyMemory<byte> source, Stream destination)
        {
            using var encoder = LZ4Frame.Encode(destination, _settings, leaveOpen: true);
            encoder.WriteManyBytes(source.Span);
        }

        public void Dispose()
        {
        }
    }
}
