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
using ZstdSharp;

namespace Apache.Arrow.Compression
{
    internal sealed class ZstdCompressionCodec : ICompressionCodec
    {
        private readonly Decompressor _decompressor;
        private readonly Compressor _compressor;

        public ZstdCompressionCodec(int? compressionLevel = null)
        {
            if (compressionLevel.HasValue &&
                (compressionLevel.Value < Compressor.MinCompressionLevel ||
                 compressionLevel.Value > Compressor.MaxCompressionLevel))
            {
                throw new ArgumentException(
                    $"Zstd compression level must be between {Compressor.MinCompressionLevel} and {Compressor.MaxCompressionLevel}",
                    nameof(compressionLevel));
            }

            _decompressor = new Decompressor();
            _compressor = new Compressor(compressionLevel ?? Compressor.DefaultCompressionLevel);
        }

        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
        {
            return _decompressor.Unwrap(source.Span, destination.Span);
        }

        public void Compress(ReadOnlyMemory<byte> source, Stream destination)
        {
            using var compressor = new CompressionStream(
                destination, _compressor, preserveCompressor: true, leaveOpen: true);
            compressor.Write(source.Span);
        }

        public void Dispose()
        {
            _decompressor.Dispose();
            _compressor.Dispose();
        }
    }
}
