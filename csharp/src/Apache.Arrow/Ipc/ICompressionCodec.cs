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

namespace Apache.Arrow.Ipc
{
    /// <summary>
    /// Codec for data compression and decompression (currently only decompression is supported)
    /// </summary>
    public interface ICompressionCodec : IDisposable
    {
        /// <summary>
        /// Decompresses a compressed data buffer
        /// </summary>
        /// <param name="source">Data buffer to read compressed data from</param>
        /// <param name="destination">Data buffer to write decompressed data to</param>
        /// <returns>The number of decompressed bytes written into the destination</returns>
        int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination);
    }
}
