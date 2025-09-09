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
    public interface ITryCompressionCodec : ICompressionCodec
    {
        /// <summary>
        /// Try to write compressed data to a fixed length memory span
        /// </summary>
        /// <param name="source">The data to compress</param>
        /// <param name="destination">Memory to write compressed data to</param>
        /// <param name="bytesWritten">The number of bytes written to the destination</param>
        /// <returns>true if compression was successful, false if the destination buffer is too small</returns>
        bool TryCompress(ReadOnlyMemory<byte> source, Memory<byte> destination, out int bytesWritten);

    }
}
