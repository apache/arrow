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
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow
{
    internal static partial class StreamExtensions
    {
        public static async ValueTask<int> ReadFullBufferAsync(this Stream stream, Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int totalBytesRead = 0;
            do
            {
                int bytesRead = 
                    await stream.ReadAsync(
                        buffer.Slice(totalBytesRead, buffer.Length - totalBytesRead),
                        cancellationToken)
                    .ConfigureAwait(false);

                if (bytesRead == 0)
                {
                    // reached the end of the stream
                    return totalBytesRead;
                }

                totalBytesRead += bytesRead;
            }
            while (totalBytesRead < buffer.Length);

            return totalBytesRead;
        }

        public static int ReadFullBuffer(this Stream stream, Memory<byte> buffer)
        {
            int totalBytesRead = 0;
            do
            {
                int bytesRead = stream.Read(
                    buffer.Slice(totalBytesRead, buffer.Length - totalBytesRead));

                if (bytesRead == 0)
                {
                    // reached the end of the stream
                    return totalBytesRead;
                }

                totalBytesRead += bytesRead;
            }
            while (totalBytesRead < buffer.Length);

            return totalBytesRead;
        }
    }
}
