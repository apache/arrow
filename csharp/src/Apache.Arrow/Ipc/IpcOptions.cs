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

namespace Apache.Arrow.Ipc
{
    public class IpcOptions
    {
        internal static IpcOptions Default { get; } = new IpcOptions();

        /// <summary>
        /// Write the pre-0.15.0 encapsulated IPC message format
        /// consisting of a 4-byte prefix instead of 8 byte.
        /// </summary>
        public bool WriteLegacyIpcFormat { get; set; }

        /// <summary>
        /// The compression codec to use to compress data buffers.
        /// If null (the default value), no compression is used.
        /// </summary>
        public CompressionCodecType? CompressionCodec { get; set; }

        /// <summary>
        /// The compression codec factory used to create compression codecs.
        /// Must be provided if a CompressionCodec is specified.
        /// </summary>
        public ICompressionCodecFactory CompressionCodecFactory { get; set; }

        /// <summary>
        /// Sets the compression level to use for codecs that support this.
        /// </summary>
        public int? CompressionLevel { get; set; }

        public IpcOptions()
        {
        }

        /// <summary>
        /// Gets the number of bytes used in the IPC message prefix.
        /// </summary>
        internal int SizeOfIpcLength => WriteLegacyIpcFormat ? 4 : 8;
    }
}
