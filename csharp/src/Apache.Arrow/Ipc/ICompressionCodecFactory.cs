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
    /// <summary>
    /// Provides compression codec implementations for different compression codecs
    /// </summary>
    public interface ICompressionCodecFactory
    {
        /// <summary>
        /// Create a new compression codec
        /// </summary>
        /// <param name="compressionCodecType">The type of codec to create</param>
        /// <returns>The created codec</returns>
        ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType);

        /// <summary>
        /// Create a new compression codec with a specified compression level
        /// </summary>
        /// <param name="compressionCodecType">The type of codec to create</param>
        /// <param name="compressionLevel">The compression level to use when compressing data</param>
        /// <returns>The created codec</returns>
        ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int? compressionLevel)
#if NET6_0_OR_GREATER
        {
            // Default implementation ignores the compression level
            return CreateCodec(compressionCodecType);
        }
#else
        ;
#endif
    }
}
