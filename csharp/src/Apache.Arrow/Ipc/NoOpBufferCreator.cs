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
    /// Creates Arrow buffers from uncompressed data
    /// </summary>
    internal sealed class NoOpBufferCreator : IBufferCreator
    {
        /// <summary>
        /// Singleton instance, used as this class doesn't need to be disposed and has no state
        /// </summary>
        public static readonly NoOpBufferCreator Instance = new NoOpBufferCreator();

        public ArrowBuffer CreateBuffer(ReadOnlyMemory<byte> source)
        {
            return new ArrowBuffer(source);
        }

        public void Dispose()
        {
        }
    }
}
