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

using FlatBuffers;
using System;

namespace Apache.Arrow.Ipc
{
    internal sealed class ReadOnlyMemoryBufferAllocator : ByteBufferAllocator
    {
        private readonly ReadOnlyMemory<byte> _buffer;

        public ReadOnlyMemoryBufferAllocator(ReadOnlyMemory<byte> buffer)
        {
            _buffer = buffer;
            Length = buffer.Length;
        }

        public override ReadOnlySpan<byte> ReadOnlySpan => _buffer.Span;
        public override ReadOnlyMemory<byte> ReadOnlyMemory => _buffer;

        // since this is read-only, the following are not supported
        public override Memory<byte> Memory => throw new NotSupportedException();
        public override Span<byte> Span => throw new NotSupportedException();
        public override void GrowFront(int newSize) => throw new NotSupportedException();
    }
}
