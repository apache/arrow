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

using Apache.Arrow.Memory;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Apache.Arrow
{
    public partial struct ArrowBuffer
    {
        public partial class Builder<T>
            where T : struct
        {

            public Builder<T> Append(ReadOnlySpan<T> source)
            {
                EnsureCapacity(source.Length);
                source.CopyToFix(Span.Slice(Length, source.Length));
                Length += source.Length;
                return this;
            }

            public ArrowBuffer Build(MemoryAllocator allocator = default)
            {
                int currentBytesLength = Length * _size;
                int bufferLength = checked((int)BitUtility.RoundUpToMultipleOf64(currentBytesLength));

                var memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
                var memoryOwner = memoryAllocator.Allocate(bufferLength);

                if (memoryOwner != null)
                {
                    Memory.Slice(0, currentBytesLength).CopyToFix(memoryOwner.Memory);
                }

                return new ArrowBuffer(memoryOwner);
            }

            private void Reallocate(int length)
            {
                if (length < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(length));
                }

                if (length != 0)
                {
                    var memory = new Memory<byte>(new byte[length]);
                    Memory.CopyToFix(memory);

                    Memory = memory;
                }
            }
        }
    }
}