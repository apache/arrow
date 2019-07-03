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
        public class Builder<T>
            where T : struct
        {
            private const int DefaultCapacity = 8;

            private readonly int _size;

            public int Capacity => Memory.Length / _size;
            public int Length { get; private set; }
            public Memory<byte> Memory { get; private set; }
            public Span<T> Span
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Memory.Span.CastTo<T>();
            }

            public Builder(int capacity = DefaultCapacity)
            {
                _size = Unsafe.SizeOf<T>();

                Memory = new byte[capacity * _size];
                Length = 0;
            }

            public Builder<T> Append(ArrowBuffer buffer)
            {
                Append(buffer.Span.CastTo<T>());
                return this;
            }

            public Builder<T> Append(T value)
            {
                EnsureCapacity(1);
                Span[Length++] = value;
                return this;
            }

            public Builder<T> Append(ReadOnlySpan<T> source)
            {
                EnsureCapacity(source.Length);
                source.CopyTo(Span.Slice(Length, source.Length));
                Length += source.Length;
                return this;
            }

            public Builder<T> AppendRange(IEnumerable<T> values)
            {
                if (values != null)
                {
                    foreach (var v in values)
                    {
                        Append(v);
                    }
                }

                return this;
            }

            public Builder<T> Reserve(int capacity)
            {
                EnsureCapacity(capacity);
                return this;
            }

            public Builder<T> Resize(int capacity)
            {
                EnsureCapacity(capacity);
                Length = Math.Max(0, capacity);

                return this;
            }

            public Builder<T> Clear()
            {
                Span.Fill(default);
                Length = 0;
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
                    Memory.Slice(0, currentBytesLength).CopyTo(memoryOwner.Memory);
                }

                return new ArrowBuffer(memoryOwner);
            }

            private void EnsureCapacity(int n)
            {
                var length = checked(Length + n);

                if (length > Capacity)
                {
                    // TODO: specifiable growth strategy

                    var capacity = Math.Max(length * _size, Memory.Length * 2);
                    Reallocate(capacity);
                }
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
                    Memory.CopyTo(memory);

                    Memory = memory;
                }
            }

        }

    }
}
