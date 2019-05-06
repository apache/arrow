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
            private readonly int _size;
            private byte[] _buffer;

            public int Capacity => _buffer.Length / _size;
            public int Length { get; private set; }

            public Builder(int capacity = 8)
            {
                _size = Unsafe.SizeOf<T>();
                _buffer = new byte[capacity * _size];

                Length = 0;
            }

            public Builder<T> Append(ArrowBuffer buffer)
            {
                Append(buffer.Span.CastTo<T>());
                return this;
            }

            public Builder<T> Append(T value)
            {
                var span = EnsureCapacity(1);
                span[Length++] = value;
                return this;
            }

            public Builder<T> Append(ReadOnlySpan<T> source)
            {
                var span = EnsureCapacity(source.Length);
                source.CopyTo(span.Slice(Length, source.Length));
                Length += source.Length;
                return this;
            }

            public Builder<T> Append(Func<IEnumerable<T>> fn)
            {
                if (fn != null)
                {
                    AppendRange(fn());
                }

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
                if (capacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(capacity));
                }

                Reallocate(capacity);
                Length = Math.Min(Length, capacity);

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

            private Span<T> EnsureCapacity(int len)
            {
                var targetCapacity = Length + len;

                if (targetCapacity > Capacity)
                {
                    // TODO: specifiable growth strategy

                    var capacity = Math.Max(
                        targetCapacity * _size, _buffer.Length * 2);

                    Reallocate(capacity);
                }

                return Span;
            }

            private void Reallocate(int length)
            {
                if (length < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(length));
                }

                if (length != 0)
                {
                    System.Array.Resize(ref _buffer, length);
                }
            }

            private Memory<byte> Memory => _buffer;

            private Span<T> Span
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Memory.Span.CastTo<T>();
            }
        }

    }
}
