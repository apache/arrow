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
using System.Runtime.InteropServices;

namespace Apache.Arrow
{
    public partial class ArrowBuffer
    {
        /// <summary>
        /// Builds an Arrow buffer from primitive values.
        /// </summary>
        /// <typeparam name="T">Primitive type</typeparam>
        public class Builder<T>
            where T : struct
        {
            private readonly int _size;
            private readonly MemoryPool _pool;
            private Memory<byte> _memory;
            private int _offset;

            public Builder(int initialCapacity = 8, MemoryPool pool = default)
            {
                if (initialCapacity <= 0) initialCapacity = 1;
                if (pool == null) pool = DefaultMemoryPool.Instance.Value;

                _size = Unsafe.SizeOf<T>();
                _pool = pool;
                _memory = _pool.Allocate(initialCapacity * _size);
            }

            public Builder<T> Append(T value)
            {
                var span = GetSpan();

                if (_offset + 1 >= span.Length)
                {
                    // TODO: Consider a specifiable growth strategy

                    _memory = _pool.Reallocate(_memory, (_memory.Length * 3) / 2);
                }

                span[_offset++] = value;
                return this;
            }

            public Builder<T> Set(int index, T value)
            {
                var span = GetSpan();
                span[index] = value;
                return this;
            }

            public Builder<T> Clear()
            {
                var span = GetSpan();
                span.Fill(default);
                return this;
            }

            public ArrowBuffer Build()
            {
                return new ArrowBuffer(_memory, _offset);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private Span<T> GetSpan() => MemoryMarshal.Cast<byte, T>(_memory.Span);
        }
    }
}
