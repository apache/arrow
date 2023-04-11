﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Apache.Arrow
{
    public abstract class PrimitiveArray<T> : Array, IEnumerable<T?>
        where T : struct
    {
        protected PrimitiveArray(ArrayData data)
            : base(data)
        {
            data.EnsureBufferCount(2);
        }

        public ArrowBuffer ValueBuffer => Data.Buffers[1];

        public ReadOnlySpan<T> Values => ValueBuffer.Span.CastTo<T>().Slice(Offset, Length);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T? GetValue(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            return IsValid(index) ? Values[index] : (T?)null;
        }

        public IList<T?> ToList(bool includeNulls = false)
        {
            ReadOnlySpan<T> span = Values;
            var list = new List<T?>(span.Length);

            for (int i = 0; i < span.Length; i++)
            {
                T? value = GetValue(i);

                if (value.HasValue)
                {
                    list.Add(value.Value);
                }
                else
                {
                    if (includeNulls)
                    {
                        list.Add(null);
                    }
                }
            }

            return list;
        }

        // IEnumerable methods
        public IEnumerator<T?> GetEnumerator()
        {
            return new Enumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private class Enumerator : IEnumerator<T?>
        {
            private int Position;
            private PrimitiveArray<T> Array;

            public Enumerator(PrimitiveArray<T> array)
            {
                Array = array;
                Position = -1;
            }

            T? IEnumerator<T?>.Current => Array.GetValue(Position);

            object IEnumerator.Current => Array.GetValue(Position);

            public bool MoveNext()
            {
                Position++;
                return (Position < Array.Length);
            }

            public void Reset() => Position = -1;

            public void Dispose() { }
        }
    }
}
