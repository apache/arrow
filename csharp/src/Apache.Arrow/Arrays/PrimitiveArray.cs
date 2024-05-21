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
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Apache.Arrow
{
    public abstract class PrimitiveArray<T> : Array, IReadOnlyList<T?>, ICollection<T?>
        where T : struct, IEquatable<T>
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
            return IsValid(index) ? Values[index] : null;
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

        int IReadOnlyCollection<T?>.Count => Length;
        T? IReadOnlyList<T?>.this[int index] => GetValue(index);

        IEnumerator<T?> IEnumerable<T?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return IsValid(index) ? Values[index] : null;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return IsValid(index) ? Values[index] : null;
            }
        }

        int ICollection<T?>.Count => Length;
        bool ICollection<T?>.IsReadOnly => true;
        void ICollection<T?>.Add(T? item) => throw new NotSupportedException("Collection is read-only.");
        bool ICollection<T?>.Remove(T? item) => throw new NotSupportedException("Collection is read-only.");
        void ICollection<T?>.Clear() => throw new NotSupportedException("Collection is read-only.");

        bool ICollection<T?>.Contains(T? item)
        {
            if (item == null)
            {
                return NullCount > 0;
            }

            ReadOnlySpan<T> values = Values;
            while (values.Length > 0)
            {
                int index = Values.IndexOf(item.Value);
                if (index < 0 || IsValid(index)) { return index >= 0; }
                values = values.Slice(index + 1);
            }
            return false;
        }

        void ICollection<T?>.CopyTo(T?[] array, int arrayIndex)
        {
            for (int srcIndex = 0, destIndex = arrayIndex; srcIndex < Length; srcIndex++, destIndex++)
            {
                array[destIndex] = GetValue(srcIndex);
            }
        }
    }
}
