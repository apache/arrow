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
    public abstract class Array : IArrowArray
    {
        public ArrayData Data { get; }

        protected Array(ArrayData data)
        {
            Data = data ?? throw new ArgumentNullException(nameof(data));
        }

        public int Length => Data.Length;

        public int Offset => Data.Offset;

        public int NullCount => Data.NullCount;

        public ArrowBuffer NullBitmapBuffer => Data.Buffers[0];

        public virtual void Accept(IArrowArrayVisitor visitor)
        {
            Accept(this, visitor);
        }

        public bool IsValid(int index) =>
            NullCount == 0 || NullBitmapBuffer.IsEmpty || BitUtility.GetBit(NullBitmapBuffer.Span, index + Offset);

        public bool IsNull(int index) => !IsValid(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Accept<T>(T array, IArrowArrayVisitor visitor)
            where T : class, IArrowArray
        {
            switch (visitor)
            {
                case IArrowArrayVisitor<T> typedVisitor:
                    typedVisitor.Visit(array);
                    break;
                default:
                    visitor.Visit(array);
                    break;
            }
        }

        public Array Slice(int offset, int length)
        {
            if (offset > Length)
            {
                throw new ArgumentException($"Offset {offset} cannot be greater than Length {Length} for Array.Slice");
            }

            length = Math.Min(Data.Length - offset, length);
            offset += Data.Offset;

            ArrayData newData = Data.Slice(offset, length);
            return ArrowArrayFactory.BuildArray(newData) as Array;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Data.Dispose();
            }
        }

        public class Accessor<TArray, TItem> : IEnumerable<TItem>
            where TArray : IArrowArray
        {
            public readonly TArray Array;
            private readonly Func<TArray, int, TItem> _getter;

            public Accessor(TArray array, Func<TArray, int, TItem> getter)
            {
                Array = array;
                _getter = getter;
            }

            public TItem Get(int index) => _getter(Array, index);

            public TItem this[int index]
            {
                get
                {
                    return index < 0 ? Get(Array.Length + index) : Get(index);
                }
                // TODO: Implement setter
                //set
                //{
                //    data[index] = value;
                //}
            }

            public int Length => Array.Length;

            // IEnum methods
            public IEnumerator<TItem> GetEnumerator() => new Enumerator(this);
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            private class Enumerator : IEnumerator<TItem>
            {
                private readonly Accessor<TArray, TItem> _accessor;
                private int _index;

                public Enumerator(Accessor<TArray, TItem> accessor)
                {
                    _accessor = accessor;
                    _index = -1;
                }

                public TItem Current => _accessor.Get(_index);

                object IEnumerator.Current => Current;

                public void Dispose() { }
                public bool MoveNext()
                {
                    _index++;
                    return _index < _accessor.Array.Length;
                }
                public void Reset() => _index = -1;
            }
        }
    }
}
