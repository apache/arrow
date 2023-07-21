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
using System.Linq;

namespace Apache.Arrow
{
    public abstract class PrimitiveArrayBuilder<TFrom, TTo, TArray, TBuilder> : IArrowArrayBuilder<TArray, TBuilder>
        where TTo : struct
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray>
    {
        protected TBuilder Instance => this as TBuilder;
        protected IArrowArrayBuilder<TTo, TArray, IArrowArrayBuilder<TArray>> ArrayBuilder { get; }

        public int Length => ArrayBuilder.Length;

        internal PrimitiveArrayBuilder(IArrowArrayBuilder<TTo, TArray, IArrowArrayBuilder<TArray>> builder)
        {
            ArrayBuilder = builder ?? throw new ArgumentNullException(nameof(builder));
        }

        public TArray Build(MemoryAllocator allocator = default) => ArrayBuilder.Build(allocator);

        public TBuilder Append(TFrom value)
        {
            ArrayBuilder.Append(ConvertTo(value));
            return Instance;
        }

        public TBuilder Append(ReadOnlySpan<TFrom> span)
        {
            ArrayBuilder.Reserve(span.Length);
            foreach (TFrom value in span)
            {
                Append(value);
            }
            return Instance;
        }

        public TBuilder AppendRange(IEnumerable<TFrom> values)
        {
            ArrayBuilder.AppendRange(values.Select(ConvertTo));
            return Instance;
        }

        public TBuilder AppendNull()
        {
            ArrayBuilder.AppendNull();
            return Instance;
        }

        public TBuilder Reserve(int capacity)
        {
            ArrayBuilder.Reserve(capacity);
            return Instance;
        }

        public TBuilder Resize(int length)
        {
            ArrayBuilder.Resize(length);
            return Instance;
        }

        public TBuilder Swap(int i, int j)
        {
            ArrayBuilder.Swap(i, j);
            return Instance;
        }

        public TBuilder Set(int index, TFrom value)
        {
            ArrayBuilder.Set(index, ConvertTo(value));
            return Instance;
        }

        public TBuilder Clear()
        {
            ArrayBuilder.Clear();
            return Instance;
        }

        protected abstract TTo ConvertTo(TFrom value);
    }

    public abstract class PrimitiveArrayBuilder<T, TArray, TBuilder> : IArrowArrayBuilder<T, TArray, TBuilder>
        where T : struct
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray>
    {
        protected TBuilder Instance => this as TBuilder;
        protected ArrowBuffer.Builder<T> ValueBuffer { get; }
        protected ArrowBuffer.BitmapBuilder ValidityBuffer { get; }

        public int Length => ValueBuffer.Length;
        protected int NullCount => ValidityBuffer.UnsetBitCount;

        internal PrimitiveArrayBuilder()
        {
            ValueBuffer = new ArrowBuffer.Builder<T>();
            ValidityBuffer = new ArrowBuffer.BitmapBuilder();
        }

        public TBuilder Resize(int length)
        {
            ValueBuffer.Resize(length);
            ValidityBuffer.Resize(length);
            return Instance;
        }

        public TBuilder Reserve(int capacity)
        {
            ValueBuffer.Reserve(capacity);
            ValidityBuffer.Reserve(capacity);
            return Instance;
        }

        public TBuilder Append(T value)
        {
            ValueBuffer.Append(value);
            ValidityBuffer.Append(true);
            return Instance;
        }

        public TBuilder Append(ReadOnlySpan<T> span)
        {
            int len = ValueBuffer.Length;
            ValueBuffer.Append(span);
            int additionalBitsCount = ValueBuffer.Length - len;
            ValidityBuffer.AppendRange(true, additionalBitsCount);
            return Instance;
        }

        public TBuilder AppendRange(IEnumerable<T> values)
        {
            int len = ValueBuffer.Length;
            ValueBuffer.AppendRange(values);
            var additionalBitsCount = ValueBuffer.Length - len;
            ValidityBuffer.AppendRange(true, additionalBitsCount);
            return Instance;
        }

        public TBuilder AppendNull()
        {
            ValidityBuffer.Append(false);
            ValueBuffer.Append(default(T));
            return Instance;
        }

        public TBuilder Clear()
        {
            ValueBuffer.Clear();
            ValidityBuffer.Clear();
            return Instance;
        }

        public TBuilder Set(int index, T value)
        {
            ValueBuffer.Span[index] = value;
            ValidityBuffer.Set(index, true);
            return Instance;
        }

        public TBuilder Swap(int i, int j)
        {
            T x = ValueBuffer.Span[i];
            ValueBuffer.Span[i] = ValueBuffer.Span[j];
            ValueBuffer.Span[j] = x;
            ValidityBuffer.Swap(i, j);
            return Instance;
        }

        public TArray Build(MemoryAllocator allocator = default)
        {
            ArrowBuffer validityBuffer = NullCount > 0
                                    ? ValidityBuffer.Build(allocator)
                                    : ArrowBuffer.Empty;

            return Build(
                ValueBuffer.Build(allocator), validityBuffer,
                ValueBuffer.Length, NullCount, 0);
        }

        protected abstract TArray Build(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset);
    }
}
