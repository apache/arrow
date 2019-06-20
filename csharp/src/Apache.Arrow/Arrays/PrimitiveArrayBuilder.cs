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
    public abstract class PrimitiveArrayBuilder<TFrom, TTo, TArray, TBuilder> : IArrowArrayBuilder<TFrom, TArray>
        where TTo: struct
        where TArray: IArrowArray
        where TBuilder: class, IArrowArrayBuilder<TArray>
    {
        protected TBuilder Instance => this as TBuilder;
        protected IArrowArrayBuilder<TTo, TArray, IArrowArrayBuilder<TTo, TArray>> ArrayBuilder { get; }

        internal PrimitiveArrayBuilder(IArrowArrayBuilder<TTo, TArray, IArrowArrayBuilder<TTo, TArray>> builder)
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
            foreach (var value in span)
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
        where T: struct
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<T, TArray>
    {
        protected TBuilder Instance => this as TBuilder;
        protected ArrowBuffer.Builder<T> ValueBuffer { get; }

        // TODO: Implement support for null values (null bitmaps)

        internal PrimitiveArrayBuilder()
        {
            ValueBuffer = new ArrowBuffer.Builder<T>();
        }

        public TBuilder Resize(int length)
        {
            ValueBuffer.Resize(length);
            return Instance;
        }

        public TBuilder Reserve(int capacity)
        {
            ValueBuffer.Reserve(capacity);
            return Instance;
        }

        public TBuilder Append(T value)
        {
            ValueBuffer.Append(value);
            return Instance;
        }

        public TBuilder Append(ReadOnlySpan<T> span)
        {
            ValueBuffer.Append(span);
            return Instance;
        }

        public TBuilder AppendRange(IEnumerable<T> values)
        {
            ValueBuffer.AppendRange(values);
            return Instance;
        }

        public TBuilder Clear()
        {
            ValueBuffer.Clear();
            return Instance;
        }

        public TBuilder Set(int index, T value)
        {
            ValueBuffer.Span[index] = value;
            return Instance;
        }

        public TBuilder Swap(int i, int j)
        {
            var x = ValueBuffer.Span[i];
            ValueBuffer.Span[i] = ValueBuffer.Span[j];
            ValueBuffer.Span[j] = x;
            return Instance;
        }

        public TArray Build(MemoryAllocator allocator = default)
        {
            return Build(
                ValueBuffer.Build(allocator), ArrowBuffer.Empty,
                ValueBuffer.Length, 0, 0);
        }

        protected abstract TArray Build(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset);
    }
}
