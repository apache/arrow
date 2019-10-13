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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Arrays
{
    public abstract class PrimitiveDictionaryArray<T> : DictionaryArray where T : struct
    {

        public ArrowBuffer ValueBuffer => Data.Buffers[2];

        public ReadOnlySpan<T> Values => ValueBuffer.Span.CastTo<T>().Slice(0, UniqueValuesCount);
        
        public ArrowBuffer IndicesBuffer => Data.Buffers[1];

        public override ReadOnlySpan<int> Indices => IndicesBuffer.Span.CastTo<int>().Slice(0, Length);


        /// <inheritdoc />
        protected PrimitiveDictionaryArray(ArrayData data, int uniqueValuesCount) : base(data, uniqueValuesCount)
        {
            Data.EnsureBufferCount(3);
        }

        protected PrimitiveDictionaryArray(IArrowType dataType, int length, int uniqueValuesCount,
            ArrowBuffer nullBitmapBuffer,
            ArrowBuffer indices,
            ArrowBuffer dataBuffer,
            int nullCount = 0, int offset = 0)
            : this(new ArrayData(dataType, length, nullCount, offset,
                new[] { nullBitmapBuffer, indices, dataBuffer }), uniqueValuesCount)
        { }

    }


    public abstract class PrimitiveDictionaryArrayBuilder<TFrom, TTo, TArray, TBuilder> :
        DictionaryArray.DictionaryArrayBuilderBase<TTo>, IDictionaryArrayBuilder<TFrom, TArray, TBuilder>
        where TTo : struct, IEquatable<TTo>
        where TArray : IDictionaryArray
        where TBuilder : class, IDictionaryArrayBuilder<TArray>

    {
        protected TBuilder Instance => this as TBuilder;

        protected IDictionaryArrayBuilder<TTo, TArray, IDictionaryArrayBuilder<TTo, TArray>> ArrayBuilder { get; }


        protected ArrowBuffer.Builder<TTo> ValuesBuffer;

        internal PrimitiveDictionaryArrayBuilder(IDictionaryArrayBuilder<TTo, TArray, IDictionaryArrayBuilder<TTo, TArray>> builder,
            IEqualityComparer<TTo> comparer = null, HashFunctionDelegate hashFunc = null) : base(comparer, hashFunc)
        {
            ArrayBuilder = builder ?? throw new ArgumentNullException(nameof(builder));
        }

        public TArray Build(MemoryAllocator allocator = default) => ArrayBuilder.Build(allocator);

        public TBuilder Append(TFrom value)
        {
            var convertedVal = ConvertTo(value);
            var temp = new DictionaryEntry(convertedVal, Comparer, HashFunction);
            if (!Entries.TryGetValue(temp, out var index))
            {
                index = NextIndex++;
                Entries.Add(temp, index);
                ValuesBuffer.Append(convertedVal);
            }

            IndicesBuffer.Append(index);
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


    public abstract class PrimitiveDictionaryArrayBuilder<T, TArray, TBuilder> : DictionaryArray.DictionaryArrayBuilderBase<T>, IDictionaryArrayBuilder<T, TArray, TBuilder>
        where T : struct, IEquatable<T>
        where TArray : IDictionaryArray
        where TBuilder : class, IDictionaryArrayBuilder<T, TArray, TBuilder>
    {
        protected TBuilder Instance => this as TBuilder;

        protected ArrowBuffer.Builder<T> ValuesBuffer;

        /// <inheritdoc />
        protected PrimitiveDictionaryArrayBuilder(IEqualityComparer<T> comparer = null, HashFunctionDelegate hashFunc = null) : base(comparer, hashFunc)
        {
            ValuesBuffer = new ArrowBuffer.Builder<T>();
        }

        //public PrimitiveDictionaryArray<T> Build(MemoryAllocator allocator)
        //{
        //    // create a buffer of values
        //    var size = Unsafe.SizeOf<T>();
        //    int bufferLength = checked((int)BitUtility.RoundUpToMultipleOf64(size * Entries.Count));

        //    allocator = allocator ?? MemoryAllocator.Default.Value;
        //    allocator.Allocate(bufferLength);

        //    var dataBuffer = new ArrowBuffer.Builder<T>(Entries.Count);
        //    foreach (var entry in Entries.Keys)
        //    {
        //        dataBuffer.Append(entry.Value);
        //    }


        //    return new PrimitiveDictionaryArray<T>(ArrowType, IndicesBuffer.Length, IndicesBuffer.Build(allocator), dataBuffer.Build(allocator),
        //        ArrowBuffer.Empty);
        //}

        public TBuilder Append(T value)
        {
            var temp = new DictionaryEntry(value, Comparer, HashFunction);
            if (!Entries.TryGetValue(temp, out var index))
            {
                index = NextIndex++;
                Entries.Add(temp, index);
                ValuesBuffer.Append(value);
            }

            IndicesBuffer.Append(index);
            return Instance;
        }

        public TBuilder Append(ReadOnlySpan<T> span)
        {
            foreach (var t in span)
                Append(t);

            return Instance;
        }

        public TBuilder AppendRange(IEnumerable<T> values)
        {
            foreach (var t in values)
                Append(t);

            return Instance;
        }

        /// <inheritdoc />
        public TBuilder Reserve(int capacity)
        {
            IndicesBuffer.Reserve(capacity);
            ValuesBuffer.Reserve(capacity);
            return Instance;
        }

        /// <inheritdoc />
        public TBuilder Resize(int length)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public TBuilder Swap(int i, int j)
        {
            if (i < 0 || j < 0 || i > IndicesBuffer.Length || j > IndicesBuffer.Length)
            {
                throw new ArgumentOutOfRangeException();
            }

            var span = IndicesBuffer.Memory.Span.CastTo<int>();
            var temp = span[i];
            span[i] = span[j];
            span[j] = temp;

            return Instance;
        }

        /// <inheritdoc />
        public TBuilder Set(int index, T value)
        {
            if (index < 0 || index > IndicesBuffer.Length)
            {
                throw new ArgumentOutOfRangeException();
            }

            var temp = new DictionaryEntry(value, Comparer, HashFunction);
            if (!Entries.TryGetValue(temp, out var valueIndex))
            {
                valueIndex = NextIndex++;
                Entries.Add(temp, valueIndex);
                ValuesBuffer.Append(value);
            }

            IndicesBuffer.Memory.Span.CastTo<int>()[index] = valueIndex;

            return Instance;
        }

        /// <inheritdoc />
        public TBuilder Clear()
        {
            Entries.Clear();
            IndicesBuffer.Clear();
            ValuesBuffer.Clear();
            NextIndex = 0;
            return Instance;
        }

        /// <inheritdoc />
        public abstract TArray Build(MemoryAllocator allocator);
    }

}
