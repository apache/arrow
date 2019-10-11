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
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public interface IDictionaryArray : IArrowArray
    {
        int UniqueValuesCount { get; }
        ReadOnlySpan<int> Indices { get; }

        ArrowTypeId EnclosedTypeId { get; }
    }

    public interface IDictionaryArrayBuilder<T, out TArray, out TBuilder> : IArrowArrayBuilder<T, TArray, TBuilder>, IDictionaryArrayBuilder<T, TArray>
        where TArray : IDictionaryArray
        where TBuilder : IArrowArrayBuilder<TArray>
    {

    }

    public interface IDictionaryArrayBuilder<T, out TArray> : IDictionaryArrayBuilder<TArray>, IArrowArrayBuilder<T, TArray>
        where TArray : IDictionaryArray
    { }

    public interface IDictionaryArrayBuilder<out TArray> : IArrowArrayBuilder<TArray>
        where TArray : IDictionaryArray
    {
        TArray Build(MemoryAllocator allocator);
    }


    public abstract class DictionaryArray : Array, IDictionaryArray
    {
        /// <inheritdoc />
        protected DictionaryArray(ArrayData data, int uniqueValuesCount) : base(data)
        {
            UniqueValuesCount = uniqueValuesCount;
            data.EnsureDataType(ArrowTypeId.Dictionary);
            EnclosedTypeId = ((DictionaryType) data.DataType).ContainedTypeId;
        }


        #region DictionaryArrayBuilderBase<T>

        public abstract class DictionaryArrayBuilderBase<T>
        {
            public delegate int HashFunctionDelegate(T obj);


            protected readonly Dictionary<DictionaryEntry, int> Entries = new Dictionary<DictionaryEntry, int>();
            protected readonly IEqualityComparer<T> Comparer = EqualityComparer<T>.Default;
            protected readonly HashFunctionDelegate HashFunction = EqualityComparer<T>.Default.GetHashCode;
            protected int NextIndex = 0;

            public ArrowBuffer.Builder<int> IndicesBuffer { get; }
            protected ArrowBuffer.Builder<byte>  NullBitmap { get; set; }


            protected DictionaryArrayBuilderBase(IEqualityComparer<T> comparer = null, HashFunctionDelegate hashFunc = null)
            {
                IndicesBuffer = new ArrowBuffer.Builder<int>();
                NullBitmap = new ArrowBuffer.Builder<byte>();

                if (comparer != null)
                    Comparer = comparer;

                if (hashFunc != null)
                    HashFunction = hashFunc;

            }

            // This allows for custom comparers/hash functions for storing objects in a dictionary array,
            // allowing for specialized use and potentially better performance in certain circumstances
            protected struct DictionaryEntry
            {
                public DictionaryEntry(T value, IEqualityComparer<T> comparer = null, HashFunctionDelegate hashFunction = null)
                {
                    if (comparer == null)
                        comparer = EqualityComparer<T>.Default;
                    _comparer = comparer;

                    if (hashFunction == null)
                        hashFunction = comparer.GetHashCode;

                    _hashFunction = hashFunction;
                    Value = value;
                }

                public T Value { get; }

                private readonly IEqualityComparer<T> _comparer; 

                private readonly HashFunctionDelegate _hashFunction;

                public bool Equals(DictionaryEntry other)
                {
                    return _comparer.Equals(Value, other.Value);
                }

                /// <inheritdoc />
                public override bool Equals(object obj)
                {
                    if (ReferenceEquals(null, obj)) return false;
                    return obj is DictionaryEntry other && Equals(other);
                }

                /// <inheritdoc />
                public override int GetHashCode()
                {
                    return _hashFunction(Value);
                }

                public static bool operator ==(DictionaryEntry left, DictionaryEntry right)
                {
                    return left.Equals(right);
                }

                public static bool operator !=(DictionaryEntry left, DictionaryEntry right)
                {
                    return !left.Equals(right);
                }
            }


        }
        #endregion

        /// <inheritdoc />
        public int UniqueValuesCount { get; }

        /// <inheritdoc />
        public abstract ReadOnlySpan<int> Indices { get; }

        /// <inheritdoc />
        public ArrowTypeId EnclosedTypeId { get; }
    }

}
