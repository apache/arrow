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
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="DelegatingArrayBuilder{T,TArray,TBuilder}"/> class can be used as the base for any array builder
    /// that needs to delegate most of its functionality to an inner array builder.
    /// </summary>
    /// <remarks>
    /// The typical use case is when an array builder may accept a number of different types as input, but which are
    /// all internally converted to a single type for assembly into an array.
    /// </remarks>
    /// <typeparam name="T">Type of item accepted by inner array builder.</typeparam>
    /// <typeparam name="TArray">Type of array produced by this (and the inner) builder.</typeparam>
    /// <typeparam name="TBuilder">Type of builder (see Curiously-Recurring Template Pattern).</typeparam>
    public abstract class DelegatingArrayBuilder<T, TArray, TBuilder> : IArrowArrayBuilder<TArray, TBuilder>
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray>
    {
        /// <summary>
        /// Gets the inner array builder.
        /// </summary>
        protected IArrowArrayBuilder<T, TArray, IArrowArrayBuilder<TArray>> InnerBuilder { get; }

        /// <summary>
        /// Gets the number of items added to the array so far.
        /// </summary>
        public int Length => InnerBuilder.Length;

        /// <summary>
        /// Construct a new instance of the <see cref="DelegatingArrayBuilder{T,TArray,TBuilder}"/> class.
        /// </summary>
        /// <param name="innerBuilder">Inner array builder.</param>
        protected DelegatingArrayBuilder(IArrowArrayBuilder<T, TArray, IArrowArrayBuilder<TArray>> innerBuilder)
        {
            InnerBuilder = innerBuilder ?? throw new ArgumentNullException(nameof(innerBuilder));
        }

        /// <summary>
        /// Build an Arrow Array from the appended contents so far.
        /// </summary>
        /// <param name="allocator">Optional memory allocator.</param>
        /// <returns>Returns the built array.</returns>
        public TArray Build(MemoryAllocator allocator = default) => InnerBuilder.Build(allocator);

        /// <summary>
        /// Reserve a given number of items' additional capacity.
        /// </summary>
        /// <param name="additionalCapacity">Number of items of required additional capacity.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Reserve(int additionalCapacity)
        {
            InnerBuilder.Reserve(additionalCapacity);
            return this as TBuilder;
        }

        /// <summary>
        /// Resize the array to a given size.
        /// </summary>
        /// <remarks>
        /// Note that if the required capacity is larger than the current length of the populated array so far,
        /// the array's contents in the new, expanded region are undefined.
        /// </remarks>
        /// <remarks>
        /// Note that if the required capacity is smaller than the current length of the populated array so far,
        /// the array will be truncated and items at the end of the array will be lost.
        /// </remarks>
        /// <param name="capacity">Number of items of required capacity.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Resize(int capacity)
        {
            InnerBuilder.Resize(capacity);
            return this as TBuilder;
        }

        /// <summary>
        /// Clear all contents appended so far.
        /// </summary>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Clear()
        {
            InnerBuilder.Clear();
            return this as TBuilder;
        }

        /// <summary>
        /// Appends a null value
        /// </summary>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public abstract TBuilder AppendNull();
    }
}
