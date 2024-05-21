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

namespace Apache.Arrow
{
    public abstract class TimeArrayBuilder<TUnderlying, TArray, TBuilder> :
        DelegatingArrayBuilder<TUnderlying, TArray, TBuilder>
#if NET6_0_OR_GREATER
        , IArrowArrayBuilder<TimeOnly, TArray, TBuilder>
#endif
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray>
    {
        /// <summary>
        /// Construct a new instance of the <see cref="TimeArrayBuilder{TUnderlying,TArray,TBuilder}"/> class.
        /// </summary>
        /// <param name="innerBuilder">Inner builder that will produce arrays of type <typeparamref name="TArray"/>.
        /// </param>
        protected TimeArrayBuilder(IArrowArrayBuilder<TUnderlying, TArray, IArrowArrayBuilder<TArray>> innerBuilder)
            : base(innerBuilder)
        { }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Append a time in the form of a <see cref="TimeOnly"/> object to the array.
        /// </summary>
        /// <param name="value">Time to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Append(TimeOnly value)
        {
            InnerBuilder.Append(Convert(value));
            return this as TBuilder;
        }
#endif

        /// <summary>
        /// Append a time
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public TBuilder Append(TUnderlying value)
        {
            InnerBuilder.Append(value);
            return this as TBuilder;
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Append a span of times in the form of <see cref="TimeOnly"/> objects to the array.
        /// </summary>
        /// <param name="span">Span of times to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Append(ReadOnlySpan<TimeOnly> span)
        {
            InnerBuilder.Reserve(span.Length);
            foreach (var item in span)
            {
                InnerBuilder.Append(Convert(item));
            }

            return this as TBuilder;
        }
#endif

        public TBuilder Append(ReadOnlySpan<TUnderlying> values)
        {
            InnerBuilder.Append(values);
            return this as TBuilder;
        }

        /// <summary>
        /// Append a null time to the array.
        /// </summary>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public override TBuilder AppendNull()
        {
            InnerBuilder.AppendNull();
            return this as TBuilder;
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Append a collection of times in the form of <see cref="TimeOnly"/> objects to the array.
        /// </summary>
        /// <param name="values">Collection of times to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder AppendRange(IEnumerable<TimeOnly> values)
        {
            InnerBuilder.AppendRange(values.Select(Convert));
            return this as TBuilder;
        }
#endif

        public TBuilder AppendRange(IEnumerable<TUnderlying> values)
        {
            InnerBuilder.AppendRange(values);
            return this as TBuilder;
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Set the value of a time in the form of a <see cref="TimeOnly"/> object at the specified index.
        /// </summary>
        /// <param name="index">Index at which to set value.</param>
        /// <param name="value">Time to set.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Set(int index, TimeOnly value)
        {
            InnerBuilder.Set(index, Convert(value));
            return this as TBuilder;
        }
#endif

        public TBuilder Set(int index, TUnderlying value)
        {
            InnerBuilder.Set(index, value);
            return this as TBuilder;
        }

        /// <summary>
        /// Swap the values of the times at the specified indices.
        /// </summary>
        /// <param name="i">First index.</param>
        /// <param name="j">Second index.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Swap(int i, int j)
        {
            InnerBuilder.Swap(i, j);
            return this as TBuilder;
        }

#if NET6_0_OR_GREATER
        protected abstract TUnderlying Convert(TimeOnly time);
#endif
    }
}
