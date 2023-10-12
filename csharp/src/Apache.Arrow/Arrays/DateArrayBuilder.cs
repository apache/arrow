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
    /// <summary>
    /// The <see cref="DateArrayBuilder{TUnderlying,TArray,TBuilder}"/> class is an abstract array builder that can
    /// accept dates in the form of <see cref="DateTime"/> or <see cref="DateTimeOffset"/> and convert to some
    /// underlying date representation.
    /// </summary>
    public abstract class DateArrayBuilder<TUnderlying, TArray, TBuilder> :
        DelegatingArrayBuilder<TUnderlying, TArray, TBuilder>,
        IArrowArrayBuilder<DateTime, TArray, TBuilder>,
        IArrowArrayBuilder<DateTimeOffset, TArray, TBuilder>
#if NET6_0_OR_GREATER
        , IArrowArrayBuilder<DateOnly, TArray, TBuilder>
#endif
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray>
    {
#if NET6_0_OR_GREATER
        protected static readonly long _epochDayNumber = new DateOnly(1970, 1, 1).DayNumber;
#endif

        /// <summary>
        /// Construct a new instance of the <see cref="DateArrayBuilder{TUnderlying,TArray,TBuilder}"/> class.
        /// </summary>
        /// <param name="innerBuilder">Inner builder that will produce arrays of type <typeparamref name="TArray"/>.
        /// </param>
        protected DateArrayBuilder(IArrowArrayBuilder<TUnderlying, TArray, IArrowArrayBuilder<TArray>> innerBuilder)
            : base(innerBuilder)
        { }

        /// <summary>
        /// Append a date in the form of a <see cref="DateTime"/> object to the array.
        /// </summary>
        /// <remarks>
        /// The value of <see cref="DateTime.Kind"/> on the input does not have any effect on the behaviour of this
        /// method.
        /// </remarks>
        /// <param name="value">Date to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Append(DateTime value)
        {
            InnerBuilder.Append(Convert(value));
            return this as TBuilder;
        }

        /// <summary>
        /// Append a date from a <see cref="DateTimeOffset"/> object to the array.
        /// </summary>
        /// <remarks>
        /// Note that to convert the supplied <paramref name="value"/> parameter to a date, it is first converted to
        /// UTC and the date then taken from the UTC date/time.  Depending on the value of its
        /// <see cref="DateTimeOffset.Offset"/> property, this may not necessarily be the same as the date obtained by
        /// calling its <see cref="DateTimeOffset.Date"/> property.
        /// </remarks>
        /// <param name="value">Date to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Append(DateTimeOffset value)
        {
            InnerBuilder.Append(Convert(value));
            return this as TBuilder;
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Append a date from a <see cref="DateOnly"/> object to the array.
        /// </summary>
        /// </remarks>
        /// <param name="value">Date to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Append(DateOnly value)
        {
            InnerBuilder.Append(Convert(value));
            return this as TBuilder;
        }
#endif

        /// <summary>
        /// Append a span of dates in the form of <see cref="DateTime"/> objects to the array.
        /// </summary>
        /// <remarks>
        /// The value of <see cref="DateTime.Kind"/> on any of the inputs does not have any effect on the behaviour of
        /// this method.
        /// </remarks>
        /// <param name="span">Span of dates to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Append(ReadOnlySpan<DateTime> span)
        {
            InnerBuilder.Reserve(span.Length);
            foreach (var item in span)
            {
                InnerBuilder.Append(Convert(item));
            }

            return this as TBuilder;
        }

        /// <summary>
        /// Append a span of dates in the form of <see cref="DateTimeOffset"/> objects to the array.
        /// </summary>
        /// <remarks>
        /// Note that to convert the <see cref="DateTimeOffset"/> objects in the <paramref name="span"/> parameter to
        /// dates, they are first converted to UTC and the date then taken from the UTC date/times.  Depending on the
        /// value of each <see cref="DateTimeOffset.Offset"/> property, this may not necessarily be the same as the
        /// date obtained by calling the <see cref="DateTimeOffset.Date"/> property.
        /// </remarks>
        /// <param name="span">Span of dates to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Append(ReadOnlySpan<DateTimeOffset> span)
        {
            InnerBuilder.Reserve(span.Length);
            foreach (var item in span)
            {
                InnerBuilder.Append(Convert(item));
            }

            return this as TBuilder;
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Append a span of dates in the form of <see cref="DateOnly"/> objects to the array.
        /// </summary>
        /// <param name="span">Span of dates to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Append(ReadOnlySpan<DateOnly> span)
        {
            InnerBuilder.Reserve(span.Length);
            foreach (var item in span)
            {
                InnerBuilder.Append(Convert(item));
            }

            return this as TBuilder;
        }
#endif

        /// <summary>
        /// Append a null date to the array.
        /// </summary>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public override TBuilder AppendNull()
        {
            InnerBuilder.AppendNull();
            return this as TBuilder;
        }

        /// <summary>
        /// Append a collection of dates in the form of <see cref="DateTime"/> objects to the array.
        /// </summary>
        /// <remarks>
        /// The value of <see cref="DateTime.Kind"/> on any of the inputs does not have any effect on the behaviour of
        /// this method.
        /// </remarks>
        /// <param name="values">Collection of dates to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder AppendRange(IEnumerable<DateTime> values)
        {
            InnerBuilder.AppendRange(values.Select(Convert));
            return this as TBuilder;
        }

        /// <summary>
        /// Append a collection of dates in the form of <see cref="DateTimeOffset"/> objects to the array.
        /// </summary>
        /// <remarks>
        /// Note that to convert the <see cref="DateTimeOffset"/> objects in the <paramref name="values"/> parameter to
        /// dates, they are first converted to UTC and the date then taken from the UTC date/times.  Depending on the
        /// value of each <see cref="DateTimeOffset.Offset"/> property, this may not necessarily be the same as the
        /// date obtained by calling the <see cref="DateTimeOffset.Date"/> property.
        /// </remarks>
        /// <param name="values">Collection of dates to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder AppendRange(IEnumerable<DateTimeOffset> values)
        {
            InnerBuilder.AppendRange(values.Select(Convert));
            return this as TBuilder;
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Append a collection of dates in the form of <see cref="DateOnly"/> objects to the array.
        /// </summary>
        /// <param name="values">Collection of dates to add.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder AppendRange(IEnumerable<DateOnly> values)
        {
            InnerBuilder.AppendRange(values.Select(Convert));
            return this as TBuilder;
        }
#endif

        /// <summary>
        /// Set the value of a date in the form of a <see cref="DateTime"/> object at the specified index.
        /// </summary>
        /// <remarks>
        /// The value of <see cref="DateTime.Kind"/> on the input does not have any effect on the behaviour of this
        /// method.
        /// </remarks>
        /// <param name="index">Index at which to set value.</param>
        /// <param name="value">Date to set.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Set(int index, DateTime value)
        {
            InnerBuilder.Set(index, Convert(value));
            return this as TBuilder;
        }

        /// <summary>
        /// Set the value of a date in the form of a <see cref="DateTimeOffset"/> object at the specified index.
        /// </summary>
        /// <remarks>
        /// Note that to convert the supplied <paramref name="value"/> parameter to a date, it is first converted to
        /// UTC and the date then taken from the UTC date/time.  Depending on the value of its
        /// <see cref="DateTimeOffset.Offset"/> property, this may not necessarily be the same as the date obtained by
        /// calling its <see cref="DateTimeOffset.Date"/> property.
        /// </remarks>
        /// <param name="index">Index at which to set value.</param>
        /// <param name="value">Date to set.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Set(int index, DateTimeOffset value)
        {
            InnerBuilder.Set(index, Convert(value));
            return this as TBuilder;
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Set the value of a date in the form of a <see cref="DateOnly"/> object at the specified index.
        /// </summary>
        /// <param name="index">Index at which to set value.</param>
        /// <param name="value">Date to set.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Set(int index, DateOnly value)
        {
            InnerBuilder.Set(index, Convert(value));
            return this as TBuilder;
        }
#endif

        /// <summary>
        /// Swap the values of the dates at the specified indices.
        /// </summary>
        /// <param name="i">First index.</param>
        /// <param name="j">Second index.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public TBuilder Swap(int i, int j)
        {
            InnerBuilder.Swap(i, j);
            return this as TBuilder;
        }

        protected abstract TUnderlying Convert(DateTime dateTime);

        protected abstract TUnderlying Convert(DateTimeOffset dateTimeOffset);

#if NET6_0_OR_GREATER
        protected abstract TUnderlying Convert(DateOnly date);
#endif
    }
}
