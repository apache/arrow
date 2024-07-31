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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Date32Array"/> class holds an array of dates in the <c>Date32</c> format, where each date is
    /// stored as the number of days since the dawn of (UNIX) time.
    /// </summary>
    public class Date32Array : PrimitiveArray<int>, IReadOnlyList<DateTime?>, ICollection<DateTime?>
#if NET6_0_OR_GREATER
        , IReadOnlyList<DateOnly?>, ICollection<DateOnly?>
#endif
    {
        private static readonly DateTime _epochDate = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
#if NET6_0_OR_GREATER
        private static readonly int _epochDayNumber = new DateOnly(1970, 1, 1).DayNumber;
#endif

        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Date32Array"/> objects.
        /// </summary>
        public class Builder : DateArrayBuilder<int, Date32Array, Builder>
        {
            private class DateBuilder : PrimitiveArrayBuilder<int, Date32Array, DateBuilder>
            {
                protected override Date32Array Build(ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer, int length,
                    int nullCount, int offset) =>
                    new(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder() : base(new DateBuilder()) { }

            protected override int Convert(DateTime dateTime)
            {
                return (int)(dateTime.Date - _epochDate).TotalDays;
            }

            protected override int Convert(DateTimeOffset dateTimeOffset)
            {
                // The internal value stored for a DateTimeOffset can be thought of as the number of 24-hour "blocks"
                // of time that have elapsed since the UNIX epoch.  This is the same as converting it to UTC first and
                // then taking the date element from that.  It is not the same as what would result from looking at the
                // DateTimeOffset.Date property.
                return (int)(dateTimeOffset.UtcDateTime.Date - _epochDate).TotalDays;
            }

#if NET6_0_OR_GREATER
            protected override int Convert(DateOnly date)
            {
                return (int)(date.DayNumber - _epochDayNumber);
            }
#endif
        }

        public Date32Array(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(Date32Type.Default, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public Date32Array(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Date32);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        [Obsolete("Use `GetDateTimeOffset()` instead")]
        public DateTimeOffset? GetDate(int index) => GetDateTimeOffset(index);

        /// <summary>
        /// Get the date at the specified index in the form of a <see cref="DateTime"/> object.
        /// </summary>
        /// <remarks>
        /// The <see cref="DateTime.Kind"/> property of the returned object is set to
        /// <see cref="DateTimeKind.Unspecified"/>.
        /// </remarks>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns a <see cref="DateTime"/> object, or <c>null</c> if there is no object at that index.
        /// </returns>
        public DateTime? GetDateTime(int index)
        {
            int? value = GetValue(index);
            return value.HasValue
                ? _epochDate.AddDays(value.Value)
                : default(DateTime?);
        }

        /// <summary>
        /// Get the date at the specified index in the form of a <see cref="DateTimeOffset"/> object.
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns a <see cref="DateTimeOffset"/> object, or <c>null</c> if there is no object at that index.
        /// </returns>
        public DateTimeOffset? GetDateTimeOffset(int index)
        {
            int? value = GetValue(index);
            return value.HasValue
                ? new DateTimeOffset(_epochDate.AddDays(value.Value), TimeSpan.Zero)
                : default(DateTimeOffset?);
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Get the date at the specified index
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns a <see cref="DateOnly" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public DateOnly? GetDateOnly(int index)
        {
            int? value = GetValue(index);
            return value.HasValue
                ? DateOnly.FromDayNumber(_epochDayNumber + value.Value)
                : default(DateOnly?);
        }

        int IReadOnlyCollection<DateOnly?>.Count => Length;

        DateOnly? IReadOnlyList<DateOnly?>.this[int index] => GetDateOnly(index);

        IEnumerator<DateOnly?> IEnumerable<DateOnly?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetDateOnly(index);
            };
        }

        int ICollection<DateOnly?>.Count => Length;
        bool ICollection<DateOnly?>.IsReadOnly => true;
        void ICollection<DateOnly?>.Add(DateOnly? item) => throw new NotSupportedException("Collection is read-only.");
        bool ICollection<DateOnly?>.Remove(DateOnly? item) => throw new NotSupportedException("Collection is read-only.");
        void ICollection<DateOnly?>.Clear() => throw new NotSupportedException("Collection is read-only.");

        bool ICollection<DateOnly?>.Contains(DateOnly? item)
        {
            for (int index = 0; index < Length; index++)
            {
                if (GetDateOnly(index).Equals(item))
                    return true;
            }

            return false;
        }

        void ICollection<DateOnly?>.CopyTo(DateOnly?[] array, int arrayIndex)
        {
            for (int srcIndex = 0, destIndex = arrayIndex; srcIndex < Length; srcIndex++, destIndex++)
            {
                array[destIndex] = GetDateOnly(srcIndex);
            }
        }
#endif

        int IReadOnlyCollection<DateTime?>.Count => Length;

        DateTime? IReadOnlyList<DateTime?>.this[int index] => GetDateTime(index);

        IEnumerator<DateTime?> IEnumerable<DateTime?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetDateTime(index);
            }
        }

        int ICollection<DateTime?>.Count => Length;
        bool ICollection<DateTime?>.IsReadOnly => true;
        void ICollection<DateTime?>.Add(DateTime? item) => throw new NotSupportedException("Collection is read-only.");
        bool ICollection<DateTime?>.Remove(DateTime? item) => throw new NotSupportedException("Collection is read-only.");
        void ICollection<DateTime?>.Clear() => throw new NotSupportedException("Collection is read-only.");

        bool ICollection<DateTime?>.Contains(DateTime? item)
        {
            for (int index = 0; index < Length; index++)
            {
                if (GetDateTime(index).Equals(item))
                    return true;
            }

            return false;
        }

        void ICollection<DateTime?>.CopyTo(DateTime?[] array, int arrayIndex)
        {
            for (int srcIndex = 0, destIndex = arrayIndex; srcIndex < Length; srcIndex++, destIndex++)
            {
                array[destIndex] = GetDateTime(srcIndex);
            }
        }
    }
}
