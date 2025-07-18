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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Date64Array"/> class holds an array of dates in the <c>Date64</c> format, where each date is
    /// stored as the number of milliseconds since the dawn of (UNIX) time, excluding leap seconds, in multiples of
    /// 86400000.
    /// </summary>
    public class Date64Array : PrimitiveArray<long>, IReadOnlyList<DateTime?>, ICollection<DateTime?>
#if NET6_0_OR_GREATER
        , IReadOnlyList<DateOnly?>, ICollection<DateOnly?>
#endif
    {
        private const long MillisecondsPerDay = 86400000;

        public Date64Array(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(Date64Type.Default, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Date64Array"/> objects.
        /// </summary>
        public class Builder : DateArrayBuilder<long, Date64Array, Builder>
        {
            private class DateBuilder : PrimitiveArrayBuilder<long, Date64Array, DateBuilder>
            {
                protected override Date64Array Build(ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer, int length,
                    int nullCount, int offset) =>
                    new(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder() : base(new DateBuilder()) { }

            protected override long Convert(DateTime dateTime)
            {
                var dateTimeOffset = new DateTimeOffset(
                    DateTime.SpecifyKind(dateTime.Date, DateTimeKind.Unspecified),
                    TimeSpan.Zero);
                return dateTimeOffset.ToUnixTimeMilliseconds();
            }

            protected override long Convert(DateTimeOffset dateTimeOffset)
            {
                return new DateTimeOffset(dateTimeOffset.UtcDateTime.Date, TimeSpan.Zero).ToUnixTimeMilliseconds();
            }

#if NET6_0_OR_GREATER
            protected override long Convert(DateOnly date)
            {
                return ((long)date.DayNumber - _epochDayNumber) * MillisecondsPerDay;
            }
#endif
        }

        public Date64Array(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Date64);
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
            long? value = GetValue(index);
            return value.HasValue
                ? DateTimeOffset.FromUnixTimeMilliseconds(value.Value).Date
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
            long? value = GetValue(index);
            return value.HasValue
                ? DateTimeOffset.FromUnixTimeMilliseconds(value.Value)
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
            long? value = GetValue(index);
            return value.HasValue
                ? DateOnly.FromDateTime(DateTimeOffset.FromUnixTimeMilliseconds(value.Value).UtcDateTime)
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
