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

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Date64Array"/> class holds an array of dates in the <c>Date64</c> format, where each date is
    /// stored as the number of milliseconds since the dawn of (UNIX) time, excluding leap seconds, in multiples of
    /// 86400000.
    /// </summary>
    public class Date64Array: PrimitiveArray<long>
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
            private class DateBuilder: PrimitiveArrayBuilder<long, Date64Array, DateBuilder>
            {
                protected override Date64Array Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new Date64Array(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
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
                // The internal value stored for a DateTimeOffset can be thought of as the number of milliseconds,
                // in multiples of 86400000, that have passed since the UNIX epoch.  It is not the same as what would
                // result from encoding the date from the DateTimeOffset.Date property.
                long millis = dateTimeOffset.ToUnixTimeMilliseconds();
                long days = millis / MillisecondsPerDay;
                return (millis < 0 ? days - 1 : days) * MillisecondsPerDay;
            }
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
    }
}
