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
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Date32Array"/> class holds an array of dates in the <c>Date32</c> format, where each date is
    /// stored as the number of days since the dawn of (UNIX) time.
    /// </summary>
    public class Date32Array : PrimitiveArray<int>
    {
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Date32Array"/> objects.
        /// </summary>
        public class Builder : DateArrayBuilder<int, Date32Array, Builder>
        {
            private class DateBuilder : PrimitiveArrayBuilder<int, Date32Array, DateBuilder>
            {
                protected override Date32Array Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new Date32Array(valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder() : base(new DateBuilder()) { }

            protected override int Convert(DateTime dateTime)
            {
                return (int)dateTime.ToUnixDays();
            }

            protected override int Convert(DateTimeOffset dateTimeOffset)
            {
                // The internal value stored for a DateTimeOffset can be thought of as the number of 24-hour "blocks"
                // of time that have elapsed since the UNIX epoch.  This is the same as converting it to UTC first and
                // then taking the date element from that.  It is not the same as what would result from looking at the
                // DateTimeOffset.Date property.
                return (int)dateTimeOffset.ToUnixDays();
            }
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
            return IsValid(index) ? UnixDaysToDateTime(index) : null;
        }

        /// <summary>
        /// Get the date at the specified index in the form of a <see cref="DateTimeOffset"/> object.
        /// </summary>
        /// <param name="index">Index at which to get the date.</param>
        /// <returns>Returns a <see cref="DateTimeOffset"/> object, or <c>null</c> if there is no object at that index.
        /// </returns>
        public DateTimeOffset? GetDateTimeOffset(int index)
        {
            return IsValid(index) ? UnixDaysToDateTimeOffset(index) : null;
        }

        public new DateTimeOffset? this[int index] => index < 0 ? GetDateTimeOffset(Length + index) : GetDateTimeOffset(index);

        // Accessors
        public new Accessor<Date32Array, DateTimeOffset?> Items() => new(this, (a, i) => IsValid(i) ? a.UnixDaysToDateTimeOffset(i) : null);
        public new Accessor<Date32Array, DateTimeOffset> NotNullItems() => new(this, (a, i) => a.UnixDaysToDateTimeOffset(i));

        // Static Methods to Convert ticks to date/time instances
        public DateTime UnixDaysToDateTime(int index) => Types.Convert.UnixDaysToDateTime(Values[index]);
        public DateTimeOffset UnixDaysToDateTimeOffset(int index) => Types.Convert.UnixDaysToDateTimeOffset(Values[index]);
    }
}
