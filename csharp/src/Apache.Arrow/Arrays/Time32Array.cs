// Licensed to the Apache Software Foundation (ASF) under one or moreDate32Array
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
using System.IO;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Time32Array"/> class holds an array of <see cref="Int32" />, where each value is
    /// stored as the number of seconds/ milliseconds (depending on the Time32Type) since midnight.
    /// </summary>
    public class Time32Array : PrimitiveArray<int>
#if NET6_0_OR_GREATER
        , IReadOnlyList<TimeOnly?>
#endif
    {
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Time32Array"/> objects.
        /// </summary>
        public class Builder : TimeArrayBuilder<int, Time32Array, Builder>
        {
            private class TimeBuilder : PrimitiveArrayBuilder<int, Time32Array, TimeBuilder>
            {
                public Time32Type DataType { get; }

                public TimeBuilder(Time32Type dataType) => DataType = dataType;

                protected override Time32Array Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new Time32Array(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            }

            public Builder()
                : this(Time32Type.Default) { }

            public Builder(TimeUnit unit)
                : this((Time32Type)TimeType.FromTimeUnit(unit)) { }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder(Time32Type type)
                : base(new TimeBuilder(type))
            {
            }

#if NET6_0_OR_GREATER
            protected override int Convert(TimeOnly time)
            {
                var unit = ((TimeBuilder)InnerBuilder).DataType.Unit;
                return unit switch
                {
                    TimeUnit.Second => (int)(time.Ticks / TimeSpan.TicksPerSecond),
                    TimeUnit.Millisecond => (int)(time.Ticks / TimeSpan.TicksPerMillisecond),
                    _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {unit}")
                };
            }
#endif
        }

        public Time32Array(
            Time32Type type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public Time32Array(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Time32);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        /// <summary>
        /// Get the time at the specified index as seconds
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns an <see cref="Int32" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public int? GetSeconds(int index)
        {
            int? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time32Type) Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => value,
                TimeUnit.Millisecond => value / 1_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {unit}")
            };
        }

        /// <summary>
        /// Get the time at the specified index as milliseconds
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns an <see cref="Int32" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public int? GetMilliSeconds(int index)
        {
            int? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time32Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => value * 1_000,
                TimeUnit.Millisecond => value,
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {unit}")
            };
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Get the time at the specified index as <see cref="TimeOnly"/>
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns a <see cref="TimeOnly" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public TimeOnly? GetTime(int index)
        {
            int? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time32Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Second => new TimeOnly(value.Value * TimeSpan.TicksPerSecond),
                TimeUnit.Millisecond => new TimeOnly(value.Value * TimeSpan.TicksPerMillisecond),
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {unit}")
            };
        }

        int IReadOnlyCollection<TimeOnly?>.Count => Length;

        TimeOnly? IReadOnlyList<TimeOnly?>.this[int index] => GetTime(index);

        IEnumerator<TimeOnly?> IEnumerable<TimeOnly?>.GetEnumerator()
        {
            for (int index = 0; index < Length; index++)
            {
                yield return GetTime(index);
            };
        }
#endif
    }
}
