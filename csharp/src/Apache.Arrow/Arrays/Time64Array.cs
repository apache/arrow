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
    /// The <see cref="Time64Array"/> class holds an array of <see cref="Int64" />, where each value is
    /// stored as the number of microseconds/nanoseconds (depending on the Time64Type) since midnight.
    /// </summary>
    public class Time64Array : PrimitiveArray<long>
#if NET6_0_OR_GREATER
        , IReadOnlyList<TimeOnly?>
#endif
    {
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Time64Array"/> objects.
        /// </summary>
        public class Builder : TimeArrayBuilder<long, Time64Array, Builder>
        {
            private class TimeBuilder : PrimitiveArrayBuilder<long, Time64Array, TimeBuilder>
            {
                public Time64Type DataType { get; }

                public TimeBuilder(Time64Type dataType) => DataType = dataType;

                protected override Time64Array Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new Time64Array(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            }

            public Builder()
                : this(Time64Type.Default) { }

            public Builder(TimeUnit unit)
                : this((Time64Type)TimeType.FromTimeUnit(unit)) { }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder(Time64Type type)
                : base(new TimeBuilder(type))
            {
            }

#if NET6_0_OR_GREATER
            protected override long Convert(TimeOnly time)
            {
                return ((TimeBuilder)InnerBuilder).DataType.Unit.ConvertFromTicks(time.Ticks);
            }
#endif
        }

        public Time64Array(
            Time64Type type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueBuffer }))
        { }

        public Time64Array(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Time64);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        /// <summary>
        /// Get the time at the specified index as microseconds
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns a <see cref="Int64" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public long? GetMicroSeconds(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time64Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Microsecond => value,
                TimeUnit.Nanosecond => value / 1_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {unit}")
            };
        }

        /// <summary>
        /// Get the time at the specified index as nanoseconds
        /// </summary>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns a <see cref="Int64" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public long? GetNanoSeconds(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            var unit = ((Time64Type)Data.DataType).Unit;
            return unit switch
            {
                TimeUnit.Microsecond => value * 1_000,
                TimeUnit.Nanosecond => value,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {unit}")
            };
        }

#if NET6_0_OR_GREATER
        /// <summary>
        /// Get the time at the specified index as <see cref="TimeOnly"/>
        /// </summary>
        /// <remarks>
        /// This may cause truncation of nanosecond values, as the resolution of TimeOnly is in 100-ns increments.
        /// </remarks>
        /// <param name="index">Index at which to get the time.</param>
        /// <returns>Returns a <see cref="TimeOnly" />, or <c>null</c> if there is no object at that index.
        /// </returns>
        public TimeOnly? GetTime(int index)
        {
            long? value = GetValue(index);
            if (value == null)
            {
                return null;
            }

            return new TimeOnly(((Time64Type)Data.DataType).Unit.ConvertToTicks(value.Value));
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
