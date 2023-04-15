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
using System.IO;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Time64Array"/> class holds an array of <see cref="Int64" />, where each value is
    /// stored as the number of microseconds/nanoseconds (depending on the Time64Type) since midnight.
    /// </summary>
    public class Time64Array : PrimitiveArray<long>
    {
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Time64Array"/> objects.
        /// </summary>
        public class Builder : PrimitiveArrayBuilder<long, Time64Array, Builder>
        {
            protected override Time64Array Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new Time64Array(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);

            protected Time64Type DataType { get; }

            public Builder()
                : this(Time64Type.Default) { }

            public Builder(TimeUnit unit)
                : this(new Time64Type(unit)) { }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder(Time64Type type)
                : base()
            {
                DataType = type;
            }
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

        public Time64Type TimeType => (Time64Type)Data.DataType;

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

            return TimeType.Unit switch
            {
                TimeUnit.Microsecond => value,
                TimeUnit.Nanosecond => value / 1_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {TimeType.Unit}")
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

            return TimeType.Unit switch
            {
                TimeUnit.Microsecond => value * 1_000,
                TimeUnit.Nanosecond => value,
                _ => throw new InvalidDataException($"Unsupported time unit for Time64Type: {TimeType.Unit}")
            };
        }

        public new TimeSpan? this[int index]
        {
            get
            {
                if (IsValid(index))
                {
                    return TimeType.Unit switch
                    {
                        TimeUnit.Second => (TimeSpan?)SecondsToTimeSpan(index),
                        TimeUnit.Millisecond => (TimeSpan?)MillisecondsToTimeSpan(index),
                        TimeUnit.Microsecond => (TimeSpan?)MicrosecondsToTimeSpan(index),
                        TimeUnit.Nanosecond => (TimeSpan?)NanosecondsToTimeSpan(index),
                        _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}"),
                    };
                }
                return null;
            }
            // TODO: Implement setter
            //set
            //{
            //    data[index] = value;
            //}
        }

        // Accessors
        public new Accessor<Time64Array, TimeSpan?> Items()
        {
            return TimeType.Unit switch
            {
                TimeUnit.Second => new(this, (a, i) => a[i]),
                TimeUnit.Millisecond => new(this, (a, i) => a[i]),
                TimeUnit.Microsecond => new(this, (a, i) => a[i]),
                TimeUnit.Nanosecond => new(this, (a, i) => a[i]),
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}"),
            };
        }
        public new Accessor<Time64Array, TimeSpan> NotNullItems()
        {
            return TimeType.Unit switch
            {
                TimeUnit.Second => new(this, (a, i) => a.SecondsToTimeSpan(i)),
                TimeUnit.Millisecond => new(this, (a, i) => a.MillisecondsToTimeSpan(i)),
                TimeUnit.Microsecond => new(this, (a, i) => a.MicrosecondsToTimeSpan(i)),
                TimeUnit.Nanosecond => new(this, (a, i) => a.NanosecondsToTimeSpan(i)),
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}"),
            };
        }

        // Static Methods to Convert ticks to date/time instances
        public TimeSpan SecondsToTimeSpan(int index) => Types.Convert.SecondsToTimeSpan(Values[index]);
        public TimeSpan MillisecondsToTimeSpan(int index) => Types.Convert.MillisecondsToTimeSpan(Values[index]);
        public TimeSpan MicrosecondsToTimeSpan(int index) => Types.Convert.MicrosecondsToTimeSpan(Values[index]);
        public TimeSpan NanosecondsToTimeSpan(int index) => Types.Convert.NanosecondsToTimeSpan(Values[index]);
    }
}
