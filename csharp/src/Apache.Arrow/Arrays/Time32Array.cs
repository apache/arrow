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

using System;
using System.IO;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// The <see cref="Time32Array"/> class holds an array of <see cref="Int32" />, where each value is
    /// stored as the number of seconds/ milliseconds (depending on the Time32Type) since midnight.
    /// </summary>
    public class Time32Array : PrimitiveArray<int>
    {
        /// <summary>
        /// The <see cref="Builder"/> class can be used to fluently build <see cref="Time32Array"/> objects.
        /// </summary>
        public class Builder : PrimitiveArrayBuilder<int, Time32Array, Builder>
        {
            protected override Time32Array Build(
                ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                int length, int nullCount, int offset) =>
                new Time32Array(DataType, valueBuffer, nullBitmapBuffer, length, nullCount, offset);
            
            protected Time32Type DataType { get; }

            public Builder()
                : this(Time32Type.Default) { }

            public Builder(TimeUnit unit)
                : this(new Time32Type(unit)) { }

            /// <summary>
            /// Construct a new instance of the <see cref="Builder"/> class.
            /// </summary>
            public Builder(Time32Type type)
                : base()
            {
                DataType = type;
            }
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

        public Time32Type TimeType => (Time32Type)Data.DataType;

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

            return TimeType.Unit switch
            {
                TimeUnit.Second => value,
                TimeUnit.Millisecond => value / 1_000,
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}")
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

            return TimeType.Unit switch
            {
                TimeUnit.Second => value * 1_000,
                TimeUnit.Millisecond => value,
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}")
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
        public new Accessor<Time32Array, TimeSpan?> Items()
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
        public new Accessor<Time32Array, TimeSpan> NotNullItems()
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
