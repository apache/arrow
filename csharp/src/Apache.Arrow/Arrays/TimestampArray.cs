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
using System.Diagnostics;
using System.IO;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class TimestampArray: PrimitiveArray<long>
    {
        public class Builder: PrimitiveArrayBuilder<DateTimeOffset, long, TimestampArray, Builder>
        {
            internal class TimestampBuilder : PrimitiveArrayBuilder<long, TimestampArray, TimestampBuilder>
            {
                internal TimestampBuilder(TimestampType type)
                {
                    DataType = type ?? throw new ArgumentNullException(nameof(type));
                }

                protected TimestampType DataType { get; }

                protected override TimestampArray Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new TimestampArray(DataType, valueBuffer, nullBitmapBuffer,
                        length, nullCount, offset);
            }

            protected TimestampType DataType { get; }

            public Builder()
                : this(TimestampType.Default) { }

            public Builder(TimeUnit unit, TimeZoneInfo timezone)
                : this(new TimestampType(unit, timezone)) { }

            public Builder(TimeUnit unit = TimeUnit.Millisecond, string timezone = "+00:00")
                : this(new TimestampType(unit, timezone)) { }

            public Builder(TimeUnit unit)
                : this(new TimestampType(unit, (string) null)) { }

            public Builder(TimestampType type)
                : base(new TimestampBuilder(type))
            {
                DataType = type;
            }

            protected override long ConvertTo(DateTimeOffset value)
            {
                // We must return the absolute time since the UNIX epoch while
                // respecting the timezone offset; the calculation is as follows:
                //
                // - Compute time span between epoch and specified time
                // - Compute time divisions per tick

                TimeSpan timeSpan = value - Types.Convert.EpochDateTimeOffset;
                long ticks = timeSpan.Ticks;

                switch (DataType.Unit)
                {
                    case TimeUnit.Nanosecond:
                        return ticks * 100;
                    case TimeUnit.Microsecond:
                        return ticks / 10;
                    case TimeUnit.Millisecond:
                        return ticks / TimeSpan.TicksPerMillisecond;
                    case TimeUnit.Second:
                        return ticks / TimeSpan.TicksPerSecond;
                    default:
                        throw new InvalidOperationException($"unsupported time unit <{DataType.Unit}>");
                }
            }
        }

        public TimestampArray(
            TimestampType type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] {nullBitmapBuffer, valueBuffer})) { }

        public TimestampArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Timestamp);

            Debug.Assert(Data.DataType is TimestampType);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public TimestampType TimeType => (TimestampType)Data.DataType;

        public DateTimeOffset? GetTimestamp(int index)
        {
            if (IsValid(index))
            {
                return TimeType.Unit switch
                {
                    TimeUnit.Second => (DateTimeOffset?)UnixSecondsToDateTimeOffset(index),
                    TimeUnit.Millisecond => (DateTimeOffset?)UnixMillisecondsToDateTimeOffset(index),
                    TimeUnit.Microsecond => (DateTimeOffset?)UnixMicrosecondsToDateTimeOffset(index),
                    TimeUnit.Nanosecond => (DateTimeOffset?)UnixNanosecondsToDateTimeOffset(index),
                    _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}"),
                };
            }
            return null;
        }

        public new DateTimeOffset? this[int index]
        {
            get
            {
                return index < 0 ? this[Length + index] : GetTimestamp(index);
            }
            // TODO: Implement setter
            //set
            //{
            //    data[index] = value;
            //}
        }

        // Accessors
        public new Accessor<TimestampArray, DateTimeOffset?> Items()
        {
            return TimeType.Unit switch
            {
                TimeUnit.Second => new(this, (a, i) => a.IsValid(i) ? a.UnixSecondsToDateTimeOffset(i) : null),
                TimeUnit.Millisecond => new(this, (a, i) => a.IsValid(i) ? a.UnixMillisecondsToDateTimeOffset(i) : null),
                TimeUnit.Microsecond => new(this, (a, i) => a.IsValid(i) ? a.UnixMicrosecondsToDateTimeOffset(i) : null),
                TimeUnit.Nanosecond => new(this, (a, i) => a.IsValid(i) ? a.UnixNanosecondsToDateTimeOffset(i) : null),
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}"),
            };
        }
        public new Accessor<TimestampArray, DateTimeOffset> NotNullItems()
        {
            return TimeType.Unit switch
            {
                TimeUnit.Second => new(this, (a, i) => a.UnixSecondsToDateTimeOffset(i)),
                TimeUnit.Millisecond => new(this, (a, i) => a.UnixMillisecondsToDateTimeOffset(i)),
                TimeUnit.Microsecond => new(this, (a, i) => a.UnixMicrosecondsToDateTimeOffset(i)),
                TimeUnit.Nanosecond => new(this, (a, i) => a.UnixNanosecondsToDateTimeOffset(i)),
                _ => throw new InvalidDataException($"Unsupported time unit for Time32Type: {TimeType.Unit}"),
            };
        }

        // Static Methods to Convert ticks to date/time instances
        public DateTimeOffset UnixSecondsToDateTimeOffset(int index) => Types.Convert.UnixSecondsToDateTimeOffset(Values[index]);
        public DateTimeOffset UnixMillisecondsToDateTimeOffset(int index) => Types.Convert.UnixMillisecondsToDateTimeOffset(Values[index]);
        public DateTimeOffset UnixMicrosecondsToDateTimeOffset(int index) => Types.Convert.UnixMicrosecondsToDateTimeOffset(Values[index]);
        public DateTimeOffset UnixNanosecondsToDateTimeOffset(int index) => Types.Convert.UnixNanosecondsToDateTimeOffset(Values[index]);
    }
}
