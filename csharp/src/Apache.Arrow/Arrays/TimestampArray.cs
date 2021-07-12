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
using System.Diagnostics;
using System.IO;

namespace Apache.Arrow
{
    public class TimestampArray: PrimitiveArray<long>
    {
        private static readonly DateTimeOffset s_epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

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

                TimeSpan timeSpan = value - s_epoch;
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

        public DateTimeOffset GetTimestampUnchecked(int index)
        {
            var type = (TimestampType) Data.DataType;
            long value = Values[index];

            long ticks;

            switch (type.Unit)
            {
                case TimeUnit.Nanosecond:
                    ticks = value / 100;
                    break;
                case TimeUnit.Microsecond:
                    ticks = value * 10;
                    break;
                case TimeUnit.Millisecond:
                    ticks = value * TimeSpan.TicksPerMillisecond;
                    break;
                case TimeUnit.Second:
                    ticks = value * TimeSpan.TicksPerSecond;
                    break;
                default:
                    throw new InvalidDataException(
                        $"Unsupported timestamp unit <{type.Unit}>");
            }

            return new DateTimeOffset(s_epoch.Ticks + ticks, TimeSpan.Zero);
        }

        public DateTimeOffset? GetTimestamp(int index)
        {
            if (IsNull(index))
            {
                return null;
            }

            return GetTimestampUnchecked(index);
        }

    }
}
