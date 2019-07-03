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
        private static readonly DateTimeOffset Epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        public class Builder: PrimitiveArrayBuilder<DateTimeOffset, long, TimestampArray, Builder>
        {
            internal class TimestampBuilder : PrimitiveArrayBuilder<long, TimestampArray, TimestampBuilder>
            {
                internal TimestampBuilder(TimestampType type)
                {
                    DataType = type ?? throw new ArgumentNullException(nameof(type));
                }

                public TimestampType DataType { get; }

                protected override TimestampArray Build(
                    ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
                    int length, int nullCount, int offset) =>
                    new TimestampArray(DataType, valueBuffer, nullBitmapBuffer,
                        length, nullCount, offset);
            }

            protected TimeZoneInfo TimeZone { get; }
            protected TimeUnit Unit { get; }

            public Builder(TimeUnit unit = TimeUnit.Millisecond, string timezone = null)
                : base(new TimestampBuilder(new TimestampType(unit, timezone)))
            {
                Unit = unit;
                TimeZone = string.IsNullOrEmpty(timezone) 
                               ? TimeZoneInfo.Utc
                               : TimeZoneInfo.FindSystemTimeZoneById(timezone) ?? TimeZoneInfo.Utc;
            }

            protected override long ConvertTo(DateTimeOffset value)
            {
                // We must return the absolute time since the UNIX epoch while
                // respecting the timezone offset; the calculation is as follows:
                //
                // - Compute span between epoch and specified time (using correct offset)
                // - Compute number of units per tick

                var span = value.ToOffset(TimeZone.BaseUtcOffset) - Epoch;
                var ticks = span.Ticks;

                switch (Unit)
                {
                    case TimeUnit.Nanosecond:
                        return ticks / 100;
                    case TimeUnit.Microsecond:
                        return ticks / TimeSpan.TicksPerMillisecond / 1000;
                    case TimeUnit.Millisecond:
                        return ticks / TimeSpan.TicksPerMillisecond;
                    case TimeUnit.Second:
                        return ticks / TimeSpan.TicksPerSecond;
                    default:
                        throw new InvalidOperationException($"unsupported time unit <{Unit}>");
                }
            }
        }

        protected TimeZoneInfo TimeZone { get; }

        public TimestampArray(
            TimestampType type,
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset)
            : this(new ArrayData(type, length, nullCount, offset,
                new[] {nullBitmapBuffer, valueBuffer}))
        {
            TimeZone = type.Timezone != null 
                ? TimeZoneInfo.FindSystemTimeZoneById(type.Timezone) ?? TimeZoneInfo.Utc
                : TimeZoneInfo.Utc;
        }

        public TimestampArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Timestamp);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        public DateTimeOffset? GetTimestamp(int index)
        {
            if (IsNull(index))
            {
                return null;
            }

            Debug.Assert((Data.DataType as TimestampType) != null);

            var value = Values[index];
            var type = (TimestampType) Data.DataType;

            long ticks;

            switch (type.Unit)
            {
                case TimeUnit.Nanosecond:
                    ticks = value * 100;
                    break;
                case TimeUnit.Microsecond:
                    ticks = value * TimeSpan.TicksPerMillisecond * 1000;
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

            return new DateTimeOffset(
                Epoch.Ticks + TimeZone.BaseUtcOffset.Ticks + ticks,
                TimeZone.BaseUtcOffset);
        }
    }
}
