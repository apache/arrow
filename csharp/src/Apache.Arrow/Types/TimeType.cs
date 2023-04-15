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

namespace Apache.Arrow.Types
{
    public enum TimeUnit
    {
        Second,
        Millisecond,
        Microsecond,
        Nanosecond
    }

    public abstract class TimeType: FixedWidthType
    {
        public TimeUnit Unit { get; }

        protected TimeType(TimeUnit unit)
        {
            Unit = unit;
        }
    }

    public class Convert
    {
        public static readonly DateTime EpochDateTime = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        public static readonly DateTimeOffset EpochDateTimeOffset = new(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // Ticks to DateTime
        public static DateTime UnixDaysToDateTime(int days) => EpochDateTime.AddDays(days);
        public static DateTime UnixDaysToDateTime(long days) => EpochDateTime.AddDays(days);

        public static DateTimeOffset UnixDaysToDateTimeOffset(int days) => EpochDateTimeOffset.AddDays(days);
        public static DateTimeOffset UnixDaysToDateTimeOffset(long days) => EpochDateTimeOffset.AddDays(days);

        public static DateTimeOffset UnixSecondsToDateTimeOffset(int seconds) => DateTimeOffset.FromUnixTimeSeconds(seconds);
        public static DateTimeOffset UnixSecondsToDateTimeOffset(long seconds) => DateTimeOffset.FromUnixTimeSeconds(seconds);

        public static DateTimeOffset UnixMillisecondsToDateTimeOffset(int millis) => DateTimeOffset.FromUnixTimeMilliseconds(millis);
        public static DateTimeOffset UnixMillisecondsToDateTimeOffset(long millis) => DateTimeOffset.FromUnixTimeMilliseconds(millis);

        public static DateTimeOffset UnixMicrosecondsToDateTimeOffset(int micros) => EpochDateTimeOffset.AddTicks(micros * 10);
        public static DateTimeOffset UnixMicrosecondsToDateTimeOffset(long micros) => EpochDateTimeOffset.AddTicks(micros * 10);

        public static DateTimeOffset UnixNanosecondsToDateTimeOffset(int nanos) => EpochDateTimeOffset.AddTicks(nanos / 100);
        public static DateTimeOffset UnixNanosecondsToDateTimeOffset(long nanos) => EpochDateTimeOffset.AddTicks(nanos / 100);

        // Ticks to TimeSpan
        public static TimeSpan SecondsToTimeSpan(int seconds) => TimeSpan.FromSeconds(seconds);
        public static TimeSpan SecondsToTimeSpan(long seconds) => TimeSpan.FromSeconds(seconds);

        public static TimeSpan MillisecondsToTimeSpan(int millis) => TimeSpan.FromMilliseconds(millis);
        public static TimeSpan MillisecondsToTimeSpan(long millis) => TimeSpan.FromMilliseconds(millis);

        public static TimeSpan MicrosecondsToTimeSpan(int micros) => TimeSpan.FromTicks(micros * 10);
        public static TimeSpan MicrosecondsToTimeSpan(long micros) => TimeSpan.FromTicks(micros * 10);

        public static TimeSpan NanosecondsToTimeSpan(int nanos) => TimeSpan.FromTicks(nanos / 100);
        public static TimeSpan NanosecondsToTimeSpan(long nanos) => TimeSpan.FromTicks(nanos / 100);
    }
}
