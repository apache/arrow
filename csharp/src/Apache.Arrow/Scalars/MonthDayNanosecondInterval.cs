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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Scalars
{
    [StructLayout(LayoutKind.Explicit)]
    public struct MonthDayNanosecondInterval : IEquatable<MonthDayNanosecondInterval>
    {
        [FieldOffset(0)]
        public readonly int Months;

        [FieldOffset(4)]
        public readonly int Days;

        [FieldOffset(8)]
        public readonly long Nanoseconds;

        public MonthDayNanosecondInterval(int months, int days, long nanoseconds)
        {
            Months = months;
            Days = days;
            Nanoseconds = nanoseconds;
        }

        public override bool Equals(object obj) => obj switch
        {
            MonthDayNanosecondInterval interval => Equals(interval),
            _ => false,
        };

        public override int GetHashCode() => unchecked(((17 + Months) * 23 + Days) * 23 + (int)Nanoseconds);

        public bool Equals(MonthDayNanosecondInterval interval)
        {
            return this.Months == interval.Months && this.Days == interval.Days && this.Nanoseconds == interval.Nanoseconds;
        }

        public static DateTime operator +(DateTime dateTime, MonthDayNanosecondInterval interval)
            => dateTime.AddMonths(interval.Months).AddDays(interval.Days).AddTicks(interval.Nanoseconds / 100);
        public static DateTime operator +(MonthDayNanosecondInterval interval, DateTime dateTime)
            => dateTime.AddMonths(interval.Months).AddDays(interval.Days).AddTicks(interval.Nanoseconds / 100);
        public static DateTime operator -(DateTime dateTime, MonthDayNanosecondInterval interval)
            => dateTime.AddMonths(-interval.Months).AddDays(-interval.Days).AddTicks(-interval.Nanoseconds / 100);

        public static DateTimeOffset operator +(DateTimeOffset dateTime, MonthDayNanosecondInterval interval)
            => dateTime.AddMonths(interval.Months).AddDays(interval.Days).AddTicks(interval.Nanoseconds / 100);
        public static DateTimeOffset operator +(MonthDayNanosecondInterval interval, DateTimeOffset dateTime)
            => dateTime.AddMonths(interval.Months).AddDays(interval.Days).AddTicks(interval.Nanoseconds / 100);
        public static DateTimeOffset operator -(DateTimeOffset dateTime, MonthDayNanosecondInterval interval)
            => dateTime.AddMonths(-interval.Months).AddDays(-interval.Days).AddTicks(-interval.Nanoseconds / 100);
    }
}
