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
    public struct YearMonthInterval : IEquatable<YearMonthInterval>
    {
        private const int MonthsPerYear = 12;

        [FieldOffset(0)]
        public readonly int Months;

        public YearMonthInterval(int totalMonths)
        {
            Months = totalMonths;
        }

        public YearMonthInterval(int years, int months)
        {
            Months = years * MonthsPerYear + months;
        }

        public override bool Equals(object obj) => obj switch
        {
            DayTimeInterval interval => Equals(interval),
            _ => false,
        };

        public override int GetHashCode() => Months;

        public bool Equals(YearMonthInterval interval)
        {
            return this.Months == interval.Months;
        }

        public static DateTime operator +(DateTime dateTime, YearMonthInterval interval) => dateTime.AddMonths(interval.Months);
        public static DateTime operator +(YearMonthInterval interval, DateTime dateTime) => dateTime.AddMonths(interval.Months);
        public static DateTime operator -(DateTime dateTime, YearMonthInterval interval) => dateTime.AddMonths(-interval.Months);

        public static DateTimeOffset operator +(DateTimeOffset dateTime, YearMonthInterval interval) => dateTime.AddMonths(interval.Months);
        public static DateTimeOffset operator +(YearMonthInterval interval, DateTimeOffset dateTime) => dateTime.AddMonths(interval.Months);
        public static DateTimeOffset operator -(DateTimeOffset dateTime, YearMonthInterval interval) => dateTime.AddMonths(-interval.Months);

#if NET6_0_OR_GREATER
        public static DateOnly operator +(DateOnly date, YearMonthInterval interval) => date.AddMonths(interval.Months);
        public static DateOnly operator +(YearMonthInterval interval, DateOnly date) => date.AddMonths(interval.Months);
        public static DateOnly operator -(DateOnly date, YearMonthInterval interval) => date.AddMonths(-interval.Months);
#endif
    }
}
