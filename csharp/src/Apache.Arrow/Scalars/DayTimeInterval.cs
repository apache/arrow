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
    public struct DayTimeInterval : IEquatable<DayTimeInterval>
    {
        [FieldOffset(0)]
        public readonly int Days;

        [FieldOffset(4)]
        public readonly int Milliseconds;

        public DayTimeInterval(int days, int milliseconds)
        {
            Days = days;
            Milliseconds = milliseconds;
        }

        public override bool Equals(object obj) => obj switch
        {
            DayTimeInterval interval => Equals(interval),
            _ => false,
        };

        public override int GetHashCode() => unchecked((17 + Days) * 23 + Milliseconds);

        public bool Equals(DayTimeInterval interval)
        {
            return this.Days == interval.Days && this.Milliseconds == interval.Milliseconds;
        }

        public static DateTime operator +(DateTime dateTime, DayTimeInterval interval)
            => dateTime.AddDays(interval.Days).AddMilliseconds(interval.Milliseconds);
        public static DateTime operator +(DayTimeInterval interval, DateTime dateTime)
            => dateTime.AddDays(interval.Days).AddMilliseconds(interval.Milliseconds);
        public static DateTime operator -(DateTime dateTime, DayTimeInterval interval)
            => dateTime.AddDays(-interval.Days).AddMilliseconds(-interval.Milliseconds);

        public static DateTimeOffset operator +(DateTimeOffset dateTime, DayTimeInterval interval)
            => dateTime.AddDays(interval.Days).AddMilliseconds(interval.Milliseconds);
        public static DateTimeOffset operator +(DayTimeInterval interval, DateTimeOffset dateTime)
            => dateTime.AddDays(interval.Days).AddMilliseconds(interval.Milliseconds);
        public static DateTimeOffset operator -(DateTimeOffset dateTime, DayTimeInterval interval)
            => dateTime.AddDays(-interval.Days).AddMilliseconds(-interval.Milliseconds);
    }
}
