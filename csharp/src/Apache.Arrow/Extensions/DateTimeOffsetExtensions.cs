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

namespace Apache.Arrow
{
    public static class DateTimeOffsetExtensions
    {
        // Class methods
        public static readonly DateTimeOffset UnixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public static DateTimeOffset FromUnixTimeSeconds(long seconds) => UnixEpoch.AddSeconds(seconds);
        public static DateTimeOffset FromUnixTimeSeconds(int seconds) => UnixEpoch.AddSeconds(seconds);

        public static DateTimeOffset FromUnixTimeMilliseconds(long milliseconds) => UnixEpoch.AddMilliseconds(milliseconds);
        public static DateTimeOffset FromUnixTimeMilliseconds(int milliseconds) => UnixEpoch.AddMilliseconds(milliseconds);

        public static DateTimeOffset FromUnixTimeMicroseconds(long micros) => UnixEpoch.AddTicks(micros * 10);
        public static DateTimeOffset FromUnixTimeMicroseconds(int micros) => UnixEpoch.AddTicks(micros * 10);

        public static DateTimeOffset FromUnixTimeNanoseconds(long nanos) => UnixEpoch.AddTicks(nanos / 100);
        public static DateTimeOffset FromUnixTimeNanoseconds(int nanos) => UnixEpoch.AddTicks(nanos / 100);

        // Instance methods
        public static long ToUnixTimeSeconds(this DateTimeOffset date) => date.ToUnixTimeSeconds();
        public static long ToUnixTimeMilliseconds(this DateTimeOffset date) => date.ToUnixTimeMilliseconds();
        public static long ToUnixTimeMicroseconds(this DateTimeOffset date) => (date.UtcTicks - UnixEpoch.Ticks) / 10;
        public static long ToUnixTimeNanoseconds(this DateTimeOffset date) => (date.UtcTicks - UnixEpoch.Ticks) * 100;

        public static DateTimeOffset Truncate(this DateTimeOffset dateTimeOffset, TimeSpan offset)
        {
            if (offset == TimeSpan.Zero)
            {
                return dateTimeOffset;
            }

            if (dateTimeOffset == DateTimeOffset.MinValue || dateTimeOffset == DateTimeOffset.MaxValue)
            {
                return dateTimeOffset;
            }

            return dateTimeOffset.AddTicks(-(dateTimeOffset.Ticks % offset.Ticks));
        }
            
    }
}
