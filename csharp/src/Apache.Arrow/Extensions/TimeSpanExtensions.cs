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
    public static class TimeSpanExtensions
    {
        // Class methods
        public static TimeSpan FromNanoseconds(long nanoseconds) => TimeSpan.FromTicks(nanoseconds / 100);
        public static TimeSpan FromNanoseconds(int nanoseconds) => TimeSpan.FromTicks(nanoseconds / 100);

        public static TimeSpan FromMicroseconds(long microseconds) => TimeSpan.FromTicks(microseconds * 10);
        public static TimeSpan FromMicroseconds(int microseconds) => TimeSpan.FromTicks(microseconds * 10);

        public static TimeSpan FromMilliseconds(long milliseconds) => TimeSpan.FromMilliseconds(milliseconds);
        public static TimeSpan FromMilliseconds(int milliseconds) => TimeSpan.FromMilliseconds(milliseconds);

        public static TimeSpan FromSeconds(long seconds) => TimeSpan.FromSeconds(seconds);
        public static TimeSpan FromSeconds(int seconds) => TimeSpan.FromSeconds(seconds);

        // Instance methods

        /// <summary>
        /// Formats a TimeSpan into an ISO 8601 compliant time offset string.
        /// </summary>
        /// <param name="timeSpan">timeSpan to format</param>
        /// <returns>ISO 8601 offset string</returns>
        public static string ToTimeZoneOffsetString(this TimeSpan timeSpan)
        {
            string sign = timeSpan.Hours >= 0 ? "+" : "-";
            int hours = Math.Abs(timeSpan.Hours);
            int minutes = Math.Abs(timeSpan.Minutes);
            return sign + hours.ToString("00") + ":" + minutes.ToString("00");
        }

        public static long TotalMicroseconds(this TimeSpan timeSpan) => timeSpan.Ticks / 10;
        public static long TotalNanoseconds(this TimeSpan timeSpan) => timeSpan.Ticks * 100;
    }
}
