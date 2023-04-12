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
using System.Linq;
using System.Text.RegularExpressions;

namespace Apache.Arrow.Types
{
    public sealed class TimestampType : FixedWidthType
    {
        public static readonly TimestampType Default = new TimestampType(TimeUnit.Millisecond, "+00:00");

        public override ArrowTypeId TypeId => ArrowTypeId.Timestamp;
        public override string Name => "timestamp";
        public override int BitWidth => 64;

        public TimeUnit Unit { get; }
        public string Timezone { get; }
        public TimeZoneInfo TimeZoneInfo => ParseTimeZone(Timezone);

        public bool IsTimeZoneAware => !string.IsNullOrWhiteSpace(Timezone);

        public TimestampType(
            TimeUnit unit = TimeUnit.Millisecond,
            string timezone = default)
        {
            Unit = unit;
            Timezone = timezone;
        }

        public TimestampType(
            TimeUnit unit = TimeUnit.Millisecond,
            TimeZoneInfo timezone = default)
        {
            Unit = unit;
            Timezone = timezone?.BaseUtcOffset.ToTimeZoneOffsetString();
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);

        // Static methods
        public static TimeZoneInfo? ParseTimeZone(string tz)
        {
            if (string.IsNullOrEmpty(tz)) return null;

            // Static offset like +00:00 or -00:00
            if (Regex.IsMatch(tz, @"^([+-]\d{2}):([0-9]\d)$"))
            {
                switch (tz)
                {
                    case "-00:00":
                    case "+00:00":
                        return TimeZoneInfo.Utc;
                    default:
#if NETCOREAPP
                        string[] offsetParts = tz.Split(':');
                        char sign = offsetParts[0].First();
                        int hours = int.Parse(offsetParts[0].Substring(1));
                        int minutes = int.Parse(offsetParts[1]);

                        TimeSpan baseOffset;

                        if (sign == '+') baseOffset = new TimeSpan(hours, minutes, 0);
                        else baseOffset = TimeSpan.FromMinutes(-1 * (hours * 60 + minutes));

                        return TimeZoneInfo.CreateCustomTimeZone(tz, baseOffset, tz, tz);
#else
                        return null;
#endif
                }
            }
            else // or IANA like Europe/Paris
            {
                return TimeZoneInfo.FindSystemTimeZoneById(tz);
            }
        }
    }
}
