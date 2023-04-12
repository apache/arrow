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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests.Types
{
    public class TimestampTypeTests
    {
        public class Utils
        {
            [Fact]
            public void ParseTimeZone_ReturnsNull_ForNullInput()
            {
                // Act
                TimeZoneInfo result = TimestampType.ParseTimeZone(null);

                // Assert
                Assert.Null(result);
            }

            [Fact]
            public void ParseTimeZone_ReturnsNull_ForEmptyInput()
            {
                // Act
                TimeZoneInfo result = TimestampType.ParseTimeZone("");

                // Assert
                Assert.Null(result);
            }

            [Fact]
            public void ParseTimeZone_ReturnsTimeZoneInfo_ZeroOffset()
            {
                // Act
                TimeZoneInfo positive = TimestampType.ParseTimeZone("+00:00");

                // Assert
                Assert.NotNull(positive);
                Assert.Equal(TimeZoneInfo.Utc, positive);

                // Act
                TimeZoneInfo negative = TimestampType.ParseTimeZone("-00:00");

                // Assert
                Assert.NotNull(negative);
                Assert.Equal(TimeZoneInfo.Utc, negative);

                // Act
                TimeZoneInfo utc = TimestampType.ParseTimeZone("UTC");

                // Assert
                Assert.NotNull(utc);
                Assert.Equal(TimeZoneInfo.Utc, utc);
            }

            [Fact]
            public void ParseTimeZone_ReturnsTimeZoneInfo_ForValidStaticOffsetInput()
            {
                // Act
                string tz = "+12:01";
                TimeSpan offset = new TimeSpan(12, 1, 0);
                TimeZoneInfo result = TimestampType.ParseTimeZone(tz);

                // Assert
#if NETCOREAPP
                Assert.NotNull(result);
                Assert.Equal(TimeZoneInfo.CreateCustomTimeZone(tz, offset, tz, tz), result);
                Assert.Equal(offset, result.BaseUtcOffset);
#else
                Assert.Null(result);
#endif
            }

            [Fact]
            public void ParseTimeZone_ReturnsTimeZoneInfo_ForValidNegativeStaticOffsetInput()
            {
                // Act
                string tz = "-12:01";
                int hours = 12;
                int minutes = 1;
                TimeSpan offset = TimeSpan.FromMinutes(-1 * (hours * 60 + minutes));
                TimeZoneInfo result = TimestampType.ParseTimeZone(tz);

                // Assert
#if NETCOREAPP
                Assert.NotNull(result);
                Assert.Equal(TimeZoneInfo.CreateCustomTimeZone(tz, offset, tz, tz), result);
                Assert.Equal(offset, result.BaseUtcOffset);
#else
                Assert.Null(result);
#endif
            }

            [Fact]
            public void ParseTimeZone_ReturnsTimeZoneInfo_ForValidIANAInput()
            {
                // Act
                TimeZoneInfo result = TimestampType.ParseTimeZone("America/New_York");
                var expected = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(expected.BaseUtcOffset, result.BaseUtcOffset);
            }

            [Fact]
            public void ParseTimeZone_ReturnsTimeZoneInfo_ForValidWindowsInput()
            {
                // Act
                TimeZoneInfo result = TimestampType.ParseTimeZone("Eastern Standard Time");
                var expected = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(expected.BaseUtcOffset, result.BaseUtcOffset);
            }
        }
    }
}
