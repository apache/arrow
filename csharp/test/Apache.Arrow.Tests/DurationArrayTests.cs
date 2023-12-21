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
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class DurationArrayTests
    {
        private const long TicksPerMicrosecond = 10;

        private static readonly TimeSpan?[] _exampleTimeSpans =
        {
            null,
            TimeSpan.FromDays(10.5),
            TimeSpan.FromHours(10.5),
            TimeSpan.FromMinutes(10.5),
            TimeSpan.FromSeconds(10.5),
            TimeSpan.FromMilliseconds(10.5),
            TimeSpan.FromTicks(11),
        };

        private static readonly long?[] _exampleDurations =
        {
            null,
            1,
            1000,
            1000000,
            1000000000,
            1000000000000,
        };

        private static readonly DurationType[] _durationTypes =
        {
            DurationType.Second,
            DurationType.Millisecond,
            DurationType.Microsecond,
            DurationType.Nanosecond,
        };

        public static IEnumerable<object[]> GetTimeSpansData() =>
            from timeSpan in _exampleTimeSpans
            from type in _durationTypes
            where type.Unit >= RequiredPrecision(timeSpan)
            select new object[] { timeSpan, type };

        public static IEnumerable<object[]> GetDurationsData() =>
            from duration in _exampleDurations
            from type in _durationTypes
            select new object[] { duration, type };

        static TimeUnit RequiredPrecision(TimeSpan? timeSpan)
        {
            if (timeSpan == null) { return TimeUnit.Second; }
            if ((timeSpan.Value.Ticks % TicksPerMicrosecond) > 0) { return TimeUnit.Nanosecond; }
#if NET5_0_OR_GREATER
            if (timeSpan.Value.Microseconds > 0) { return TimeUnit.Microsecond; }
#else
            if ((timeSpan.Value.Ticks % (TicksPerMicrosecond * 1000)) > 0) { return TimeUnit.Microsecond; }
#endif
            if (timeSpan.Value.Milliseconds > 0) { return TimeUnit.Millisecond; }
            return TimeUnit.Second;
        }

        public class AppendNull
        {
            [Fact]
            public void AppendThenGetGivesNull()
            {
                // Arrange
                var builder = new DurationArray.Builder(DurationType.Millisecond);

                // Act
                builder = builder.AppendNull();

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Null(array.GetValue(0));
                Assert.Null(array.GetTimeSpan(0));
            }
        }

        public class AppendTimeSpan
        {
            [Theory]
            [MemberData(nameof(GetTimeSpansData), MemberType = typeof(DurationArrayTests))]
            public void AppendTimeSpanGivesSameTimeSpan(TimeSpan? timeSpan, DurationType type)
            {
                // Arrange
                var builder = new DurationArray.Builder(type);

                // Act
                builder = builder.Append(timeSpan);

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Equal(timeSpan, array.GetTimeSpan(0));

                IReadOnlyList<TimeSpan?> asList = array;
                Assert.Equal(1, asList.Count);
                Assert.Equal(timeSpan, asList[0]);
            }
        }

        public class AppendDuration
        {
            [Theory]
            [MemberData(nameof(GetDurationsData), MemberType = typeof(DurationArrayTests))]
            public void AppendDurationGivesSameDuration(long? duration, DurationType type)
            {
                // Arrange
                var builder = new DurationArray.Builder(type);

                // Act
                builder = builder.Append(duration);

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Equal(duration, array.GetValue(0));
            }
        }
    }
}
