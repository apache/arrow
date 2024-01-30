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
    public class TimeOnlyTests
    {
        private static IEnumerable<object[]> GetTimeOnlyData(params TimeUnit[] units) =>
            from time in TestDateAndTimeData.ExampleTimes
            from unit in units
            select new object[] { TimeOnly.FromTimeSpan(time), unit };

        public class Time32
        {
            public static IEnumerable<object[]> GetTestData => GetTimeOnlyData(TimeUnit.Second, TimeUnit.Millisecond);

            [Fact]
            public void AppendThenGetGivesNull()
            {
                // Arrange
                var builder = new Time32Array.Builder();

                // Act
                builder = builder.AppendNull();

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Null(array.GetTime(0));
                Assert.Null(array.GetValue(0));
            }

            [Theory]
            [MemberData(nameof(GetTestData))]
            public void AppendTimeGivesSameTime(TimeOnly time, TimeUnit timeUnit)
            {
                // Arrange
                var builder = new Time32Array.Builder(timeUnit);
                var expectedTime = time;
                int expectedMilliseconds = (int)(time.Ticks / TimeSpan.TicksPerMillisecond);

                // Act
                builder = builder.Append(time);

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Equal(expectedTime, array.GetTime(0));
                Assert.Equal(expectedMilliseconds, array.GetMilliSeconds(0));
            }
        }

        public class Time64
        {
            public static IEnumerable<object[]> GetTestData => GetTimeOnlyData(TimeUnit.Microsecond, TimeUnit.Nanosecond);

            [Fact]
            public void AppendThenGetGivesNull()
            {
                // Arrange
                var builder = new Time64Array.Builder();

                // Act
                builder = builder.AppendNull();

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Null(array.GetTime(0));
                Assert.Null(array.GetValue(0));
            }

            [Theory]
            [MemberData(nameof(GetTestData))]
            public void AppendTimeGivesSameTime(TimeOnly time, TimeUnit timeUnit)
            {
                // Arrange
                var builder = new Time64Array.Builder(timeUnit);
                var expectedTime = time;
                long expectedNanoseconds = time.Ticks * TimeSpan.NanosecondsPerTick;

                // Act
                builder = builder.Append(time);

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Equal(expectedTime, array.GetTime(0));
                Assert.Equal(expectedNanoseconds, array.GetNanoSeconds(0));
            }
        }
    }
}
