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
using Xunit;

namespace Apache.Arrow.Tests
{
    public class Date32ArrayTests
    {
        public static IEnumerable<object[]> GetDatesData() =>
            TestDateAndTimeData.ExampleDates.Select(d => new object[] { d });

        public static IEnumerable<object[]> GetDateTimesData() =>
            TestDateAndTimeData.ExampleDateTimes.Select(dt => new object[] { dt });

        public static IEnumerable<object[]> GetDateTimeOffsetsData() =>
            TestDateAndTimeData.ExampleDateTimeOffsets.Select(dto => new object[] { dto });

        public class AppendNull
        {
            [Fact]
            public void AppendThenGetGivesNull()
            {
                // Arrange
                var builder = new Date32Array.Builder();

                // Act
                builder = builder.AppendNull();

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Null(array.GetDateTime(0));
                Assert.Null(array.GetDateTimeOffset(0));
                Assert.Null(array.GetValue(0));
            }
        }

        public class AppendDateTime
        {
            [Theory]
            [MemberData(nameof(GetDatesData), MemberType = typeof(Date32ArrayTests))]
            public void AppendDateGivesSameDate(DateTime date)
            {
                // Arrange
                var builder = new Date32Array.Builder();
                var expectedDateTime = date;
                var expectedDateTimeOffset =
                    new DateTimeOffset(DateTime.SpecifyKind(date, DateTimeKind.Unspecified), TimeSpan.Zero);
                int expectedValue = (int)date.Subtract(new DateTime(1970, 1, 1)).TotalDays;

                // Act
                builder = builder.Append(date);

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Equal(expectedDateTime, array.GetDateTime(0));
                Assert.Equal(expectedDateTimeOffset, array.GetDateTimeOffset(0));
                Assert.Equal(expectedValue, array.GetValue(0));
            }

            [Theory]
            [MemberData(nameof(GetDateTimesData), MemberType = typeof(Date32ArrayTests))]
            public void AppendWithTimeGivesSameWithTimeIgnored(DateTime dateTime)
            {
                // Arrange
                var builder = new Date32Array.Builder();
                var expectedDateTime = dateTime.Date;
                var expectedDateTimeOffset =
                    new DateTimeOffset(DateTime.SpecifyKind(dateTime.Date, DateTimeKind.Unspecified), TimeSpan.Zero);
                int expectedValue = (int)dateTime.Date.Subtract(new DateTime(1970, 1, 1)).TotalDays;

                // Act
                builder = builder.Append(dateTime);

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Equal(expectedDateTime, array.GetDateTime(0));
                Assert.Equal(expectedDateTimeOffset, array.GetDateTimeOffset(0));
                Assert.Equal(expectedValue, array.GetValue(0));
            }
        }

        public class AppendDateTimeOffset
        {
            [Theory]
            [MemberData(nameof(GetDateTimeOffsetsData), MemberType = typeof(Date32ArrayTests))]
            public void AppendGivesUtcDate(DateTimeOffset dateTimeOffset)
            {
                // Arrange
                var builder = new Date32Array.Builder();
                var expectedDateTime = dateTimeOffset.UtcDateTime.Date;
                var expectedDateTimeOffset = new DateTimeOffset(dateTimeOffset.UtcDateTime.Date, TimeSpan.Zero);
                int expectedValue = (int)dateTimeOffset.UtcDateTime.Date.Subtract(new DateTime(1970, 1, 1)).TotalDays;

                // Act
                builder = builder.Append(dateTimeOffset);

                // Assert
                var array = builder.Build();
                Assert.Equal(1, array.Length);
                Assert.Equal(expectedDateTime, array.GetDateTime(0));
                Assert.Equal(expectedDateTimeOffset, array.GetDateTimeOffset(0));
                Assert.Equal(expectedValue, array.GetValue(0));
            }
        }
    }
}
