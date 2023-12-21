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
using Apache.Arrow.Scalars;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class IntervalScalarTests
    {
        private static TimeSpan[] timeSpans = new[] { TimeSpan.FromHours(0), TimeSpan.FromHours(-1), TimeSpan.FromHours(1), TimeSpan.FromHours(11.5), TimeSpan.FromHours(-11.5) };

        private static IEnumerable<object[]> ToDateTimeOffsets(IEnumerable<object[]> datetimes) =>
            from dtdata in datetimes
            from ts in timeSpans
            select new object[] { new DateTimeOffset((DateTime)dtdata[0], ts), dtdata[1], new DateTimeOffset((DateTime)dtdata[2], ts) };

#if NET6_0_OR_GREATER
        private static IEnumerable<object[]> ToDateOnlys(IEnumerable<object[]> datetimes) =>
            from dtdata in datetimes
            select new object[] { DateOnly.FromDateTime((DateTime)dtdata[0]), dtdata[1], DateOnly.FromDateTime((DateTime)dtdata[2]) };
#endif

        public class YearMonth
        {
            public static IEnumerable<object[]> GetGoodDateTimeArithmeticData()
            {
                yield return new object[] { new DateTime(2020, 3, 4, 12, 14, 15), new YearMonthInterval(-3), new DateTime(2019, 12, 4, 12, 14, 15) };
                yield return new object[] { new DateTime(2020, 1, 31), new YearMonthInterval(14), new DateTime(2021, 3, 31) };
                yield return new object[] { new DateTime(2020, 1, 1, 23, 59, 59), new YearMonthInterval(-95), new DateTime(2012, 2, 1, 23, 59, 59) };
            }

            public static IEnumerable<object[]> GetBadDateTimeArithmeticData()
            {
                yield return new object[] { new DateTime(2020, 3, 4, 0, 0, 1), new YearMonthInterval(-2), new DateTime(2019, 1, 4, 0, 0, 0) };
                yield return new object[] { new DateTime(2019, 12, 1), new YearMonthInterval(15), new DateTime(2021, 3, 2) };
            }

            public static IEnumerable<object[]> GetGoodDateTimeOffsetArithmeticData() => ToDateTimeOffsets(GetGoodDateTimeArithmeticData());
            public static IEnumerable<object[]> GetBadDateTimeOffsetArithmeticData() => ToDateTimeOffsets(GetBadDateTimeArithmeticData());

#if NET6_0_OR_GREATER
            public static IEnumerable<object[]> GetGoodDateOnlyArithmeticData() => ToDateOnlys(GetGoodDateTimeArithmeticData());
            public static IEnumerable<object[]> GetBadDateOnlyArithmeticData() => ToDateOnlys(GetBadDateTimeArithmeticData());
#endif

            [Fact]
            public void Equality()
            {
                Assert.Equal(new YearMonthInterval(10), new YearMonthInterval(1, -2));
                Assert.NotEqual(new YearMonthInterval(9), new YearMonthInterval(1, -2));
            }

            [Theory]
            [MemberData(nameof(GetGoodDateTimeArithmeticData))]
            public void CorrectDateTimeMath(DateTime a, YearMonthInterval b, DateTime c)
            {
                Assert.Equal(c, a + b);
                Assert.Equal(c, b + a);
                Assert.Equal(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetBadDateTimeArithmeticData))]
            public void IncorrectDateTimeMath(DateTime a, YearMonthInterval b, DateTime c)
            {
                Assert.NotEqual(c, a + b);
                Assert.NotEqual(c, b + a);
                Assert.NotEqual(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetGoodDateTimeOffsetArithmeticData))]
            public void CorrectDateTimeOffsetMath(DateTimeOffset a, YearMonthInterval b, DateTimeOffset c)
            {
                Assert.Equal(c, a + b);
                Assert.Equal(c, b + a);
                Assert.Equal(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetBadDateTimeOffsetArithmeticData))]
            public void IncorrectDateTimeOffsetMath(DateTimeOffset a, YearMonthInterval b, DateTimeOffset c)
            {
                Assert.NotEqual(c, a + b);
                Assert.NotEqual(c, b + a);
                Assert.NotEqual(a, c - b);
            }

#if NET6_0_OR_GREATER
            [Theory]
            [MemberData(nameof(GetGoodDateOnlyArithmeticData))]
            public void CorrectDateOnlyMath(DateOnly a, YearMonthInterval b, DateOnly c)
            {
                Assert.Equal(c, a + b);
                Assert.Equal(c, b + a);
                Assert.Equal(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetBadDateOnlyArithmeticData))]
            public void IncorrectDateOnlyMath(DateOnly a, YearMonthInterval b, DateOnly c)
            {
                Assert.NotEqual(c, a + b);
                Assert.NotEqual(c, b + a);
                Assert.NotEqual(a, c - b);
            }
#endif
        }

        public class DayTime
        {
            public static IEnumerable<object[]> GetGoodDateTimeArithmeticData()
            {
                yield return new object[] { new DateTime(2020, 3, 4, 12, 14, 15), new DayTimeInterval(-3, 123456), new DateTime(2020, 3, 1, 12, 16, 18, 456) };
                yield return new object[] { new DateTime(2020, 1, 31), new DayTimeInterval(14, 0), new DateTime(2020, 2, 14) };
                yield return new object[] { new DateTime(2020, 1, 1, 23, 59, 59), new DayTimeInterval(300, -300000), new DateTime(2020, 10, 27, 23, 54, 59) };
            }

            public static IEnumerable<object[]> GetBadDateTimeArithmeticData()
            {
                yield return new object[] { new DateTime(2020, 3, 4, 0, 0, 1), new DayTimeInterval(-2, 0), new DateTime(2019, 1, 4, 0, 0, 0) };
                yield return new object[] { new DateTime(2019, 12, 1), new DayTimeInterval(15, 0), new DateTime(2021, 3, 2) };
            }

            public static IEnumerable<object[]> GetGoodDateTimeOffsetArithmeticData() => ToDateTimeOffsets(GetGoodDateTimeArithmeticData());
            public static IEnumerable<object[]> GetBadDateTimeOffsetArithmeticData() => ToDateTimeOffsets(GetBadDateTimeArithmeticData());

            [Fact]
            public void Equality()
            {
                Assert.Equal(new DayTimeInterval(10, -1), new DayTimeInterval(10, -1));
                Assert.NotEqual(new DayTimeInterval(10, 0), new DayTimeInterval(9, 86400*1000));
            }

            [Theory]
            [MemberData(nameof(GetGoodDateTimeArithmeticData))]
            public void CorrectDateTimeMath(DateTime a, DayTimeInterval b, DateTime c)
            {
                Assert.Equal(c, a + b);
                Assert.Equal(c, b + a);
                Assert.Equal(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetBadDateTimeArithmeticData))]
            public void IncorrectDateTimeMath(DateTime a, DayTimeInterval b, DateTime c)
            {
                Assert.NotEqual(c, a + b);
                Assert.NotEqual(c, b + a);
                Assert.NotEqual(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetGoodDateTimeOffsetArithmeticData))]
            public void CorrectDateTimeOffsetMath(DateTimeOffset a, DayTimeInterval b, DateTimeOffset c)
            {
                Assert.Equal(c, a + b);
                Assert.Equal(c, b + a);
                Assert.Equal(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetBadDateTimeOffsetArithmeticData))]
            public void IncorrectDateTimeOffsetMath(DateTimeOffset a, DayTimeInterval b, DateTimeOffset c)
            {
                Assert.NotEqual(c, a + b);
                Assert.NotEqual(c, b + a);
                Assert.NotEqual(a, c - b);
            }
        }

        public class MonthDayNanosecond
        {
            public static IEnumerable<object[]> GetGoodDateTimeArithmeticData()
            {
                yield return new object[] { new DateTime(2020, 3, 4, 12, 14, 15), new MonthDayNanosecondInterval(-3, 0, 100), new DateTime(2019, 12, 4, 12, 14, 15).AddTicks(1) };
                yield return new object[] { new DateTime(2020, 1, 31), new MonthDayNanosecondInterval(14, -2, -200), new DateTime(2021, 3, 29).AddTicks(-2) };
                yield return new object[] { new DateTime(2020, 1, 1, 23, 59, 59), new MonthDayNanosecondInterval(-95, 0, 0), new DateTime(2012, 2, 1, 23, 59, 59) };
            }

            public static IEnumerable<object[]> GetBadDateTimeArithmeticData()
            {
                yield return new object[] { new DateTime(2020, 3, 4, 0, 0, 1), new MonthDayNanosecondInterval(-2, 0, 0), new DateTime(2019, 1, 4, 0, 0, 0) };
                yield return new object[] { new DateTime(2019, 12, 1), new MonthDayNanosecondInterval(15, 0, 0), new DateTime(2021, 3, 2) };
            }

            public static IEnumerable<object[]> GetGoodDateTimeOffsetArithmeticData() => ToDateTimeOffsets(GetGoodDateTimeArithmeticData());
            public static IEnumerable<object[]> GetBadDateTimeOffsetArithmeticData() => ToDateTimeOffsets(GetBadDateTimeArithmeticData());

            [Fact]
            public void Equality()
            {
                Assert.Equal(new MonthDayNanosecondInterval(1, -2, 3), new MonthDayNanosecondInterval(1, -2, 3));
                Assert.NotEqual(new MonthDayNanosecondInterval(1, -2, 3), new MonthDayNanosecondInterval(1, -2, 4));
            }

            [Theory]
            [MemberData(nameof(GetGoodDateTimeArithmeticData))]
            public void CorrectDateTimeMath(DateTime a, MonthDayNanosecondInterval b, DateTime c)
            {
                Assert.Equal(c, a + b);
                Assert.Equal(c, b + a);
                Assert.Equal(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetBadDateTimeArithmeticData))]
            public void IncorrectDateTimeMath(DateTime a, MonthDayNanosecondInterval b, DateTime c)
            {
                Assert.NotEqual(c, a + b);
                Assert.NotEqual(c, b + a);
                Assert.NotEqual(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetGoodDateTimeOffsetArithmeticData))]
            public void CorrectDateTimeOffsetMath(DateTimeOffset a, MonthDayNanosecondInterval b, DateTimeOffset c)
            {
                Assert.Equal(c, a + b);
                Assert.Equal(c, b + a);
                Assert.Equal(a, c - b);
            }

            [Theory]
            [MemberData(nameof(GetBadDateTimeOffsetArithmeticData))]
            public void IncorrectDateTimeOffsetMath(DateTimeOffset a, MonthDayNanosecondInterval b, DateTimeOffset c)
            {
                Assert.NotEqual(c, a + b);
                Assert.NotEqual(c, b + a);
                Assert.NotEqual(a, c - b);
            }
        }
    }
}
