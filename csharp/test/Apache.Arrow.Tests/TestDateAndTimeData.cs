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

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// The <see cref="TestDateAndTimeData"/> class holds example dates and times useful for testing.
    /// </summary>
    internal static class TestDateAndTimeData
    {
        private static readonly DateTime _earliestDate = new DateTime(1, 1, 1);
        private static readonly DateTime _latestDate = new DateTime(9999, 12, 31);

        private static readonly DateTime[] _exampleDates =
        {
            _earliestDate, new DateTime(1969, 12, 31), new DateTime(1970, 1, 1), new DateTime(1970, 1, 2),
            new DateTime(1972, 6, 30), new DateTime(2015, 6, 30), new DateTime(2016, 12, 31), new DateTime(2020, 2, 29),
            new DateTime(2020, 7, 1), _latestDate,
        };

        private static readonly TimeSpan[] _exampleTimes =
        {
            new TimeSpan(0, 0, 1), new TimeSpan(12, 0, 0), new TimeSpan(23, 59, 59),
        };

        private static readonly DateTimeKind[] _exampleKinds =
        {
            DateTimeKind.Local, DateTimeKind.Unspecified, DateTimeKind.Utc,
        };

        private static readonly TimeSpan[] _exampleOffsets =
        {
            TimeSpan.FromHours(-2),
            TimeSpan.Zero,
            TimeSpan.FromHours(2),
        };

        /// <summary>
        /// Gets a collection of example dates (i.e. with a zero time component), of all different kinds.
        /// </summary>
        public static IEnumerable<DateTime> ExampleDates =>
            from date in _exampleDates
            from kind in _exampleKinds
            select DateTime.SpecifyKind(date, kind);

        /// <summary>
        /// Gets a collection of example times
        /// </summary>
        public static IEnumerable<TimeSpan> ExampleTimes => _exampleTimes;

        /// <summary>
        /// Gets a collection of example date/times, of all different kinds.
        /// </summary>
        public static IEnumerable<DateTime> ExampleDateTimes =>
            from date in _exampleDates
            from time in _exampleTimes
            from kind in _exampleKinds
            select DateTime.SpecifyKind(date.Add(time), kind);

        /// <summary>
        /// Gets a collection of example date time offsets.
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<DateTimeOffset> ExampleDateTimeOffsets =>
            from date in _exampleDates
            from time in _exampleTimes
            from offset in _exampleOffsets
            where !(date == _earliestDate && offset.Ticks > 0)
            where !(date == _latestDate && offset.Ticks < 0)
            select new DateTimeOffset(date.Add(time), offset);
    }
}
