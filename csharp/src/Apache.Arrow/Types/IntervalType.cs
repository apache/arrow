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

namespace Apache.Arrow.Types
{
    public sealed class IntervalType : FixedWidthType
    {
        public static readonly IntervalType YearMonth = new IntervalType(IntervalUnit.YearMonth);
        public static readonly IntervalType DayTime = new IntervalType(IntervalUnit.DayTime);
        public static readonly IntervalType MonthDayNanosecond = new IntervalType(IntervalUnit.MonthDayNanosecond);
        private static readonly IntervalType[] _types = new IntervalType[] { YearMonth, DayTime, MonthDayNanosecond };

        public override ArrowTypeId TypeId => ArrowTypeId.Interval;
        public override string Name => "interval";
        public override int BitWidth => Unit switch
        {
            IntervalUnit.YearMonth => 32,
            IntervalUnit.DayTime => 64,
            IntervalUnit.MonthDayNanosecond => 128,
            _ => throw new InvalidOperationException($"Unsupported interval unit {Unit}"),
        };

        public IntervalUnit Unit { get; }

        public IntervalType(IntervalUnit unit = IntervalUnit.YearMonth)
        {
            Unit = unit;
        }

        public override void Accept(IArrowTypeVisitor visitor) => Accept(this, visitor);

        public static IntervalType FromIntervalUnit(IntervalUnit unit)
        {
            return _types[(int)unit];
        }
    }
}
