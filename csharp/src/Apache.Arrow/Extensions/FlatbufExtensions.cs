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
    internal static class FlatbufExtensions
    {
        public static bool IsFixedPrimitive(this Flatbuf.Type t)
        {
            if (t == Flatbuf.Type.Utf8 || t == Flatbuf.Type.Binary)
                return false;
            return true;
        }

        public static bool IsFixedPrimitive(this Types.IArrowType t)
        {
            return t.TypeId.IsFixedPrimitive();
        }

        public static bool IsFixedPrimitive(this Types.ArrowTypeId t)
        {
            if (t == Types.ArrowTypeId.String || t == Types.ArrowTypeId.Binary)
                return false;
            return true;
        }

        public static Types.IntervalUnit ToArrow(this Flatbuf.IntervalUnit unit)
        {
            switch (unit)
            {
                case Flatbuf.IntervalUnit.DAY_TIME:
                    return Types.IntervalUnit.DayTime;
                case Flatbuf.IntervalUnit.YEAR_MONTH:
                    return Types.IntervalUnit.YearMonth;
                default:
                    throw new ArgumentException($"Unexpected Flatbuf IntervalUnit", nameof(unit));
            }
        }

        public static Types.DateUnit ToArrow(this Flatbuf.DateUnit unit)
        {
            switch (unit)
            {
                case Flatbuf.DateUnit.DAY:
                    return Types.DateUnit.Day;
                case Flatbuf.DateUnit.MILLISECOND:
                    return Types.DateUnit.Milliseconds;
                default:
                    throw new ArgumentException($"Unexpected Flatbuf IntervalUnit", nameof(unit));
            }
        }

        public static Types.TimeUnit ToArrow(this Flatbuf.TimeUnit unit)
        {
            switch (unit)
            {
                case Flatbuf.TimeUnit.MICROSECOND:
                    return Types.TimeUnit.Microsecond;
                case Flatbuf.TimeUnit.MILLISECOND:
                    return Types.TimeUnit.Millisecond;
                case Flatbuf.TimeUnit.NANOSECOND:
                    return Types.TimeUnit.Nanosecond;
                case Flatbuf.TimeUnit.SECOND:
                    return Types.TimeUnit.Second;
                default:
                    throw new ArgumentException($"Unexpected Flatbuf TimeUnit", nameof(unit));
            }
        }
    }
}

