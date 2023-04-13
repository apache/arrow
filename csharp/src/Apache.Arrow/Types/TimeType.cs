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


using System.IO;
using System;

namespace Apache.Arrow.Types
{
    public enum TimeUnit
    {
        Second,
        Millisecond,
        Microsecond,
        Nanosecond
    }

    public abstract class TimeType: FixedWidthType
    {
        public TimeUnit Unit { get; }

        protected TimeType(TimeUnit unit)
        {
            Unit = unit;
        }

        public long ToLong(TimeSpan ts)
        {
            switch (Unit)
            {
                case TimeUnit.Second:
                    return Convert.ToInt64(ts.TotalSeconds);
                case TimeUnit.Millisecond:
                    return Convert.ToInt64(ts.TotalMilliseconds);
                case TimeUnit.Microsecond:
                    return ts.Ticks / 10;
                case TimeUnit.Nanosecond:
                    return ts.Ticks * 100;
                default:
                    throw new InvalidDataException($"Unsupported time unit for TimeType: {unit}");
            }
        }

        public int ToInt(TimeSpan ts)
        {
            switch (Unit)
            {
                case TimeUnit.Second:
                    return Convert.ToInt32(ts.TotalSeconds);
                case TimeUnit.Millisecond:
                    return Convert.ToInt32(ts.TotalMilliseconds);
                case TimeUnit.Microsecond:
                    return Convert.ToInt32(ts.Ticks / 10);
                case TimeUnit.Nanosecond:
                    return Convert.ToInt32(ts.Ticks * 100);
                default:
                    throw new InvalidDataException($"Unsupported time unit for TimeType: {unit}");
            }
        }
    }
}
