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

    internal static class TimeUnitExtensions
    {
        private const long TicksPerMicrosecond = 10;
        private const long NanosecondsPerTick = 100;

        public static long ConvertFromTicks(this TimeUnit unit, long ticks)
        {
            return unit switch
            {
                TimeUnit.Second => ticks / TimeSpan.TicksPerSecond,
                TimeUnit.Millisecond => ticks / TimeSpan.TicksPerMillisecond,
                TimeUnit.Microsecond => ticks / TicksPerMicrosecond,
                TimeUnit.Nanosecond => ticks * NanosecondsPerTick,
                _ => throw new InvalidDataException($"Unsupported time unit: {unit}")
            };
        }

        public static long ConvertToTicks(this TimeUnit unit, long units)
        {
            return unit switch
            {
                TimeUnit.Second => units * TimeSpan.TicksPerSecond,
                TimeUnit.Millisecond => units * TimeSpan.TicksPerMillisecond,
                TimeUnit.Microsecond => units * TicksPerMicrosecond,
                TimeUnit.Nanosecond => units / NanosecondsPerTick,
                _ => throw new InvalidDataException($"Unsupported time unit: {unit}")
            };
        }
    }
}
