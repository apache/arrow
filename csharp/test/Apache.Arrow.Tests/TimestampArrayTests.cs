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

namespace Apache.Arrow.Tests
{
    public class TimestampArrayTests
    {
        public class IEnumerableArray
        {
            [Fact]
            public void TimestampArray_Seconds_ShouldBe_IEnumerable()
            {
                // Build test array
                Array.Accessor<TimestampArray, DateTimeOffset?> array = new TimestampArray.Builder(TimeUnit.Second)
                    .Append(DateTimeOffset.FromUnixTimeMilliseconds(1)).AppendNull().Append(DateTimeOffset.FromUnixTimeSeconds(-1))
                    .Build()
                    .Items();

                DateTimeOffset?[] expected = new DateTimeOffset?[] { DateTimeOffset.FromUnixTimeMilliseconds(0), null, DateTimeOffset.FromUnixTimeSeconds(-1) };

                int i = 0;
                foreach (DateTimeOffset? value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }

            [Fact]
            public void TimestampArray_Milliseconds_ShouldBe_IEnumerable()
            {
                // Build test array
                Array.Accessor<TimestampArray, DateTimeOffset?> array = new TimestampArray.Builder(TimeUnit.Millisecond)
                    .Append(DateTimeOffset.FromUnixTimeMilliseconds(1)).AppendNull().Append(DateTimeOffset.FromUnixTimeSeconds(-1))
                    .Build()
                    .Items();

                DateTimeOffset?[] expected = new DateTimeOffset?[] { DateTimeOffset.FromUnixTimeMilliseconds(1), null, DateTimeOffset.FromUnixTimeSeconds(-1) };

                int i = 0;
                foreach (DateTimeOffset? value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }

            [Fact]
            public void TimestampArray_Microseconds_ShouldBe_IEnumerable()
            {
                // Build test array
                Array.Accessor<TimestampArray, DateTimeOffset?> array = new TimestampArray.Builder(TimeUnit.Microsecond)
                    .Append(DateTimeOffset.FromUnixTimeMilliseconds(1)).AppendNull().Append(DateTimeOffset.FromUnixTimeSeconds(-1))
                    .Build()
                    .Items();

                DateTimeOffset?[] expected = new DateTimeOffset?[] { DateTimeOffset.FromUnixTimeMilliseconds(1), null, DateTimeOffset.FromUnixTimeSeconds(-1) };

                int i = 0;
                foreach (DateTimeOffset? value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }

            [Fact]
            public void TimestampArray_Nanoseconds_ShouldBe_IEnumerable()
            {
                // Build test array
                Array.Accessor<TimestampArray, DateTimeOffset?> array = new TimestampArray.Builder(TimeUnit.Nanosecond)
                    .Append(DateTimeOffset.FromUnixTimeMilliseconds(1)).AppendNull().Append(DateTimeOffset.FromUnixTimeSeconds(-1))
                    .Build()
                    .Items();

                DateTimeOffset?[] expected = new DateTimeOffset?[] { DateTimeOffset.FromUnixTimeMilliseconds(1), null, DateTimeOffset.FromUnixTimeSeconds(-1) };

                int i = 0;
                foreach (DateTimeOffset? value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }
        }
    }
}
