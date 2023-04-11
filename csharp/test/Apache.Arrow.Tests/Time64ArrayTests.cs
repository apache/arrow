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
using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class Time64ArrayTests
    {
        public class IEnumerableArray
        {
            [Fact]
            public void Time64Array_MicroSeconds_ShouldBe_IEnumerable()
            {
                // Build test array
                Time64Array array = new Time64Array.Builder(TimeUnit.Microsecond)
                    .Append(1).AppendNull().Append(-1)
                    .Build();

                TimeSpan?[] expected = new TimeSpan?[] { TimeSpan.FromMicroseconds(1), null, TimeSpan.FromMicroseconds(-1) };

                int i = 0;
                foreach (TimeSpan? value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }

            [Fact]
            public void Time64Array_NanoSeconds_ShouldBe_IEnumerable()
            {
                // Build test array
                Time64Array array = new Time64Array.Builder(TimeUnit.Nanosecond)
                    .Append(123).AppendNull().Append(-987)
                    .Build();

                TimeSpan?[] expected = new TimeSpan?[] { TimeSpan.FromTicks(1), null, TimeSpan.FromTicks(-9) };

                int i = 0;
                foreach (TimeSpan? value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }
        }
    }
}
