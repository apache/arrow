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
    public class Time32ArrayTests
    {
        public class IEnumerableArray
        {
            [Fact]
            public void Time32Array_Seconds_ShouldBe_IEnumerable()
            {
                // Build test array
                Array.Accessor<Time32Array, TimeSpan?> array = new Time32Array.Builder(TimeUnit.Second)
                    .Append(1).AppendNull().Append(-1)
                    .Build()
                    .Items();

                TimeSpan?[] expected = new TimeSpan?[] { TimeSpan.FromSeconds(1), null, TimeSpan.FromSeconds(-1) };

                int i = 0;
                foreach (TimeSpan? value in array)
                {
                    Assert.Equal(expected[i], value);
                    i++;
                }
            }

            [Fact]
            public void Time32Array_MilliSeconds_ShouldBe_IEnumerable()
            {
                // Build test array
                Array.Accessor<Time32Array, TimeSpan?> array = new Time32Array.Builder(TimeUnit.Millisecond)
                    .Append(1).AppendNull().Append(-1)
                    .Build()
                    .Items();

                TimeSpan?[] expected = new TimeSpan?[] { TimeSpan.FromMilliseconds(1), null, TimeSpan.FromMilliseconds(-1) };

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
