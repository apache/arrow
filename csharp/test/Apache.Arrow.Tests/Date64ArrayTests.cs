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
using Xunit;

namespace Apache.Arrow.Tests
{
    public class Date64ArrayTests
    {
        public class GetDate
        {
            [Fact]
            public void SetAndGetNull()
            {
                // Arrange
                var array = new Date64Array.Builder()
                    .AppendNull()
                    .Build();

                // Act
                var actual = array.GetDateTime(0);

                // Assert
                Assert.Null(actual);
            }

            [Theory]
            [InlineData(1, 1, 1)]
            [InlineData(1969, 12, 31)]
            [InlineData(1970, 1, 1)]
            [InlineData(1970, 1, 2)]
            [InlineData(2020, 2, 29)]
            [InlineData(2020, 7, 1)]
            [InlineData(9999, 12, 31)]
            public void SetAndGet(int year, int month, int day)
            {
                // Arrange
                var expected = new DateTime(year, month, day);
                var array = new Date64Array.Builder()
                    .Resize(1)
                    .Set(0, expected)
                    .Build();

                // Act
                var actual = array.GetDateTime(0);

                // Assert
                Assert.NotNull(actual);
                Assert.Equal(expected, actual.Value);
            }
        }

        public class GetValue
        {
            [Theory]
            [InlineData(1, 1, 1)]
            [InlineData(1969, 12, 31)]
            [InlineData(1970, 1, 1)]
            [InlineData(1970, 1, 2)]
            [InlineData(1972, 6, 30)] // First announced leap second
            [InlineData(2015, 6, 30)] // This date contained a leap second
            [InlineData(2016, 12, 31)] // This date contained a leap second
            [InlineData(9999, 12, 31)]
            public void MultipleOf86400000(int year, int month, int day)
            {
                // Arrange
                var date = new DateTime(year, month, day);
                var array = new Date64Array.Builder()
                    .Resize(1)
                    .Set(0, date)
                    .Build();

                // Act
                var value = array.GetValue(0);

                // Assert
                // According to the schema, values in a Date64 type are required
                // to be evenly divisible by 86400000 (i.e. no leap seconds).
                Assert.NotNull(value);
                Assert.Equal(0, value.Value % 86400000);
            }
        }
    }
}
