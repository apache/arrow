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
    public class Date32ArrayTests
    {
        public class GetDateTime
        {
            [Fact]
            public void SetAndGetNull()
            {
                // Arrange
                var array = new Date32Array.Builder()
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
                var array = new Date32Array.Builder()
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

        public class GetDateTimeOffset
        {
            [Fact]
            public void SetAndGetNull()
            {
                // Arrange
                var array = new Date32Array.Builder()
                    .AppendNull()
                    .Build();

                // Act
                var actual = array.GetDateTimeOffset(0);

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
                var expected = new DateTimeOffset(year, month, day, 0, 0, 0, TimeSpan.Zero);
                var array = new Date32Array.Builder()
                    .Resize(1)
                    .Set(0, expected)
                    .Build();

                // Act
                var actual = array.GetDateTimeOffset(0);

                // Assert
                Assert.NotNull(actual);
                Assert.Equal(expected, actual.Value);
            }
        }
    }
}
