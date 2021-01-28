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
    public class DecimalArrayTests
    {
        public class Builder
        {
            public class AppendNull
            {
                [Fact]
                public void AppendThenGetGivesNull()
                {
                    // Arrange
                    var builder = new DecimalArray.Builder();

                    // Act
                    builder = builder.AppendNull();

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(1, array.Length);
                    Assert.Null(array.GetValue(0));
                }
            }

            public class Append
            {
                [Theory]
                [InlineData(100)]
                public void AppendDecimal(int count)
                {
                    // Arrange
                    var builder = new DecimalArray.Builder();

                    // Act
                    decimal?[] testData = new decimal?[count];
                    for (int i = 0; i < count; i++)
                    {
                        if (i == count - 2)
                        {
                            builder.AppendNull();
                            testData[i] = null;
                            continue;
                        }
                        decimal rnd = i * (decimal)new Random().NextDouble();
                        testData[i] = rnd;
                        builder.Append(rnd);
                    }

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(count, array.Length);
                    for (int i = 0; i < count; i++)
                    {
                        Assert.Equal(testData[i], array.GetValue(i));
                    }
                }

                [Fact]
                public void AppendLargeDecimal()
                {
                    // Arrange
                    var builder = new DecimalArray.Builder();
                    decimal large = 91654987987978.00000001M;

                    // Act
                    builder.Append(large);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(large, array.GetValue(0));
                }
            }
        }
    }
}
