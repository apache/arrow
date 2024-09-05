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
    public class Decimal32ArrayTests
    {
        public class Builder
        {
            public class AppendNull
            {
                [Fact]
                public void AppendThenGetGivesNull()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3));

                    // Act

                    builder = builder.AppendNull();
                    builder = builder.AppendNull();
                    builder = builder.AppendNull();
                    // Assert
                    var array = builder.Build();

                    Assert.Equal(3, array.Length);
                    Assert.Equal(array.Data.Buffers[1].Length, array.ByteWidth * 3);
                    Assert.Null(array.GetValue(0));
                    Assert.Null(array.GetValue(1));
                    Assert.Null(array.GetValue(2));
                }
            }

            public class Append
            {
                [Theory]
                [InlineData(200)]
                public void AppendDecimal(int count)
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3));

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
                        decimal rnd = i * (decimal)Math.Round(new Random().NextDouble(), 2);
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
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3));
                    decimal large = 9999.999M;
                    // Act
                    builder.Append(large);
                    builder.Append(-large);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(large, array.GetValue(0));
                    Assert.Equal(-large, array.GetValue(1));
                }

                [Fact]
                public void AppendFractionalDecimal()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(9, 9));
                    decimal fraction = 0.999999999M;
                    // Act
                    builder.Append(fraction);
                    builder.Append(-fraction);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(fraction, array.GetValue(0));
                    Assert.Equal(-fraction, array.GetValue(1));
                }

                [Fact]
                public void AppendRangeDecimal()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3));
                    var range = new decimal[] { 2.123M, 1.598M, -0.001M, 7987.123M };

                    // Act
                    builder.AppendRange(range);
                    builder.AppendNull();

                    // Assert
                    var array = builder.Build();
                    for (int i = 0; i < range.Length; i++)
                    {
                        Assert.Equal(range[i], array.GetValue(i));
                    }

                    Assert.Null(array.GetValue(range.Length));
                }

                [Fact]
                public void AppendClearAppendDecimal()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3));

                    // Act
                    builder.Append(1);
                    builder.Clear();
                    builder.Append(10);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(10, array.GetValue(0));
                }

                [Fact]
                public void AppendInvalidPrecisionAndScaleDecimal()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(2, 1));

                    // Assert
                    Assert.Throws<OverflowException>(() => builder.Append(100));
                    Assert.Throws<OverflowException>(() => builder.Append(0.01M));
                    builder.Append(-9.9M);
                    builder.Append(0);
                    builder.Append(9.9M);
                }
            }

            public class Set
            {
                [Fact]
                public void SetDecimal()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3))
                        .Resize(1);

                    // Act
                    builder.Set(0, 50.123M);
                    builder.Set(0, 1.01M);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(1.01M, array.GetValue(0));
                }

                [Fact]
                public void SetNull()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3))
                        .Resize(1);

                    // Act
                    builder.Set(0, 50.123M);
                    builder.SetNull(0);

                    // Assert
                    var array = builder.Build();
                    Assert.Null(array.GetValue(0));
                }
            }

            public class Swap
            {
                [Fact]
                public void SetDecimal()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3));

                    // Act
                    builder.Append(123.45M);
                    builder.Append(678.9M);
                    builder.Swap(0, 1);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(678.9M, array.GetValue(0));
                    Assert.Equal(123.45M, array.GetValue(1));
                }

                [Fact]
                public void SwapNull()
                {
                    // Arrange
                    var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3));

                    // Act
                    builder.Append(123.456M);
                    builder.AppendNull();
                    builder.Swap(0, 1);

                    // Assert
                    var array = builder.Build();
                    Assert.Null(array.GetValue(0));
                    Assert.Equal(123.456M, array.GetValue(1));
                }
            }

            public class Strings
            {
                [Theory]
                [InlineData(200)]
                public void AppendString(int count)
                {
                    // Arrange
                    const int precision = 4;
                    var builder = new Decimal32Array.Builder(new Decimal32Type(9, precision));

                    // Act
                    string[] testData = new string[count];
                    for (int i = 0; i < count; i++)
                    {
                        if (i == count - 2)
                        {
                            builder.AppendNull();
                            testData[i] = null;
                            continue;
                        }
                        decimal rnd = i * (decimal)Math.Round(new Random().NextDouble(), precision - 2);
                        builder.Append(rnd);
                        testData[i] = decimal.Round(rnd, precision - 1).ToString();
                    }

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(count, array.Length);
                    for (int i = 0; i < count; i++)
                    {
                        if (testData[i] == null)
                        {
                            Assert.Null(array.GetString(i));
                            Assert.Null(array.GetDecimal(i));
                        }
                        else
                        {
                            Assert.Equal(NormalizeNumber(testData[i]), NormalizeNumber(array.GetString(i)));
                            Assert.Equal(Decimal.Parse(testData[i]), array.GetDecimal(i));
                        }
                    }
                }

                static string NormalizeNumber(string number)
                {
                    if (number.IndexOf('.') > 0)
                    {
                        number = number.TrimEnd('0');
                        number = number.TrimEnd('.');
                    }
                    return number;
                }
            }
        }

        [Fact]
        public void SliceDecimal32Array()
        {
            // Arrange
            const int originalLength = 50;
            const int offset = 3;
            const int sliceLength = 32;

            var builder = new Decimal32Array.Builder(new Decimal32Type(7, 3));
            var random = new Random();

            for (int i = 0; i < originalLength; i++)
            {
                if (random.NextDouble() < 0.2)
                {
                    builder.AppendNull();
                }
                else
                {
                    builder.Append(i * (decimal)Math.Round(random.NextDouble(), 2));
                }
            }

            var array = builder.Build();

            // Act
            var slice = (Decimal32Array)array.Slice(offset, sliceLength);

            // Assert
            Assert.NotNull(slice);
            Assert.Equal(sliceLength, slice.Length);
            for (int i = 0; i < sliceLength; ++i)
            {
                Assert.Equal(array.GetValue(offset + i), slice.GetValue(i));
                Assert.Equal(array.GetString(offset + i), slice.GetString(i));
            }

            Assert.Equal(
                array.ToList(includeNulls: true).Skip(offset).Take(sliceLength).ToList(),
                slice.ToList(includeNulls: true));
        }
    }
}
