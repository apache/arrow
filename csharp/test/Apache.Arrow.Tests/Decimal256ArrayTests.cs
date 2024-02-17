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
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class Decimal256ArrayTests
    {
        static SqlDecimal? GetSqlDecimal(Decimal256Array array, int index)
        {
            SqlDecimal? result;
            Assert.True(array.TryGetSqlDecimal(index, out result));
            return result;
        }

        static SqlDecimal? Convert(decimal? value)
        {
            return value == null ? null : new SqlDecimal(value.Value);
        }

        static decimal? Convert(SqlDecimal? value)
        {
            return value == null ? null : value.Value.Value;
        }

        static decimal? Convert(string value)
        {
            return value == null ? null : decimal.Parse(value);
        }

        public class Builder
        {
            public class AppendNull
            {
                [Fact]
                public void AppendThenGetGivesNull()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(8,2));

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

                    Assert.Null(GetSqlDecimal(array, 0));
                    Assert.Null(GetSqlDecimal(array, 1));
                    Assert.Null(GetSqlDecimal(array, 2));
                }
            }

            public class Append
            {
                [Theory]
                [InlineData(200)]
                public void AppendDecimal(int count)
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(14, 10));

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
                        decimal rnd = i * (decimal)Math.Round(new Random().NextDouble(),10);
                        testData[i] = rnd;
                        builder.Append(rnd);
                    }

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(count, array.Length);
                    for (int i = 0; i < count; i++)
                    {
                        Assert.Equal(testData[i], array.GetValue(i));
                        Assert.Equal(Convert(testData[i]), GetSqlDecimal(array, i));
                    }
                }

                [Fact]
                public void AppendLargeDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(26, 2));
                    decimal large = 999999999999909999999999.80M;
                    // Act
                    builder.Append(large);
                    builder.Append(-large);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(large, array.GetValue(0));
                    Assert.Equal(-large, array.GetValue(1));

                    Assert.Equal(Convert(large), GetSqlDecimal(array, 0));
                    Assert.Equal(Convert(-large), GetSqlDecimal(array, 1));
                }

                [Fact]
                public void AppendMaxAndMinDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(29, 0));

                    // Act
                    builder.Append(Decimal.MaxValue);
                    builder.Append(Decimal.MinValue);
                    builder.Append(Decimal.MaxValue - 10);
                    builder.Append(Decimal.MinValue + 10);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(Decimal.MaxValue, array.GetValue(0));
                    Assert.Equal(Decimal.MinValue, array.GetValue(1));
                    Assert.Equal(Decimal.MaxValue - 10, array.GetValue(2));
                    Assert.Equal(Decimal.MinValue + 10, array.GetValue(3));

                    Assert.Equal(Convert(Decimal.MaxValue), GetSqlDecimal(array, 0));
                    Assert.Equal(Convert(Decimal.MinValue), GetSqlDecimal(array, 1));
                    Assert.Equal(Convert(Decimal.MaxValue) - 10, GetSqlDecimal(array, 2));
                    Assert.Equal(Convert(Decimal.MinValue) + 10, GetSqlDecimal(array, 3));
                }

                [Fact]
                public void AppendFractionalDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(26, 20));
                    decimal fraction = 0.99999999999990999992M;
                    // Act
                    builder.Append(fraction);
                    builder.Append(-fraction);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(fraction, array.GetValue(0));
                    Assert.Equal(-fraction, array.GetValue(1));

                    Assert.Equal(Convert(fraction), GetSqlDecimal(array, 0));
                    Assert.Equal(Convert(-fraction), GetSqlDecimal(array, 1));
                }

                [Fact]
                public void AppendRangeDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(24, 8));
                    var range = new decimal[] {2.123M, 1.5984M, -0.0000001M, 9878987987987987.1235407M};

                    // Act
                    builder.AppendRange(range);
                    builder.AppendNull();

                    // Assert
                    var array = builder.Build();
                    for(int i = 0; i < range.Length; i ++)
                    {
                        Assert.Equal(range[i], array.GetValue(i));
                        Assert.Equal(Convert(range[i]), GetSqlDecimal(array, i));
                    }

                    Assert.Null( array.GetValue(range.Length));
                }

                [Fact]
                public void AppendClearAppendDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(24, 8));
                    
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
                    var builder = new Decimal256Array.Builder(new Decimal256Type(2, 1));

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
                    var builder = new Decimal256Array.Builder(new Decimal256Type(24, 8))
                        .Resize(1);
                    
                    // Act
                    builder.Set(0, 50.123456M);
                    builder.Set(0, 1.01M);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(1.01M, array.GetValue(0));
                }

                [Fact]
                public void SetNull()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(24, 8))
                        .Resize(1);

                    // Act
                    builder.Set(0, 50.123456M);
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
                    var builder = new Decimal256Array.Builder(new Decimal256Type(24, 8));

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
                    var builder = new Decimal256Array.Builder(new Decimal256Type(24, 8));

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

            public class SqlDecimals
            {
                [Theory]
                [InlineData(200)]
                public void AppendSqlDecimal(int count)
                {
                    // Arrange
                    const int precision = 10;
                    var builder = new Decimal256Array.Builder(new Decimal256Type(14, precision));

                    // Act
                    SqlDecimal?[] testData = new SqlDecimal?[count];
                    for (int i = 0; i < count; i++)
                    {
                        if (i == count - 2)
                        {
                            builder.AppendNull();
                            testData[i] = null;
                            continue;
                        }
                        SqlDecimal rnd = i * (SqlDecimal)Math.Round(new Random().NextDouble(), 10);
                        builder.Append(rnd);
                        testData[i] = SqlDecimal.Round(rnd, precision);
                    }

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(count, array.Length);
                    for (int i = 0; i < count; i++)
                    {
                        Assert.Equal(testData[i], GetSqlDecimal(array, i));
                        Assert.Equal(Convert(testData[i]), array.GetValue(i));
                    }

                    IReadOnlyList<SqlDecimal?> asDecimalList = array;
                    for (int i = 0; i < asDecimalList.Count; i++)
                    {
                        Assert.Equal(testData[i], asDecimalList[i]);
                    }

                    IReadOnlyList<string> asStringList = array;
                    for (int i = 0; i < asStringList.Count; i++)
                    {
                        Assert.Equal(Convert(testData[i]?.ToString()), Convert(asStringList[i]));
                    }
                }

                [Fact]
                public void AppendMaxAndMinSqlDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(38, 0));

                    // Act
                    builder.Append(SqlDecimal.MaxValue);
                    builder.Append(SqlDecimal.MinValue);
                    builder.Append(SqlDecimal.MaxValue - 10);
                    builder.Append(SqlDecimal.MinValue + 10);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(SqlDecimal.MaxValue, GetSqlDecimal(array, 0));
                    Assert.Equal(SqlDecimal.MinValue, GetSqlDecimal(array, 1));
                    Assert.Equal(SqlDecimal.MaxValue - 10, GetSqlDecimal(array, 2));
                    Assert.Equal(SqlDecimal.MinValue + 10, GetSqlDecimal(array, 3));
                }

                [Fact]
                public void AppendRangeSqlDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(24, 8));
                    var range = new SqlDecimal[] { 2.123M, 1.5984M, -0.0000001M, 9878987987987987.1235407M };

                    // Act
                    builder.AppendRange(range);
                    builder.AppendNull();

                    // Assert
                    var array = builder.Build();
                    for (int i = 0; i < range.Length; i++)
                    {
                        Assert.Equal(range[i], GetSqlDecimal(array, i));
                        Assert.Equal(Convert(range[i]), array.GetValue(i));
                    }

                    Assert.Null(array.GetValue(range.Length));
                }
            }

            public class Strings
            {
                [Theory]
                [InlineData(200)]
                public void AppendString(int count)
                {
                    // Arrange
                    const int precision = 10;
                    var builder = new Decimal256Array.Builder(new Decimal256Type(14, precision));

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
                        SqlDecimal rnd = i * (SqlDecimal)Math.Round(new Random().NextDouble(), 10);
                        builder.Append(rnd);
                        testData[i] = SqlDecimal.Round(rnd, precision).ToString();
                    }

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(count, array.Length);
                    for (int i = 0; i < count; i++)
                    {
                        if (testData[i] == null)
                        {
                            Assert.Null(array.GetString(i));
                            Assert.Null(GetSqlDecimal(array, i));
                        }
                        else
                        {
                            Assert.Equal(testData[i].TrimEnd('0'), array.GetString(i).TrimEnd('0'));
                            Assert.Equal(SqlDecimal.Parse(testData[i]), GetSqlDecimal(array, i));
                        }
                    }
                }

                [Fact]
                public void AppendMaxAndMinSqlDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(38, 0));

                    // Act
                    builder.Append(SqlDecimal.MaxValue.ToString());
                    builder.Append(SqlDecimal.MinValue.ToString());
                    string maxMinusTen = (SqlDecimal.MaxValue - 10).ToString();
                    string minPlusTen = (SqlDecimal.MinValue + 10).ToString();
                    builder.Append(maxMinusTen);
                    builder.Append(minPlusTen);

                    // Assert
                    var array = builder.Build();
                    Assert.Equal(SqlDecimal.MaxValue.ToString(), array.GetString(0));
                    Assert.Equal(SqlDecimal.MinValue.ToString(), array.GetString(1));
                    Assert.Equal(maxMinusTen, array.GetString(2));
                    Assert.Equal(minPlusTen, array.GetString(3));
                }

                [Fact]
                public void AppendRangeSqlDecimal()
                {
                    // Arrange
                    var builder = new Decimal256Array.Builder(new Decimal256Type(24, 8));
                    var range = new SqlDecimal[] { 2.123M, 1.5984M, -0.0000001M, 9878987987987987.1235407M };

                    // Act
                    builder.AppendRange(range.Select(d => d.ToString()));
                    builder.AppendNull();

                    // Assert
                    var array = builder.Build();
                    for (int i = 0; i < range.Length; i++)
                    {
                        Assert.Equal(range[i], GetSqlDecimal(array, i));
                        Assert.Equal(range[i].ToString().TrimEnd('0'), array.GetString(i).TrimEnd('0'));
                    }

                    Assert.Null(array.GetValue(range.Length));
                }
            }
        }
    }
}
