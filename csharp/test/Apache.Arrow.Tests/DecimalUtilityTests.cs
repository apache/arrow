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
#if !NETSTANDARD1_3
using System.Data.SqlTypes;
#endif
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class DecimalUtilityTests
    {
        public class Overflow
        {
            [Theory]
            [InlineData(100.123, 10, 4, false)]
            [InlineData(100.123, 6, 4, false)]
            [InlineData(100.123, 3, 3, true)]
            [InlineData(100.123, 10, 2, true)]
            [InlineData(100.123, 5, 2, true)]
            [InlineData(100.123, 5, 3, true)]
            [InlineData(100.123, 6, 3, false)]
            public void HasExpectedResultOrThrows(decimal d, int precision, int scale, bool shouldThrow)
            {
                var builder = new Decimal128Array.Builder(new Decimal128Type(precision, scale));

                if (shouldThrow)
                {
                    Assert.Throws<OverflowException>(() => builder.Append(d));
                }
                else
                {
                    builder.Append(d);
                    var result = builder.Build(new TestMemoryAllocator());
                    Assert.Equal(d, result.GetValue(0));
                }
            }

            [Theory]
            [InlineData(4.56, 38, 9, false)]
            [InlineData(7.89, 76, 38, true)]
            public void Decimal256HasExpectedResultOrThrows(decimal d, int precision, int scale, bool shouldThrow)
            {
                var builder = new Decimal256Array.Builder(new Decimal256Type(precision, scale));
                builder.Append(d);
                Decimal256Array result = builder.Build(new TestMemoryAllocator()); ;

                if (shouldThrow)
                {
                    Assert.Throws<OverflowException>(() => result.GetValue(0));
                }
                else
                {
                    Assert.Equal(d, result.GetValue(0));
                }
            }
        }

        public class SqlDecimals
        {

#if !NETSTANDARD1_3
            [Fact]
            public void NegativeSqlDecimal()
            {
                const int precision = 38;
                const int scale = 0;
                const int bitWidth = 16;

                var negative = new SqlDecimal(precision, scale, false, 0, 0, 1, 0);
                var bytes = new byte[16];
                DecimalUtility.GetBytes(negative.Value, precision, scale, bitWidth, bytes);
                var sqlNegative = DecimalUtility.GetSqlDecimal128(new ArrowBuffer(bytes), 0, precision, scale);
                Assert.Equal(negative, sqlNegative);

                DecimalUtility.GetBytes(sqlNegative, precision, scale, bytes);
                var decimalNegative = DecimalUtility.GetDecimal(new ArrowBuffer(bytes), 0, scale, bitWidth);
                Assert.Equal(negative.Value, decimalNegative);
            }

            [Fact]
            public void LargeScale()
            {
                string digits = "1.2345678901234567890123456789012345678";

                var positive = SqlDecimal.Parse(digits);
                Assert.Equal(38, positive.Precision);
                Assert.Equal(37, positive.Scale);

                var bytes = new byte[16];
                DecimalUtility.GetBytes(positive, positive.Precision, positive.Scale, bytes);
                var sqlPositive = DecimalUtility.GetSqlDecimal128(new ArrowBuffer(bytes), 0, positive.Precision, positive.Scale);

                Assert.Equal(positive, sqlPositive);
                Assert.Equal(digits, sqlPositive.ToString());

                digits = "-" + digits;
                var negative = SqlDecimal.Parse(digits);
                Assert.Equal(38, positive.Precision);
                Assert.Equal(37, positive.Scale);

                DecimalUtility.GetBytes(negative, negative.Precision, negative.Scale, bytes);
                var sqlNegative = DecimalUtility.GetSqlDecimal128(new ArrowBuffer(bytes), 0, negative.Precision, negative.Scale);

                Assert.Equal(negative, sqlNegative);
                Assert.Equal(digits, sqlNegative.ToString());
            }
#endif
        }
    }
}
