using System;
using System.Collections.Generic;
using System.Text;
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
            public void HasExpectedResultOrThrows(decimal d, int precision , int scale, bool shouldThrow)
            {
                var bytes = new byte[16];
                if (shouldThrow)
                {
                    Assert.Throws<OverflowException>(() =>
                        DecimalUtility.GetBytes(d, precision, scale, 16, bytes));
                }
                else
                {
                    DecimalUtility.GetBytes(d, precision, scale, 16, bytes);
                    Assert.NotEqual(new byte[16], bytes);
                }
            }
        }
    }
}
