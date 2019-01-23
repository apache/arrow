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
    public class BitUtilityTests
    {
        public class ByteCount
        {
            [Theory]
            [InlineData(0, 0)]
            [InlineData(1, 1)]
            [InlineData(8, 1)]
            [InlineData(9, 2)]
            [InlineData(32, 4)]
            public void HasExpectedResult(int n, int expected)
            {
                var count = BitUtility.ByteCount(n);
                Assert.Equal(expected, count);
            }
        }

        public class CountBits
        {
            [Theory]
            [InlineData(new byte[] { 0b00000000 }, 0)]
            [InlineData(new byte[] { 0b00000001 }, 1)]
            [InlineData(new byte[] { 0b11111111 }, 8)]
            [InlineData(new byte[] { 0b01001001, 0b01010010 }, 6)]
            public void CountsAllOneBits(byte[] data, int expectedCount)
            {
                Assert.Equal(expectedCount,
                    BitUtility.CountBits(data));
            }

            [Theory]
            [InlineData(new byte[] { 0b11111111 }, 0, 8)]
            [InlineData(new byte[] { 0b11111111 }, 3, 5)]
            [InlineData(new byte[] { 0b11111111, 0b11111111 }, 9, 7)]
            [InlineData(new byte[] { 0b11111111 }, -1, 0)]
            public void CountsAllOneBitsFromAnOffset(byte[] data, int offset, int expectedCount)
            {
                Assert.Equal(expectedCount,
                    BitUtility.CountBits(data, offset));
            }

            [Fact]
            public void CountsZeroBitsWhenDataIsEmpty()
            {
                Assert.Equal(0,
                    BitUtility.CountBits(null));
            }
        }

        public class GetBit
        {
            [Theory]
            [InlineData(new byte[] { 0b01001001 }, 0, true)]
            [InlineData(new byte[] { 0b01001001 }, 1, false)]
            [InlineData(new byte[] { 0b01001001 }, 2, false)]
            [InlineData(new byte[] { 0b01001001 }, 3, true)]
            [InlineData(new byte[] { 0b01001001 }, 4, false)]
            [InlineData(new byte[] { 0b01001001 }, 5, false)]
            [InlineData(new byte[] { 0b01001001 }, 6, true)]
            [InlineData(new byte[] { 0b01001001 }, 7, false)]
            [InlineData(new byte[] { 0b01001001, 0b01010010 }, 8, false)]
            [InlineData(new byte[] { 0b01001001, 0b01010010 }, 14, true)]
            public void GetsCorrectBitForIndex(byte[] data, int index, bool expectedValue)
            {
                Assert.Equal(expectedValue,
                    BitUtility.GetBit(data, index));
            }

            [Theory]
            [InlineData(null, 0)]
            [InlineData(new byte[] { 0b00000000 }, -1)]
            public void ThrowsWhenBitIndexOutOfRange(byte[] data, int index)
            {
                Assert.Throws<IndexOutOfRangeException>(() =>
                    BitUtility.GetBit(data, index));
            }
        }

        public class SetBit
        {
            [Theory]
            [InlineData(new byte[] { 0b00000000 }, 0, new byte[] { 0b00000001 })]
            [InlineData(new byte[] { 0b00000000 }, 2, new byte[] { 0b00000100 })]
            [InlineData(new byte[] { 0b00000000 }, 7, new byte[] { 0b10000000 })]
            [InlineData(new byte[] { 0b00000000, 0b00000000 }, 8, new byte[] { 0b00000000, 0b00000001 })]
            [InlineData(new byte[] { 0b00000000, 0b00000000 }, 15, new byte[] { 0b00000000, 0b10000000 })]
            public void SetsBitAtIndex(byte[] data, int index, byte[] expectedValue)
            {
                BitUtility.SetBit(data, index);
                Assert.Equal(expectedValue, data);
            }
        }

        public class ClearBit
        {
            [Theory]
            [InlineData(new byte[] { 0b00000001 }, 0, new byte[] { 0b00000000 })]
            [InlineData(new byte[] { 0b00000010 }, 1, new byte[] { 0b00000000 })]
            [InlineData(new byte[] { 0b10000001 }, 7, new byte[] { 0b00000001 })]
            [InlineData(new byte[] { 0b11111111, 0b11111111 }, 15, new byte[] { 0b11111111, 0b01111111 })]
            public void ClearsBitAtIndex(byte[] data, int index, byte[] expectedValue)
            {
                BitUtility.ClearBit(data, index);
                Assert.Equal(expectedValue, data);
            }
        }

        public class RoundUpToMultipleOf64
        {
            [Theory]
            [InlineData(0, 0)]
            [InlineData(1, 64)]
            [InlineData(63, 64)]
            [InlineData(64, 64)]
            [InlineData(65, 128)]
            [InlineData(129, 192)]
            public void ReturnsNextMultiple(int size, int expectedSize)
            {
                Assert.Equal(expectedSize,
                    BitUtility.RoundUpToMultipleOf64(size));
            }

            [Theory]
            [InlineData(0)]
            [InlineData(-1)]
            public void ReturnsZeroWhenSizeIsLessThanOrEqualToZero(int size)
            {
                Assert.Equal(0,
                    BitUtility.RoundUpToMultipleOf64(size));
            }
        }
    }
}
