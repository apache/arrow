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
                        
            [Theory]
            [InlineData(new byte[] { 0b11111111 }, 0, 8, 8)]
            [InlineData(new byte[] { 0b11111111 }, 0, 4, 4)]
            [InlineData(new byte[] { 0b11111111 }, 3, 2, 2)]
            [InlineData(new byte[] { 0b11111111 }, 3, 5, 5)]
            [InlineData(new byte[] { 0b11111111, 0b11111111 }, 9, 7, 7)]
            [InlineData(new byte[] { 0b11111111, 0b11111111 }, 7, 2, 2)]
            [InlineData(new byte[] { 0b11111111, 0b11111111, 0b11111111 }, 0, 24, 24)]
            [InlineData(new byte[] { 0b11111111, 0b11111111, 0b11111111 }, 8, 16, 16)]
            [InlineData(new byte[] { 0b11111111, 0b11111111, 0b11111111 }, 0, 16, 16)]
            [InlineData(new byte[] { 0b11111111, 0b11111111, 0b11111111 }, 3, 18, 18)]
            [InlineData(new byte[] { 0b11111111 }, -1, 0, 0)]
            public void CountsAllOneBitsFromOffsetWithinLength(byte[] data, int offset, int length, int expectedCount)
            {
                var actualCount = BitUtility.CountBits(data, offset, length);
                Assert.Equal(expectedCount, actualCount);
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

            [Theory]
            [InlineData(new byte[] { 0b10000000 }, 0, true, new byte[] { 0b10000001 })]
            [InlineData(new byte[] { 0b00000000 }, 2, true, new byte[] { 0b00000100 })]
            [InlineData(new byte[] { 0b11111111 }, 2, false, new byte[] { 0b11111011 })]
            [InlineData(new byte[] { 0b01111111 }, 7, true, new byte[] { 0b11111111 })]
            [InlineData(new byte[] { 0b00000000, 0b00000000 }, 8, true, new byte[] { 0b00000000, 0b00000001 })]
            [InlineData(new byte[] { 0b11111110, 0b11111110 }, 15, false, new byte[] { 0b11111110, 0b01111110 })]
            public void SetsBitValueAtIndex(byte[] data, int index, bool value, byte[] expectedValue)
            {
                BitUtility.SetBit(data, index, value);
                Assert.Equal(expectedValue, data);
            }
        }

        public class SetBits
        {
            [Theory]
            [InlineData(new byte[] { 0b00000000 }, 0, 0, true, new byte[] { 0b00000000 })]
            [InlineData(new byte[] { 0b00000000 }, 0, 1, true, new byte[] { 0b00000001 })]
            [InlineData(new byte[] { 0b00000000 }, 0, 8, true, new byte[] { 0b11111111 })]
            [InlineData(new byte[] { 0b00000000 }, 2, 2, true, new byte[] { 0b00001100 })]
            [InlineData(new byte[] { 0b00000000 }, 5, 3, true, new byte[] { 0b11100000 })]
            [InlineData(new byte[] { 0b00000000, 0b00000000 }, 8, 1, true, new byte[] { 0b00000000, 0b00000001 })]
            [InlineData(new byte[] { 0b00000000, 0b00000000 }, 15, 1, true, new byte[] { 0b00000000, 0b10000000 })]
            [InlineData(new byte[] { 0b00000000, 0b00000000 }, 7, 2, true, new byte[] { 0b10000000, 0b00000001 })]
            [InlineData(new byte[] { 0b11111111, 0b11111111 }, 7, 2, false, new byte[] { 0b01111111, 0b11111110 })]
            public void SetsBitsInRangeOnSmallRanges(byte[] data, int index, int length, bool value, byte[] expectedValue)
            {
                BitUtility.SetBits(data, index, length, value);
                Assert.Equal(expectedValue, data);
            }
                        
            [Fact]
            public void SetsBitsInRangeOnBigRanges()
            {
                //Arrange
                //Allocate 64 bits
                Span<byte> data = stackalloc byte[8];
                data.Clear();

                //Act
                //Sets 56 bits in the middle
                BitUtility.SetBits(data, 4, 56, true);

                //Assert
                //Check that 4 bits in the lower and upper boundaries are left untouched
                Assert.Equal(0b11110000, data[0]);
                Assert.Equal(0b00001111, data[7]);

                //Check bits in the middle
                for (int i = 1; i < 7; i++)
                    Assert.Equal(0b11111111, data[i]);
            }

            [Fact]
            public void SetsBitsInRangeOnBigRanges_LowerBoundaryCornerCase()
            {
                //Arrange
                //Allocate 64 bits
                Span<byte> data = stackalloc byte[8];
                data.Clear();

                //Act
                //Sets all bits starting from 1
                BitUtility.SetBits(data, 1, 63, true);

                //Assert
                //Check lower boundary
                Assert.Equal(0b11111110, data[0]);

                //Check other bits
                for (int i = 1; i < 8; i++)
                    Assert.Equal(0b11111111, data[i]);
            }

            [Fact]
            public void SetsBitsInRangeOnBigRanges_UpperBoundaryCornerCase()
            {
                //Arrange
                //Allocate 64 bits
                Span<byte> data = stackalloc byte[8];
                data.Clear();

                //Act
                //Sets all bits starting from 0
                BitUtility.SetBits(data, 0, 63, true);

                //Assert
                //Check upper boundary
                Assert.Equal(0b01111111, data[7]);

                //Check other bits
                for (int i = 0; i < 7; i++)
                    Assert.Equal(0b11111111, data[i]);
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
