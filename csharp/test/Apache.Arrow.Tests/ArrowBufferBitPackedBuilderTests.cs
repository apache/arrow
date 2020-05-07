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

namespace Apache.Arrow.Tests
{
    using System;
    using System.Linq;
    using Xunit;

    /// <summary>
    /// The <see cref="ArrowBufferBitPackedBuilderTests"/> class provides unit tests for the
    /// <see cref="ArrowBuffer.BitPackedBuilder"/> class.
    /// </summary>
    public class ArrowBufferBitPackedBuilderTests
    {
        public class Build
        {
            [Theory]
            [InlineData(new bool[] { }, new byte[] { })]
            [InlineData(new[] { true, false, true, false }, new byte[] { 0b00000101 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false, true, false },
                new byte[] { 0b01010101, 0b00000101 })]
            public void AppendedRangeBitPacks(bool[] contents, byte[] expectedBytes)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                builder.AppendRange(contents);

                // Act
                var buf = builder.Build();

                // Assert
                AssertBuffer(expectedBytes, buf);
            }
        }

        public class Clear
        {
            [Theory]
            [InlineData(10)]
            [InlineData(100)]
            public void ClearingSetsBitCountToZero(int numBitsBeforeClear)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                var data = Enumerable.Repeat(true, numBitsBeforeClear).Select(x => x).ToArray();
                builder.AppendRange(data);

                // Act
                builder.Clear();

                // Assert
                Assert.Equal(0, builder.BitCount);
            }

            [Theory]
            [InlineData(0, 4, new byte[] { 0b00001111 })]
            [InlineData(0, 12, new byte[] { 0b11111111, 0b00001111 })]
            [InlineData(4, 0, new byte[] {})]
            [InlineData(12, 0, new byte[] {})]
            [InlineData(12, 24, new byte[] { 0b11111111, 0b11111111, 0b11111111 })]
            public void ClearThenAppendWorksAsExpected(
                int numBitsBeforeClear, int numBitsAfterClear, byte[] expectedBytes)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                var preData = Enumerable.Repeat(true, numBitsBeforeClear).Select(x => x).ToArray();
                var postData = Enumerable.Repeat(true, numBitsAfterClear).Select(x => x).ToArray();
                builder.AppendRange(preData);

                // Act
                builder.Clear();
                builder.AppendRange(postData);
                var buf = builder.Build();

                // Assert
                Assert.Equal(numBitsAfterClear, builder.BitCount);
                AssertBuffer(expectedBytes, buf);
            }
        }

        public class CountSetBits
        {
            [Theory]
            [InlineData(new bool[] {}, 0)]
            [InlineData(new [] { false, false, false }, 0)]
            [InlineData(new [] { true, false }, 1)]
            [InlineData(new [] { false, true }, 1)]
            [InlineData(new [] { true, true, true, true, true, false, true, false, true, false, true, false }, 8)]
            public void CountAsExpected(bool[] bits, int expectedCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                if (bits.Any())
                {
                    builder.AppendRange(bits);
                }

                // Act
                var actualCount = builder.CountSetBits();

                // Assert
                Assert.Equal(expectedCount, actualCount);
            }
        }

        public class CountUnsetBits
        {
            [Theory]
            [InlineData(new bool[] {}, 0)]
            [InlineData(new [] { true, true, true }, 0)]
            [InlineData(new [] { true, false }, 1)]
            [InlineData(new [] { false, true }, 1)]
            [InlineData(new [] { false, false, false, false, true, false, true, false, true, false, true, false }, 8)]
            public void CountAsExpected(bool[] bits, int expectedCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                if (bits.Any())
                {
                    builder.AppendRange(bits);
                }

                // Act
                var actualCount = builder.CountUnsetBits();

                // Assert
                Assert.Equal(expectedCount, actualCount);
            }
        }

        public class Resize
        {
            [Fact]
            public void LengthHasExpectedValueAfterResize()
            {
                var builder = new ArrowBuffer.BitPackedBuilder();
                builder.Resize(16);

                Assert.True(builder.BitCapacity >= 16);
                Assert.Equal(16, builder.BitCount);
            }

            [Fact]
            public void NegativeLengthResizesToZero()
            {
                var builder = new ArrowBuffer.BitPackedBuilder();
                builder.Append(false);
                builder.Append(true);
                builder.Resize(-1);

                Assert.Equal(0, builder.BitCount);
            }
        }

        public class Reserve
        {
            [Theory]
            [InlineData(0, 0, 0)]
            [InlineData(0, 0, 8)]
            [InlineData(8, 8, 8)]
            [InlineData(8, 8, 16)]
            public void CapacityIncreased(int initialCapacity, int numBitsToAppend, int additionalCapacity)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder(initialCapacity);
                builder.AppendRange(Enumerable.Repeat(true, numBitsToAppend));

                // Act
                builder.Reserve(additionalCapacity);

                // Assert
                Assert.True(builder.BitCapacity >= numBitsToAppend + additionalCapacity);
            }

            [Fact]
            public void NegtativeCapacityThrows()
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();

                // Act/Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => builder.Reserve(-1));
            }
        }

        public class Set
        {
            [Theory]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                2,
                new byte[] { 0b01010101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                3,
                new byte[] { 0b01011101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                8,
                new byte[] { 0b01010101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                9,
                new byte[] { 0b01010101, 0b00000011 })]
            public void OverloadWithNoValueParameterSetsAsExpected(bool[] bits, int indexToSet, byte[] expectedBytes)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                builder.AppendRange(bits);

                // Act
                builder.Set(indexToSet);

                // Assert
                var buf = builder.Build();
                AssertBuffer(expectedBytes, buf);
            }

            [Theory]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                2, true,
                new byte[] { 0b01010101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                2, false,
                new byte[] { 0b01010001, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                3, true,
                new byte[] { 0b01011101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                3, false,
                new byte[] { 0b01010101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                8, true,
                new byte[] { 0b01010101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                8, false,
                new byte[] { 0b01010101, 0b00000000 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                9, true,
                new byte[] { 0b01010101, 0b00000011 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                9, false,
                new byte[] { 0b01010101, 0b00000001 })]
            public void OverloadWithValueParameterSetsAsExpected(
                bool[] bits, int indexToSet, bool valueToSet, byte[] expectedBytes)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                builder.AppendRange(bits);

                // Act
                builder.Set(indexToSet, valueToSet);

                // Assert
                var buf = builder.Build();
                AssertBuffer(expectedBytes, buf);
            }

            [Theory]
            [InlineData(0, -1)]
            [InlineData(0, 0)]
            [InlineData(1, 1)]
            [InlineData(10, 10)]
            [InlineData(10, 11)]
            public void BadIndexThrows(int numBitsToAppend, int indexToSet)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                var bits = Enumerable.Repeat(true, numBitsToAppend);
                builder.AppendRange(bits);

                // Act/Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => builder.Set(indexToSet));
                Assert.Throws<ArgumentOutOfRangeException>(() => builder.Set(indexToSet, true));
            }
        }

        public class Swap
        {
            [Theory]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                0, 2,
                new byte[] { 0b01010101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                0, 3,
                new byte[] { 0b01011100, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                4, 8,
                new byte[] { 0b01010101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                4, 9,
                new byte[] { 0b01000101, 0b00000011 })]
            public void SwapsAsExpected(bool[] bits, int firstIndex, int secondIndex, byte[] expectedBytes)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                builder.AppendRange(bits);

                // Act
                builder.Swap(firstIndex, secondIndex);

                // Assert
                var buf = builder.Build();
                AssertBuffer(expectedBytes, buf);
            }

            [Theory]
            [InlineData(0, -1, 0)]
            [InlineData(0, 0, -1)]
            [InlineData(0, 0, 0)]
            [InlineData(1, 0, 1)]
            [InlineData(1, 1, 0)]
            [InlineData(1, 0, -1)]
            [InlineData(1, -1, 0)]
            [InlineData(1, 1, 1)]
            [InlineData(10, 10, 0)]
            [InlineData(10, 0, 10)]
            [InlineData(10, 10, 10)]
            [InlineData(10, 11, 0)]
            [InlineData(10, 0, 11)]
            [InlineData(10, 11, 11)]
            public void BadIndicesThrows(int numBitsToAppend, int firstIndex, int secondIndex)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                var bits = Enumerable.Repeat(true, numBitsToAppend);
                builder.AppendRange(bits);

                // Act/Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => builder.Swap(firstIndex, secondIndex));
            }
        }

        public class Toggle
        {
            [Theory]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                2,
                new byte[] { 0b01010001, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                3,
                new byte[] { 0b01011101, 0b00000001 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                8,
                new byte[] { 0b01010101, 0b00000000 })]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                9,
                new byte[] { 0b01010101, 0b00000011 })]
            public void TogglesAsExpected(bool[] bits, int indexToToggle, byte[] expectedBytes)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                builder.AppendRange(bits);

                // Act
                builder.Toggle(indexToToggle);

                // Assert
                var buf = builder.Build();
                AssertBuffer(expectedBytes, buf);
            }

            [Theory]
            [InlineData(0, -1)]
            [InlineData(0, 0)]
            [InlineData(1, 1)]
            [InlineData(10, 10)]
            [InlineData(10, 11)]
            public void BadIndexThrows(int numBitsToAppend, int indexToToggle)
            {
                // Arrange
                var builder = new ArrowBuffer.BitPackedBuilder();
                var bits = Enumerable.Repeat(true, numBitsToAppend);
                builder.AppendRange(bits);

                // Act/Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => builder.Toggle(indexToToggle));
            }
        }

        private static void AssertBuffer(byte[] expectedBytes, ArrowBuffer buf)
        {
            Assert.True(buf.Length >= expectedBytes.Length);
            for (var i = 0; i < expectedBytes.Length; i++)
            {
                Assert.Equal(expectedBytes[i], buf.Span[i]);
            }
        }
    }
}