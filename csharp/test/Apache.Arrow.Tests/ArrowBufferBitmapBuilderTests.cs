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
    /// The <see cref="ArrowBufferBitmapBuilderTests"/> class provides unit tests for the
    /// <see cref="ArrowBuffer.BitmapBuilder"/> class.
    /// </summary>
    public class ArrowBufferBitmapBuilderTests
    {
        public class Append
        {
            [Theory]
            [InlineData(new bool[] {}, false, 1, 0, 1)]
            [InlineData(new bool[] {}, true, 1, 1, 0)]
            [InlineData(new[] { true, false }, true, 3, 2, 1)]
            [InlineData(new[] { true, false }, false, 3, 1, 2)]
            public void IncreasesLength(
                bool[] initialContents,
                bool valueToAppend,
                int expectedLength,
                int expectedSetBitCount,
                int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(initialContents);

                // Act
                var actualReturnValue = builder.Append(valueToAppend);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                Assert.True(builder.Capacity >= expectedLength);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
            }

            [Theory]
            [InlineData(new bool[] {}, false)]
            [InlineData(new bool[] {}, true)]
            [InlineData(new[] { true, false }, true)]
            [InlineData(new[] { true, false }, false)]
            public void AfterClearIncreasesLength(bool[] initialContentsToClear, bool valueToAppend)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(initialContentsToClear);
                builder.Clear();

                // Act
                var actualReturnValue = builder.Append(valueToAppend);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(1, builder.Length);
                Assert.True(builder.Capacity >= 1);
                Assert.Equal(valueToAppend ? 1 : 0, builder.SetBitCount);
                Assert.Equal(valueToAppend ? 0 : 1, builder.UnsetBitCount);
            }

            [Fact]
            public void IncreasesCapacityWhenRequired()
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                int initialCapacity = builder.Capacity;
                builder.AppendRange(true, initialCapacity); // Fill to capacity.

                // Act
                var actualReturnValue = builder.Append(true);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(initialCapacity + 1, builder.Length);
                Assert.True(builder.Capacity >= initialCapacity + 1);
            }
        }

        public class AppendSpan
        {
            [Theory]
            [InlineData(new byte[] { 0b00000110 }, 4, 4, 2, 2)]    
            [InlineData(new byte[] { 0b11111110 }, 4, 4, 3, 1)]
            [InlineData(new byte[] { 0b11111110, 0b00000001 }, 9, 9, 8, 1)]
            [InlineData(new byte[] { 0b11111001, 0b00000001 }, 9, 9, 7, 2)]
            public void BitsAreAppendedToEmptyBuilder(byte[] bytesToAppend,
                int validBits,
                int expectedLength,
                int expectedSetBitCount,
                int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();

                // Act
                var actualReturnValue = builder.Append(new Span<byte>(bytesToAppend), validBits);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                Assert.True(builder.Capacity >= expectedLength);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);

            }

            [Theory]
            [InlineData(new byte[] { 6 }, 4, 12, 10, 2)]
            [InlineData(new byte[] { 254 }, 4, 12, 11, 1)]
            [InlineData(new byte[] { 254, 1 }, 9, 17, 16, 1)]
            [InlineData(new byte[] { 249, 1 }, 9, 17, 15, 2)]
            public void BitsAreAppendedToBuilderContainingByteAllignedData(byte[] bytesToAppend,
                int validBits,
                int expectedLength,
                int expectedSetBitCount,
                int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(true, 8);

                // Act
                var actualReturnValue = builder.Append(new Span<byte>(bytesToAppend), validBits);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                Assert.True(builder.Capacity >= expectedLength);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
            }

            [Theory]
            [InlineData(new byte[] { 6 }, 4, 13, 11, 2)]
            [InlineData(new byte[] { 254 }, 4, 13, 12, 1)]
            [InlineData(new byte[] { 254, 1 }, 9, 18, 17, 1)]
            [InlineData(new byte[] { 249, 1 }, 9, 18, 16, 2)]
            public void BitsAreAppendedToBuilderContainingNotAllignedData(byte[] bytesToAppend,
                int validBits,
                int expectedLength,
                int expectedSetBitCount,
                int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(true, 9);

                // Act
                var actualReturnValue = builder.Append(new Span<byte>(bytesToAppend), validBits);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                Assert.True(builder.Capacity >= expectedLength);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
            }

            [Fact]
            public void EmptySpanAppendsCorrectNumberOfBits()
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(true, 8);

                // Act
                var actualReturnValue = builder.Append(Span<byte>.Empty, 8);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(16, builder.Length);
                Assert.True(builder.Capacity >= 16);
                Assert.Equal(16, builder.SetBitCount);
                Assert.Equal(0, builder.UnsetBitCount);
            }

            [Fact]
            public void ThrowsWhenLengthIsTooBig()
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(true, 8);

                // Act
                Assert.Throws<ArgumentException>(() => builder.Append(new byte[] { 0b0010111 }, 9));
                Assert.Throws<ArgumentException>(() => builder.Append(new byte[] { 0, 1, 3, 4 }, 33));
            }
        }

        public class AppendRange
        {
            [Theory]
            [InlineData(new bool[] {}, new bool[] {}, 0, 0, 0)]
            [InlineData(new bool[] {}, new[] { true, false }, 2, 1, 1)]
            [InlineData(new[] { true, false }, new bool[] {}, 2, 1, 1)]
            [InlineData(new[] { true, false }, new[] { true, false }, 4, 2, 2)]
            public void AppendingEnumerableIncreasesLength(
                bool[] initialContents,
                bool[] toAppend,
                int expectedLength,
                int expectedSetBitCount,
                int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(initialContents);

                // Act
                var actualReturnValue = builder.AppendRange(toAppend);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                Assert.True(builder.Capacity >= expectedLength);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
            }

            [Theory]
            [InlineData(new bool[] { }, true, 0, 0, 0, 0)]
            [InlineData(new bool[] { }, true, 2, 2, 2, 0)]
            [InlineData(new[] { true, false }, false, 0, 2, 1, 1)]
            [InlineData(new[] { true, false }, false, 2, 4, 1, 3)]
            [InlineData(new[] { true, false }, true, 2, 4, 3, 1)]
            public void AppendingValueMultipleTimesIncreasesLength(
                bool[] initialContents,
                bool valueToAppend,
                int numberOfTimes,
                int expectedLength,
                int expectedSetBitCount,
                int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(initialContents);

                // Act
                var actualReturnValue = builder.AppendRange(valueToAppend, numberOfTimes);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                Assert.True(builder.Capacity >= expectedLength);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
            }
        }

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
                var builder = new ArrowBuffer.BitmapBuilder();
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
                var builder = new ArrowBuffer.BitmapBuilder();
                var data = Enumerable.Repeat(true, numBitsBeforeClear).Select(x => x).ToArray();
                builder.AppendRange(data);

                // Act
                var actualReturnValue = builder.Clear();

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(0, builder.Length);
            }
        }

        public class Resize
        {
            [Theory]
            [InlineData(new bool[] {}, 256, 0, 256)]
            [InlineData(new[] { true, true, true, true}, 256, 4, 252)]
            [InlineData(new[] { false, false, false, false}, 256, 0, 256)]
            [InlineData(new[] { true, true, true, true}, 2, 2, 0)]
            [InlineData(new[] { true, true, true, true}, 0, 0, 0)]
            public void LengthHasExpectedValueAfterResize(
                bool[] bits, int newSize, int expectedSetBitCount, int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(bits);

                // Act
                var actualReturnValue = builder.Resize(newSize);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.True(builder.Capacity >= newSize);
                Assert.Equal(newSize, builder.Length);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
            }

            [Fact]
            public void NegativeLengthThrows()
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.Append(false);
                builder.Append(true);

                // Act/Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => builder.Resize(-1));
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
                var builder = new ArrowBuffer.BitmapBuilder(initialCapacity);
                builder.AppendRange(true, numBitsToAppend);

                // Act
                var actualReturnValue = builder.Reserve(additionalCapacity);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.True(builder.Capacity >= numBitsToAppend + additionalCapacity);
            }

            [Fact]
            public void NegtativeCapacityThrows()
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();

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
                new byte[] { 0b01010101, 0b00000001 },
                5, 5)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                3,
                new byte[] { 0b01011101, 0b00000001 },
                6, 4)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                8,
                new byte[] { 0b01010101, 0b00000001 },
                5, 5)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                9,
                new byte[] { 0b01010101, 0b00000011 },
                6, 4)]
            public void OverloadWithNoValueParameterSetsAsExpected(
                bool[] bits, int indexToSet, byte[] expectedBytes,
                int expectedSetBitCount, int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(bits);

                // Act
                var actualReturnValue = builder.Set(indexToSet);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
                var buf = builder.Build();
                AssertBuffer(expectedBytes, buf);
            }

            [Theory]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                2, true,
                new byte[] { 0b01010101, 0b00000001 },
                5, 5)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                2, false,
                new byte[] { 0b01010001, 0b00000001 },
                4, 6)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                3, true,
                new byte[] { 0b01011101, 0b00000001 },
                6, 4)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                3, false,
                new byte[] { 0b01010101, 0b00000001 },
                5, 5)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                8, true,
                new byte[] { 0b01010101, 0b00000001 },
                5, 5)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                8, false,
                new byte[] { 0b01010101, 0b00000000 },
                4, 6)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                9, true,
                new byte[] { 0b01010101, 0b00000011 },
                6, 4)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                9, false,
                new byte[] { 0b01010101, 0b00000001 },
                5, 5)]
            public void OverloadWithValueParameterSetsAsExpected(
                bool[] bits, int indexToSet, bool valueToSet, byte[] expectedBytes,
                int expectedSetBitCount, int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(bits);

                // Act
                var actualReturnValue = builder.Set(indexToSet, valueToSet);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
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
                var builder = new ArrowBuffer.BitmapBuilder();
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
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(bits);

                // Act
                var actualReturnValue = builder.Swap(firstIndex, secondIndex);

                // Assert
                Assert.Equal(builder, actualReturnValue);
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
                var builder = new ArrowBuffer.BitmapBuilder();
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
                new byte[] { 0b01010001, 0b00000001 },
                4, 6)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                3,
                new byte[] { 0b01011101, 0b00000001 },
                6, 4)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                8,
                new byte[] { 0b01010101, 0b00000000 },
                4, 6)]
            [InlineData(
                new[] { true, false, true, false, true, false, true, false, true, false},
                9,
                new byte[] { 0b01010101, 0b00000011 },
                6, 4)]
            public void TogglesAsExpected(
                bool[] bits, int indexToToggle, byte[] expectedBytes,
                int expectedSetBitCount, int expectedUnsetBitCount)
            {
                // Arrange
                var builder = new ArrowBuffer.BitmapBuilder();
                builder.AppendRange(bits);

                // Act
                var actualReturnValue = builder.Toggle(indexToToggle);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedSetBitCount, builder.SetBitCount);
                Assert.Equal(expectedUnsetBitCount, builder.UnsetBitCount);
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
                var builder = new ArrowBuffer.BitmapBuilder();
                var bits = Enumerable.Repeat(true, numBitsToAppend);
                builder.AppendRange(bits);

                // Act/Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => builder.Toggle(indexToToggle));
            }
        }

        private static void AssertBuffer(byte[] expectedBytes, ArrowBuffer buf)
        {
            Assert.True(buf.Length >= expectedBytes.Length);
            for (int i = 0; i < expectedBytes.Length; i++)
            {
                Assert.Equal(expectedBytes[i], buf.Span[i]);
            }
        }
    }
}
