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
using System.Linq;
using Apache.Arrow.Memory;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class BinaryArrayBuilderTests
    {
        private static readonly MemoryAllocator _allocator = new NativeMemoryAllocator();

        // Various example byte arrays for use in testing.
        private static readonly byte[] _exampleNull = null;
        private static readonly byte[] _exampleEmpty = { };
        private static readonly byte[] _exampleNonEmpty1 = { 10, 20, 30, 40 };
        private static readonly byte[] _exampleNonEmpty2 = { 50, 60, 70, 80 };
        private static readonly byte[] _exampleNonEmpty3 = { 90 };

        // Base set of single bytes that may be used to append to a builder in testing.
        private static readonly byte[] _singleBytesToAppend = { 0, 123, 127, 255 };

        // Base set of byte arrays that may be used to append to a builder in testing.
        private static readonly byte[][] _byteArraysToAppend =
        {
            _exampleNull,
            _exampleEmpty,
            _exampleNonEmpty2,
            _exampleNonEmpty3,
        };

        // Base set of multiple byte arrays that may be used to append to a builder in testing.
        private static readonly byte[][][] _byteArrayArraysToAppend =
        {
            new byte[][] { },
            new[] { _exampleNull },
            new[] { _exampleEmpty },
            new[] { _exampleNonEmpty2 },
            new[] { _exampleNonEmpty2, _exampleNonEmpty3 },
            new[] { _exampleNonEmpty2, _exampleEmpty, _exampleNull },
        };

        // Base set of byte arrays that can be used as "initial contents" of any builder under test.
        private static readonly byte[][][] _initialContentsSet =
        {
            new byte[][] { },
            new[] { _exampleNull },
            new[] { _exampleEmpty },
            new[] { _exampleNonEmpty1 },
            new[] { _exampleNonEmpty1, _exampleNonEmpty3 },
            new[] { _exampleNonEmpty1, _exampleEmpty, _exampleNull },
        };

        public class Append
        {
            public static IEnumerable<object[]> _appendSingleByteTestData =
                from initialContents in _initialContentsSet
                from singleByte in _singleBytesToAppend
                select new object[] { initialContents, singleByte };

            [Theory]
            [MemberData(nameof(_appendSingleByteTestData))]
            public void AppendSingleByte(byte[][] initialContents, byte singleByte)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                int initialLength = builder.Length;
                int expectedLength = initialLength + 1;
                var expectedArrayContents = initialContents.Concat(new[] { new[] { singleByte } });

                // Act
                var actualReturnValue = builder.Append(singleByte);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            [Theory]
            [MemberData(nameof(_appendSingleByteTestData))]
            public void AppendSingleByteAfterClear(byte[][] initialContents, byte singleByte)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                builder.Clear();
                var expectedArrayContents = new[] { new[] { singleByte } };

                // Act
                var actualReturnValue = builder.Append(singleByte);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(1, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            public static readonly IEnumerable<object[]> _appendNullTestData =
                from initialContents in _initialContentsSet
                select new object[] { initialContents };

            [Theory]
            [MemberData(nameof(_appendNullTestData))]
            public void AppendNull(byte[][] initialContents)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                int initialLength = builder.Length;
                int expectedLength = initialLength + 1;
                var expectedArrayContents = initialContents.Concat(new byte[][] { null });

                // Act
                var actualReturnValue = builder.AppendNull();

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            [Theory]
            [MemberData(nameof(_appendNullTestData))]
            public void AppendNullAfterClear(byte[][] initialContents)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                builder.Clear();
                var expectedArrayContents = new byte[][] { null };

                // Act
                var actualReturnValue = builder.AppendNull();

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(1, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            public static readonly IEnumerable<object[]> _appendNonNullByteArrayTestData =
                from initialContents in _initialContentsSet
                from bytes in _byteArraysToAppend
                where bytes != null
                select new object[] { initialContents, bytes };

            [Theory]
            [MemberData(nameof(_appendNonNullByteArrayTestData))]
            public void AppendReadOnlySpan(byte[][] initialContents, byte[] bytes)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                int initialLength = builder.Length;
                var span = (ReadOnlySpan<byte>)bytes;
                int expectedLength = initialLength + 1;
                var expectedArrayContents = initialContents.Concat(new[] { bytes });

                // Act
                var actualReturnValue = builder.Append(span);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            [Theory]
            [MemberData(nameof(_appendNonNullByteArrayTestData))]
            public void AppendReadOnlySpanAfterClear(byte[][] initialContents, byte[] bytes)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                builder.Clear();
                var span = (ReadOnlySpan<byte>)bytes;
                var expectedArrayContents = new[] { bytes };

                // Act
                var actualReturnValue = builder.Append(span);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(1, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            public static readonly IEnumerable<object[]> _appendByteArrayTestData =
                from initialContents in _initialContentsSet
                from bytes in _byteArraysToAppend
                select new object[] { initialContents, bytes };

            [Theory]
            [MemberData(nameof(_appendByteArrayTestData))]
            public void AppendEnumerable(byte[][] initialContents, byte[] bytes)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                int initialLength = builder.Length;
                int expectedLength = initialLength + 1;
                var enumerable = (IEnumerable<byte>)bytes;
                var expectedArrayContents = initialContents.Concat(new[] { bytes });

                // Act
                var actualReturnValue = builder.Append(enumerable);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedLength, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            [Theory]
            [MemberData(nameof(_appendByteArrayTestData))]
            public void AppendEnumerableAfterClear(byte[][] initialContents, byte[] bytes)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                builder.Clear();
                var enumerable = (IEnumerable<byte>)bytes;
                var expectedArrayContents = new[] { bytes };

                // Act
                var actualReturnValue = builder.Append(enumerable);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(1, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }
        }

        public class AppendRange
        {
            public static readonly IEnumerable<object[]> _appendRangeSingleBytesTestData =
                from initialContents in _initialContentsSet
                select new object[] { initialContents, _singleBytesToAppend };

            [Theory]
            [MemberData(nameof(_appendRangeSingleBytesTestData))]
            public void AppendRangeSingleBytes(byte[][] initialContents, byte[] singleBytes)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                int initialLength = builder.Length;
                int expectedNewLength = initialLength + singleBytes.Length;
                var expectedArrayContents = initialContents.Concat(singleBytes.Select(b => new[] { b }));

                // Act
                var actualReturnValue = builder.AppendRange(singleBytes);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedNewLength, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);

            }

            [Theory]
            [MemberData(nameof(_appendRangeSingleBytesTestData))]
            public void AppendRangeSingleBytesAfterClear(byte[][] initialContents, byte[] singleBytes)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                builder.Clear();
                var expectedArrayContents = singleBytes.Select(b => new[] { b });

                // Act
                var actualReturnValue = builder.AppendRange(singleBytes);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(singleBytes.Length, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            public static readonly IEnumerable<object[]> _appendRangeByteArraysTestData =
                from initialContents in _initialContentsSet
                from byteArrays in _byteArrayArraysToAppend
                select new object[] { initialContents, byteArrays };

            [Theory]
            [MemberData(nameof(_appendRangeByteArraysTestData))]
            public void AppendRangeArrays(byte[][] initialContents, byte[][] byteArrays)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                int initialLength = builder.Length;
                int expectedNewLength = initialLength + byteArrays.Length;
                var expectedArrayContents = initialContents.Concat(byteArrays);

                // Act
                var actualReturnValue = builder.AppendRange(byteArrays);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(expectedNewLength, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }

            [Theory]
            [MemberData(nameof(_appendRangeByteArraysTestData))]
            public void AppendRangeArraysAfterClear(byte[][] initialContents, byte[][] byteArrays)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                if (initialContents.Length > 0)
                    builder.AppendRange(initialContents);
                builder.Clear();
                var expectedArrayContents = byteArrays;

                // Act
                var actualReturnValue = builder.AppendRange(byteArrays);

                // Assert
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(byteArrays.Length, builder.Length);
                var actualArray = builder.Build(_allocator);
                AssertArrayContents(expectedArrayContents, actualArray);
            }
        }

        public class Clear
        {
            [Fact]
            public void ClearEmpty()
            {
                // Arrange
                var builder = new BinaryArray.Builder();

                // Act
                var actualReturnValue = builder.Clear();

                // Assert
                Assert.NotNull(actualReturnValue);
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(0, builder.Length);
                var array = builder.Build(_allocator);
                Assert.Equal(0, array.Length);
            }

            public static readonly IEnumerable<object[]> _testData =
                from byteArrays in _byteArrayArraysToAppend
                select new object[] { byteArrays };

            [Theory]
            [MemberData(nameof(_testData))]
            public void ClearNonEmpty(byte[][] byteArrays)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                builder.AppendRange(byteArrays);

                // Act
                var actualReturnValue = builder.Clear();

                // Assert
                Assert.NotNull(actualReturnValue);
                Assert.Equal(builder, actualReturnValue);
                Assert.Equal(0, builder.Length);
                var array = builder.Build(_allocator);
                Assert.Equal(0, array.Length);
            }
        }

        public class Build
        {
            [Fact]
            public void BuildImmediately()
            {
                // Arrange
                var builder = new BinaryArray.Builder();

                // Act
                var array = builder.Build(_allocator);

                // Assert
                Assert.Equal(0, array.Length);
            }

            public static readonly IEnumerable<object[]> _testData =
                from ba1 in _initialContentsSet
                from ba2 in _byteArrayArraysToAppend
                select new object[] { ba1.Concat(ba2) };

            [Theory]
            [MemberData(nameof(_testData))]
            public void AppendThenBuild(byte[][] byteArrays)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                foreach (var byteArray in byteArrays)
                {
                    // Test the type of byte array to ensure each Append() overload is exercised.
                    if (byteArray == null)
                    {
                        builder.AppendNull();
                    }
                    else if (byteArray.Length == 1)
                    {
                        builder.Append(byteArray[0]);
                    }
                    else
                    {
                        builder.Append((ReadOnlySpan<byte>)byteArray);
                    }
                }

                // Act
                var array = builder.Build(_allocator);

                // Assert
                AssertArrayContents(byteArrays, array);
            }

            [Theory]
            [MemberData(nameof(_testData))]
            public void BuildMultipleTimes(byte[][] byteArrays)
            {
                // Arrange
                var builder = new BinaryArray.Builder();
                builder.AppendRange(byteArrays);
                builder.Build(_allocator);

                // Act
                var array = builder.Build(_allocator);

                // Assert
                AssertArrayContents(byteArrays, array);
            }
        }

        private static void AssertArrayContents(IEnumerable<byte[]> expectedContents, BinaryArray array)
        {
            var expectedContentsArr = expectedContents.ToArray();
            Assert.Equal(expectedContentsArr.Length, array.Length);
            for (int i = 0; i < array.Length; i++)
            {
                var expectedArray = expectedContentsArr[i];
                var actualSpan = array.GetBytes(i, out bool isNull);
                var actualArray = isNull ? null : actualSpan.ToArray();
                Assert.Equal(expectedArray, actualArray);
            }
        }
    }
}
