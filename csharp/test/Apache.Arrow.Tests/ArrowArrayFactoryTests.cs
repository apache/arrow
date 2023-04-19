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
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowArrayFactoryTests
    {
        [Fact]
        public void BuildArray_StringArray_ReturnsExpectedResult()
        {
            // Arrange
            string[] values = new[] { "foo", null, "baz" };

            // Act
            StringArray result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<StringArray>(result);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildArray_BinaryArray_ReturnsExpectedResult()
        {
            // Arrange
            IEnumerable<byte>[] values = new[]
            {
                new byte[] { 0x01, 0x02 },
                null,
                new byte[] { 0x05, 0x06 }
            };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<BinaryArray>(result);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildArray_BooleanArray_ReturnsExpectedResult()
        {
            // Arrange
            bool[] values = new[] { true, false, true };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<BooleanArray>(result);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildArray_NullableBooleanArray_ReturnsExpectedResult()
        {
            // Arrange
            bool?[] values = new bool?[] { true, null, false };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<BooleanArray>(result);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildArray_Int8Array_ReturnsExpectedResult()
        {
            // Arrange
            sbyte[] values = new sbyte[] { -128, 0, 127 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Int8Array>(result);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildArray_NullableInt8Array_ReturnsExpectedResult()
        {
            // Arrange
            sbyte?[] values = new sbyte?[] { -128, null, 127 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Int8Array>(result);
            
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildInt16Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<short> { 1, 2, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Int16Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildInt32Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<int> { 1, 2, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Int32Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildInt64Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<long> { 1, 2, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Int64Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildUInt8Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<byte> { 1, 2, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<UInt8Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildUInt16Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<ushort> { 1, 2, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<UInt16Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildUInt32Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<uint> { 1, 2, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<UInt32Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildUInt64Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<ulong> { 1, 2, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<UInt64Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }
#if NET5_0_OR_GREATER
        [Fact]
        public void BuildHalfFloatArray_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<Half> { (Half)1.1f, (Half)2.2f, (Half)3.3f };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<HalfFloatArray>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildNullableHalfFloatArray_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<Half?> { (Half)1.1, null, (Half)3.3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<HalfFloatArray>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }
#endif
        [Fact]
        public void BuildFloatArray_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<float> { 1.1f, 2.2f, 3.3f };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<FloatArray>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildDoubleArray_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<double> { 1.1, 2.2, 3.3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<DoubleArray>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildDecimal256Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<decimal> { 1.1m, 2.2m, 3.3m };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Decimal256Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildDecimal256Array_NullableValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<decimal?> { 1.1m, null, 3.3m };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Decimal256Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableInt16Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<short?> { 1, null, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Int16Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableInt32Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<int?> { 1, null, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Int32Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableInt64Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<long?> { 1, null, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Int64Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableUInt8Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<byte?> { 1, null, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<UInt8Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableUInt16Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<ushort?> { 1, null, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<UInt16Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableUInt32Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<uint?> { 1, 2, null, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<UInt32Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableUInt64Array_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<ulong?> { 1, 2, null, 3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<UInt64Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableFloatArray_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<float?> { 1.1f, 2.2f, null, 3.3f };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<FloatArray>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildNullableDoubleArray_ValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<double?> { 1.1, 2.2, null, 3.3 };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<DoubleArray>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildTimestampArray_DateTimeValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<DateTimeOffset> { DateTimeOffset.FromUnixTimeMilliseconds(1), DateTimeOffset.FromUnixTimeMilliseconds(-1) };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<TimestampArray>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildNullableTimestampArray_DateTimeValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<DateTimeOffset?> { DateTimeOffset.FromUnixTimeMilliseconds(1), null, DateTimeOffset.FromUnixTimeMilliseconds(-1) };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<TimestampArray>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }

        [Fact]
        public void BuildTimeSpanArray_TimeSpanValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<TimeSpan> { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(-1) };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Time64Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray(true));
        }

        [Fact]
        public void BuildNullableTimeSpanArray_TimeSpanValidValues_ReturnsCorrectArray()
        {
            // Arrange
            var values = new List<TimeSpan?> { TimeSpan.FromSeconds(1), null, TimeSpan.FromSeconds(-1) };

            // Act
            var result = ArrowArrayFactory.BuildArray(values);

            // Assert
            Assert.IsType<Time64Array>(result);
            Assert.Equal(values.Count, result.Length);
            Assert.Equal(values, result.ToArray());
        }
    }
}
