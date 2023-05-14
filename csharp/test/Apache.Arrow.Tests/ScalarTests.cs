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
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class IntegerScalarTests
    {
        [Fact]
        public void UInt8Scalar_Should_BuildFromDotNet()
        {
            byte expected = 128;
            var value = new UInt8Scalar(expected);
            byte casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt8Type.Default, value.Type);
            Assert.Equal(new byte[] { 128 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt8Scalar_Should_BuildFromMaxValue()
        {
            byte expected = byte.MaxValue;
            var value = new UInt8Scalar(expected);
            byte casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt8Type.Default, value.Type);
            Assert.Equal(new byte[] { byte.MaxValue }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt8Scalar_Should_BuildFromZero()
        {
            byte expected = 0;
            var value = new UInt8Scalar(expected);
            byte casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt8Type.Default, value.Type);
            Assert.Equal(new byte[] { 0 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int8Scalar_Should_BuildFromDotNet()
        {
            sbyte expected = -56;
            var value = new Int8Scalar(expected);
            sbyte casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int8Type.Default, value.Type);
            Assert.Equal(new byte[] { 200 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int8Scalar_Should_BuildFromMaxValue()
        {
            sbyte expected = sbyte.MaxValue;
            var value = new Int8Scalar(expected);
            sbyte casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int8Type.Default, value.Type);
            Assert.Equal(new byte[] { 127 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int8Scalar_Should_BuildFromMinValue()
        {
            sbyte expected = sbyte.MinValue;
            var value = new Int8Scalar(expected);
            sbyte casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int8Type.Default, value.Type);
            Assert.Equal(new byte[] { 128 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int16Scalar_Should_BuildFromDotNet()
        {
            short expected = -3456;
            var value = new Int16Scalar(expected);
            short casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int16Type.Default, value.Type);
            Assert.Equal(new byte[] { 128, 242 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int16Scalar_Should_BuildFromMaxValue()
        {
            short expected = short.MaxValue;
            var value = new Int16Scalar(expected);
            short casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int16Type.Default, value.Type);
            Assert.Equal(new byte[] { 255, 127 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int16Scalar_Should_BuildFromMinValue()
        {
            short expected = short.MinValue;
            var value = new Int16Scalar(expected);
            short casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int16Type.Default, value.Type);
            Assert.Equal(new byte[] { 0, 128 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt16Scalar_Should_BuildFromDotNet()
        {
            ushort expected = 56789;
            var value = new UInt16Scalar(expected);
            ushort casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt16Type.Default, value.Type);
            Assert.Equal(new byte[] { 213, 221 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt16Scalar_Should_BuildFromMaxValue()
        {
            ushort expected = ushort.MaxValue;
            var value = new UInt16Scalar(expected);
            ushort casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt16Type.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt16Scalar_Should_BuildFromZero()
        {
            ushort expected = 0;
            var value = new UInt16Scalar(expected);
            ushort casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt16Type.Default, value.Type);
            Assert.Equal(new byte[] { 0, 0 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int32Scalar_Should_BuildFromDotNet()
        {
            int expected = 123456789;
            var value = new Int32Scalar(expected);
            int casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int32Type.Default, value.Type);
            Assert.Equal(new byte[] { 21, 205, 91, 7 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int32Scalar_Should_BuildFromMaxValue()
        {
            int expected = int.MaxValue;
            var value = new Int32Scalar(expected);
            int casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int32Type.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255, 255, 127 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int32Scalar_Should_BuildFromMinValue()
        {
            int expected = int.MinValue;
            var value = new Int32Scalar(expected);
            int casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int32Type.Default, value.Type);
            Assert.Equal(new byte[] { 0, 0, 0, 128 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt32Scalar_Should_BuildFromDotNet()
        {
            uint expected = 3456789012;
            var value = new UInt32Scalar(expected);
            uint casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt32Type.Default, value.Type);
            Assert.Equal(new byte[] { 20, 106, 10, 206 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt32Scalar_Should_BuildFromMaxValue()
        {
            uint expected = uint.MaxValue;
            var value = new UInt32Scalar(expected);
            uint casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt32Type.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255, 255, 255 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt32Scalar_Should_BuildFromZero()
        {
            uint expected = 0;
            var value = new UInt32Scalar(expected);
            uint casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt32Type.Default, value.Type);
            Assert.Equal(new byte[] { 0, 0, 0, 0 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int64Scalar_Should_BuildFromDotNet()
        {
            long expected = 1234567890123456789;
            var value = new Int64Scalar(expected);
            long casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int64Type.Default, value.Type);
            Assert.Equal(new byte[] { 21, 129, 233, 125, 244, 16, 34, 17 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int64Scalar_Should_BuildFromMaxValue()
        {
            long expected = long.MaxValue;
            var value = new Int64Scalar(expected);
            long casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int64Type.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255, 255, 255, 255, 255, 255, 127 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Int64Scalar_Should_BuildFromMinValue()
        {
            long expected = long.MinValue;
            var value = new Int64Scalar(expected);
            long casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Int64Type.Default, value.Type);
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 128 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt64Scalar_Should_BuildFromDotNet()
        {
            ulong expected = 12345678901234567890;
            var value = new UInt64Scalar(expected);
            ulong casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt64Type.Default, value.Type);
            Assert.Equal(new byte[] { 210, 10, 31, 235, 140, 169, 84, 171 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt64Scalar_Should_BuildFromMaxValue()
        {
            ulong expected = ulong.MaxValue;
            var value = new UInt64Scalar(expected);
            ulong casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt64Type.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255, 255, 255, 255, 255, 255, 255 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void UInt64Scalar_Should_BuildFromZero()
        {
            ulong expected = 0;
            var value = new UInt64Scalar(expected);
            ulong casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(UInt64Type.Default, value.Type);
            Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, value.AsBytes().ToArray());
        }
    }

    public class FloatScalarTests
    {
        [Fact]
        public void FloatScalar_Should_BuildFromDotNet()
        {
            float expected = 1234.567f;
            var value = new FloatScalar(expected);
            float casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(FloatType.Default, value.Type);
            Assert.Equal(new byte[] { 37, 82, 154, 68 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void FloatScalar_Should_BuildFromMaxValue()
        {
            float expected = float.MaxValue;
            var value = new FloatScalar(expected);

            Assert.Equal(expected, value.DotNet);
            Assert.Equal(FloatType.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255, 127, 127 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void FloatScalar_Should_BuildFromMinValue()
        {
            float expected = float.MinValue;
            var value = new FloatScalar(expected);
            float casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(FloatType.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255, 127, 255 }, value.AsBytes().ToArray());
        }
    }

    public class HalfScalarTests
    {
        [Fact]
        public void HalfScalar_Should_BuildFromDotNet()
        {
            Half expected = Half.Epsilon;
            var value = new HalfFloatScalar(expected);
            Half casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(HalfFloatType.Default, value.Type);
            Assert.Equal(new byte[] { 1, 0 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void HalfScalar_Should_BuildFromMaxValue()
        {
            Half expected = Half.MaxValue;
            var value = new HalfFloatScalar(expected);
            Half casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(HalfFloatType.Default, value.Type);
            Assert.Equal(new byte[] { 255, 123 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void HalfScalar_Should_BuildFromMinValue()
        {
            Half expected = Half.MinValue;
            var value = new HalfFloatScalar(expected);
            Half casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(HalfFloatType.Default, value.Type);
            Assert.Equal(new byte[] { 255, 251 }, value.AsBytes().ToArray());
        }
    }

    public class DoubleScalarTests
    {
        [Fact]
        public void DoubleScalar_Should_BuildFromDotNet()
        {
            double expected = 1234.56789;
            var value = new DoubleScalar(expected);
            double casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(DoubleType.Default, value.Type);
            Assert.Equal(new byte[] { 231, 198, 244, 132, 69, 74, 147, 64 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void DoubleScalar_Should_BuildFromMaxValue()
        {
            double expected = double.MaxValue;
            var value = new DoubleScalar(expected);
            double casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(DoubleType.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255, 255, 255, 255, 255, 239, 127 }, value.AsBytes().ToArray());
        }

        [Fact]
        public void DoubleScalar_Should_BuildFromMinValue()
        {
            double expected = double.MinValue;
            var value = new DoubleScalar(expected);
            double casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(DoubleType.Default, value.Type);
            Assert.Equal(new byte[] { 255, 255, 255, 255, 255, 255, 239, 255 }, value.AsBytes().ToArray());
        }
    }

    public class Decimal128ScalarTests
    {
        [Fact]
        public void Decimal128Scalar_Should_BuildWithCustomType()
        {
            decimal expected = 123.00001m;
            var value = new Decimal128Scalar(new Decimal128Type(15,5), expected);
            decimal casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(new byte[] {
                225, 174, 187, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Decimal128Scalar_Should_BuildFromDotNet()
        {
            decimal expected = 123.00001m;
            var value = new Decimal128Scalar(expected);
            decimal casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(Decimal128Type.Default, value.Type);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(new byte[] {
                16, 53, 95, 163, 28, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Decimal128Scalar_Should_BuildFromNegativeDotNet()
        {
            decimal expected = -987.00001m;
            var value = new Decimal128Scalar(expected);
            decimal casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(Decimal128Type.Default, value.Type);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(new byte[] {
                240, 10, 55, 50, 26, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            }, value.AsBytes().ToArray());
        }
    }

    public class Decimal256ScalarTests
    {
        [Fact]
        public void Decimal256Scalar_Should_BuildWithCustomType()
        {
            decimal expected = 123.000000000000000000000001m;
            var value = new Decimal256Scalar(new Decimal256Type(32, 27), expected);
            decimal casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(new byte[] {
                232, 3, 0, 120, 67, 157, 45, 201, 32, 55, 111, 141, 1, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            }, value.AsBytes().ToArray());
        }

        [Fact]
        public void Decimal256Scalar_Should_BuildFromDotNet()
        {
            decimal expected = 123.000000000000000000000001m;
            var value = new Decimal256Scalar(expected);
            var bytes = new byte[] {
                16, 39, 0, 176, 162, 36, 200, 219, 71, 39, 88, 134, 15,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            };
            decimal casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Decimal256Type.SystemDefault, value.Type);
            Assert.Equal(bytes, value.AsBytes().ToArray());
        }

        [Fact]
        public void Decimal256Scalar_Should_BuildFromNegativeDotNet()
        {
            decimal expected = -987.000000000000000000000001m;
            var value = new Decimal256Scalar(expected);
            var bytes = new byte[] {
                240, 216, 255, 79, 199, 211, 79, 103, 166, 90, 79, 108, 131, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255
            };
            decimal casted = value;

            Assert.Equal(casted, value.DotNet);
            Assert.Equal(expected, value.DotNet);
            Assert.Equal(Decimal256Type.SystemDefault, value.Type);
            Assert.Equal(bytes, value.AsBytes().ToArray());
        }
    }
}
