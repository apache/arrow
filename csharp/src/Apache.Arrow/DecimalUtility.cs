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
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Apache.Arrow
{
    /// <summary>
    /// This is semi-optimised best attempt at converting to / from decimal and the buffers
    /// </summary>
    internal static class DecimalUtility
    {
        private static readonly byte[] _minusOne = { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
        private static readonly BigInteger _maxDecimal = new BigInteger(decimal.MaxValue);
        private static readonly BigInteger _minDecimal = new BigInteger(decimal.MinValue);
        private static readonly ulong[] s_powersOfTen =
        {
            1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
            1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000, 100000000000000000,
            1000000000000000000, 10000000000000000000
        };
        private static int PowersOfTenLength => s_powersOfTen.Length - 1;


        public static decimal GetDecimal(in ArrowBuffer valueBuffer, int index, int scale, int byteWidth,
            bool isUnsigned = false)
        {
            int startIndex = index * byteWidth;
            ReadOnlySpan<byte> value = valueBuffer.Span.Slice(startIndex, byteWidth);
            BigInteger integerValue;

#if NETCOREAPP
            integerValue = new BigInteger(value);
#else
            integerValue = new BigInteger(value.ToArray());
#endif

            if (integerValue > _maxDecimal || integerValue < _minDecimal)
            {
                BigInteger scaleBy = BigInteger.Pow(10, scale);
                BigInteger integerPart = BigInteger.DivRem(integerValue, scaleBy, out BigInteger fractionalPart);
                if (integerPart > _maxDecimal || integerPart < _minDecimal) // decimal overflow, not much we can do here - C# needs a BigDecimal
                {
                    throw new OverflowException("Value: " + integerPart + " too big or too small to be represented as a decimal");
                }
                return (decimal)integerPart + DivideByScale(fractionalPart, scale);
            }
            else
            {
                return DivideByScale(integerValue, scale);
            }
        }

        private static decimal DivideByScale(BigInteger integerValue, int scale)
        {
            decimal result = (decimal)integerValue; // this cast is safe here
            int drop = scale;
            while (drop > PowersOfTenLength)
            {
                result /= s_powersOfTen[PowersOfTenLength];
                drop -= PowersOfTenLength;
            }

            result /= s_powersOfTen[drop];
            return result;
        }

        public static byte[] GetBytes(BigInteger integerValue, int byteWidth)
        {
            byte[] integerBytes = integerValue.ToByteArray();
            if (integerBytes.Length > byteWidth)
            {
                throw new OverflowException("Decimal size greater than " + byteWidth + " bytes: " + integerBytes.Length);
            }

            if (integerBytes.Length == byteWidth)
                return integerBytes;

            byte[] result = new byte[byteWidth];
            if (integerValue.Sign == -1)
            {
                Buffer.BlockCopy(integerBytes, 0, result, 0, integerBytes.Length);
                Buffer.BlockCopy(_minusOne, 0, result, integerBytes.Length, byteWidth - integerBytes.Length);
            } else
            {
                Buffer.BlockCopy(integerBytes, 0, result, 0, integerBytes.Length);
            }

            return result;
        }

        public static bool CheckPrecisionAndScale(decimal value, int precision, int scale, out BigInteger integerValue)
        {
            DecimalLayout layout = new DecimalLayout(value); // use in place of decimal.GetBits(value) to avoid an allocation
            integerValue = new BigInteger(BitConverter.GetBytes(layout.Lo).Concat(BitConverter.GetBytes(layout.Mid)).Concat(BitConverter.GetBytes(layout.Hi)).ToArray());

            if (layout.Scale > scale)
                throw new OverflowException("Decimal scale can not be greater than that in the Arrow vector: " + layout.Scale + " != " + scale);

            if(integerValue >= BigInteger.Pow(10, precision))
                throw new OverflowException("Decimal precision can not be greater than that in the Arrow vector: " + value + " has precision > " + precision);

            if (layout.Scale < scale) // pad with trailing zeros
            {
                integerValue *= BigInteger.Pow(10, scale - layout.Scale);
            }

            if (value < 0) // sign the big int
                integerValue = -integerValue;

            return true;
        }

        private static decimal GetDecimalFromBigInteger(BigInteger value, int scale)
        {
            var b = new BigIntegerLayout(value);
            if (b.Bits == null)
                return b.Sign;

            int length = b.Bits.Length;
            if (length > 3) throw new OverflowException("Decimal overflow");

            int lo = 0, mi = 0, hi = 0;

            unchecked
            {
                if (length > 2) hi = b.Bits[2];
                if (length > 1) mi = b.Bits[1];
                if (length > 0) lo = b.Bits[0];
            }

            return new decimal(lo, mi, hi, b.Sign < 0, (byte)scale);
        }

        [StructLayout(LayoutKind.Explicit)]
        readonly struct BigIntegerLayout
        {
            public BigIntegerLayout(BigInteger value)
            {
                this = default;
                bi = value;
            }

            [FieldOffset(0)] readonly BigInteger bi;
            [FieldOffset(0)] readonly int[] bits;

            public int Sign => bi.Sign;
            public int[] Bits =>  bits;
        }


        [StructLayout(LayoutKind.Explicit)]
        readonly struct DecimalLayout
        {
            public DecimalLayout(decimal value)
            {
                this = default;
                d = value;
            }

            [FieldOffset(0)] readonly decimal d;
            [FieldOffset(0)] readonly int flags;
            [FieldOffset(4)] readonly int hi;
            [FieldOffset(8)] readonly int lo;
            [FieldOffset(12)] readonly int mid;

            public int Scale => (flags >> 16) & 0x7F;
            public int Lo => lo;
            public int Mid => mid;
            public int Hi => hi;
        }
    }
}
