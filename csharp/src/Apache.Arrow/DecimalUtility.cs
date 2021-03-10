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
using System.Numerics;

namespace Apache.Arrow
{
    /// <summary>
    /// This is semi-optimised best attempt at converting to / from decimal and the buffers
    /// </summary>
    internal static class DecimalUtility
    {
        private static readonly BigInteger _maxDecimal = new BigInteger(decimal.MaxValue);
        private static readonly BigInteger _minDecimal = new BigInteger(decimal.MinValue);
        private static readonly ulong[] s_powersOfTen =
        {
            1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000,
            1000000000000, 10000000000000, 100000000000000, 1000000000000000, 10000000000000000, 100000000000000000,
            1000000000000000000, 10000000000000000000
        };

        private static int PowersOfTenLength => s_powersOfTen.Length - 1;

        internal static decimal GetDecimal(in ArrowBuffer valueBuffer, int index, int scale, int byteWidth)
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
                    throw new OverflowException($"Value: {integerPart} too big or too small to be represented as a decimal");
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

        internal static void GetBytes(decimal value, int precision, int scale, int byteWidth, Span<byte> bytes)
        {
            // create BigInteger from decimal
            BigInteger bigInt;
            int[] decimalBits = decimal.GetBits(value);
            int decScale = (decimalBits[3] >> 16) & 0x7F;
#if NETCOREAPP
            Span<byte> bigIntBytes = stackalloc byte[12];

                for (int i = 0; i < 3; i++)
                {
                    int bit = decimalBits[i];
                    Span<byte> intBytes = stackalloc byte[4];
                    if (!BitConverter.TryWriteBytes(intBytes, bit))
                        throw new OverflowException($"Could not extract bytes from int {bit}");

                    for (int j = 0; j < 4; j++)
                    {
                        bigIntBytes[4 * i + j] = intBytes[j];
                    }
                }
                bigInt = new BigInteger(bigIntBytes);
#else
            byte[] bigIntBytes = new byte[12];
                for (int i = 0; i < 3; i++)
                {
                    int bit = decimalBits[i];
                    byte[] intBytes = BitConverter.GetBytes(bit);
                    for (int j = 0; j < intBytes.Length; j++)
                    {
                        bigIntBytes[4 * i + j] = intBytes[j];
                    }
                }
                bigInt = new BigInteger(bigIntBytes);
#endif

            if (value < 0)
            {
                bigInt = -bigInt;
            }

            // validate precision and scale
            if (decScale > scale)
                throw new OverflowException($"Decimal scale cannot be greater than that in the Arrow vector: {decScale} != {scale}");

            if (bigInt >= BigInteger.Pow(10, precision))
                throw new OverflowException($"Decimal precision cannot be greater than that in the Arrow vector: {value} has precision > {precision}");

            if (decScale < scale) // pad with trailing zeros
            {
                bigInt *= BigInteger.Pow(10, scale - decScale);
            }

            // extract bytes from BigInteger
            if (bytes.Length != byteWidth)
            {
                throw new OverflowException($"ValueBuffer size not equal to {byteWidth} byte width: {bytes.Length}");
            }

            int bytesWritten;
#if NETCOREAPP
            if (!bigInt.TryWriteBytes(bytes, out bytesWritten, false, !BitConverter.IsLittleEndian))
                throw new OverflowException("Could not extract bytes from integer value " + bigInt);
#else
            byte[] tempBytes = bigInt.ToByteArray();
            tempBytes.CopyTo(bytes);
            bytesWritten = tempBytes.Length;
#endif

            if (bytes.Length > byteWidth)
            {
                throw new OverflowException($"Decimal size greater than {byteWidth} bytes: {bytes.Length}");
            }

            if (bigInt.Sign == -1)
            {
                for (int i = bytesWritten; i < byteWidth; i++)
                {
                    bytes[i] = 255;
                }
            }
        }
    }
}
