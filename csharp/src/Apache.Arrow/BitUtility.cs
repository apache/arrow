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
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Apache.Arrow
{
    public static class BitUtility
    {
        private static readonly byte[] PopcountTable = {
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
        };

        private static readonly byte[] BitMask = {
            1, 2, 4, 8, 16, 32, 64, 128
        };

        public static bool GetBit(byte data, int index) =>
            ((data >> index) & 1) != 0;

        public static bool GetBit(ReadOnlySpan<byte> data, int index) =>
            (data[index / 8] & BitMask[index % 8]) != 0;

        public static void ClearBit(Span<byte> data, int index)
        {
            data[index / 8] &= (byte) ~BitMask[index % 8];
        }

        public static void SetBit(Span<byte> data, int index)
        {
            data[index / 8] |= BitMask[index % 8];
        }

        public static void SetBit(Span<byte> data, int index, bool value)
        {
            var idx = index / 8;
            var mod = index % 8;
            data[idx] = value
                ? (byte)(data[idx] | BitMask[mod])
                : (byte)(data[idx] & ~BitMask[mod]);
        }

        public static void ToggleBit(Span<byte> data, int index)
        {
            data[index / 8] ^= BitMask[index % 8];
        }

        /// <summary>
        /// Counts the number of set bits in a span of bytes starting
        /// at a specific bit offset.
        /// </summary>
        /// <param name="data">Span to count bits</param>
        /// <param name="offset">Bit offset to start counting from</param>
        /// <returns>Count of set (one) bits/returns>
        public static int CountBits(ReadOnlySpan<byte> data, int offset)
        {
            var start = (offset / 8);
            var startBit = offset % 8;

            if (startBit < 0) return 0;
            if (startBit == 0) return CountBits(data);

            var count = 0;

            count += CountBits(data.Slice(start + 1));

            for (var i = startBit; i < 8; i++)
            {
                if (GetBit(data.Slice(start, 1), i))
                    count++;
            }

            return count;
        }

        /// <summary>
        /// Counts the number of set bits in a span of bytes.
        /// </summary>
        /// <param name="data">Span to count bits</param>
        /// <returns>Count of set (one) bits.</returns>
        public static int CountBits(ReadOnlySpan<byte> data)
        {
            var count = 0;
            foreach (var t in data)
                count += PopcountTable[t];
            return count;
        }

        /// <summary>
        /// Rounds an integer to the nearest multiple of 64.
        /// </summary>
        /// <param name="n">Integer to round.</param>
        /// <returns>Integer rounded to the nearest multiple of 64.</returns>
        public static long RoundUpToMultipleOf64(long n) =>
            RoundUpToMultiplePowerOfTwo(n, 64);

        /// <summary>
        /// Rounds an integer to the nearest multiple of 8.
        /// </summary>
        /// <param name="n">Integer to round.</param>
        /// <returns>Integer rounded to the nearest multiple of 8.</returns>
        public static long RoundUpToMultipleOf8(long n) =>
            RoundUpToMultiplePowerOfTwo(n, 8);

        /// <summary>
        /// Rounds an integer up to the nearest multiple of factor, where
        /// factor must be a power of two.
        /// 
        /// This function does not throw when the factor is not a power of two.
        /// </summary>
        /// <param name="n">Integer to round up.</param>
        /// <param name="factor">Power of two factor to round up to.</param>
        /// <returns>Integer rounded up to the nearest power of two.</returns>
        public static long RoundUpToMultiplePowerOfTwo(long n, int factor)
        {
            // Assert that factor is a power of two.
            Debug.Assert(factor > 0 && (factor & (factor - 1)) == 0);
            return (n + (factor - 1)) & ~(factor - 1);
        }

        /// <summary>
        /// Calculates the number of bytes required to store n bits.
        /// </summary>
        /// <param name="n">number of bits</param>
        /// <returns>number of bytes</returns>
        public static int ByteCount(int n)
        {
            Debug.Assert(n >= 0);
            return n / 8 + (n % 8 != 0 ? 1 : 0); // ceil(n / 8)
        }
            
        internal static int ReadInt32(ReadOnlyMemory<byte> value)
        {
            Debug.Assert(value.Length >= sizeof(int));

            return Unsafe.ReadUnaligned<int>(ref MemoryMarshal.GetReference(value.Span));
        }
    }
}
