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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Apache.Arrow
{
    public static class SpanExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Span<T> CastTo<T>(this Span<byte> span)
            where T: struct =>
            MemoryMarshal.Cast<byte, T>(span);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ReadOnlySpan<T> CastTo<T>(this ReadOnlySpan<byte> span)
            where T: struct =>
                MemoryMarshal.Cast<byte, T>(span);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyToFix<T>(this ReadOnlySpan<T> source, Span<T> target)
        {
            CopyToFix(source, 0, target, 0, source.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyToFix<T>(this ReadOnlySpan<T> source, T[] target)
        {
            CopyToFix(source, 0, target, 0, source.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyToFix<T>(this Span<T> source, Span<T> target)
        {
            CopyToFix(source, 0, target, 0, source.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyToFix<T>(this Memory<T> source, Memory<T> target)
        {
            CopyToFix(source.Span, 0, target.Span, 0, source.Length);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyToFix<T>(ReadOnlySpan<T> source, int sourceOffset, Span<T> target, int targetOffset, int length)
        {
            for (int i = 0; i < length; ++i)
            {
                target[targetOffset + i] = source[sourceOffset + i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyToFix<T>(ReadOnlySpan<T> source, int sourceOffset, T[] target, int targetOffset, int length)
        {
            for (int i = 0; i < length; ++i)
            {
                target[targetOffset + i] = source[sourceOffset + i];
            }
        }
    }
}
