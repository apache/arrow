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

namespace Apache.Arrow.Scalars
{
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct BinaryView : IEquatable<BinaryView>
    {
        public const int PrefixLength = 4;
        public const int MaxInlineLength = 12;

        [FieldOffset(0)]
        public readonly int Length;

        [FieldOffset(4)]
        internal readonly int _prefix;

        [FieldOffset(8)]
        internal readonly int _bufferIndex;

        [FieldOffset(12)]
        internal readonly int _bufferOffset;

        [FieldOffset(4)]
        internal fixed byte _inline[MaxInlineLength];

        public unsafe BinaryView(ReadOnlySpan<byte> inline) : this()
        {
            if (inline.Length > MaxInlineLength)
            {
                throw new ArgumentException("invalid inline data length", nameof(inline));
            }

            Length = inline.Length;
            fixed (byte* dest = _inline)
            fixed (byte* src = inline)
            {
                Buffer.MemoryCopy(src, dest, MaxInlineLength, inline.Length);
            }
        }

        public BinaryView(int length, ReadOnlySpan<byte> prefix, int bufferIndex, int bufferOffset)
        {
            if (length < MaxInlineLength)
            {
                throw new ArgumentException("invalid length", nameof(length));
            }
            if (prefix.Length != PrefixLength)
            {
                throw new ArgumentException("invalid prefix length", nameof(prefix));
            }

            Length = length;
            _bufferIndex = bufferIndex;
            _bufferOffset = bufferOffset;
            _prefix = prefix.CastTo<int>()[0];
        }

        private BinaryView(int length, int prefix, int bufferIndex, int offset)
        {
            Length = length;
            _prefix = prefix;
            _bufferIndex = bufferIndex;
            _bufferOffset = offset;
        }

        public bool IsInline => Length <= MaxInlineLength;

#if NET5_0_OR_GREATER
        public ReadOnlySpan<byte> Bytes => MemoryMarshal.CreateReadOnlySpan<byte>(ref Unsafe.AsRef(_inline[0]), IsInline ? Length : PrefixLength);
#else
        public unsafe ReadOnlySpan<byte> Bytes => new ReadOnlySpan<byte>(Unsafe.AsPointer(ref _inline[0]), IsInline ? Length : PrefixLength);
#endif

        public int BufferIndex => IsInline ? -1 : _bufferIndex;

        public int BufferOffset => IsInline ? -1 : _bufferOffset;

        public override int GetHashCode() => Length ^ _prefix ^ _bufferIndex ^ _bufferOffset;

        public override bool Equals(object obj)
        {
            BinaryView? other = obj as BinaryView?;
            return other != null && Equals(other.Value);
        }

        public bool Equals(BinaryView other) =>
            Length == other.Length && _prefix == other._prefix && _bufferIndex == other._bufferIndex && _bufferOffset == other._bufferOffset;

        internal BinaryView AdjustBufferIndex(int bufferOffset)
        {
            return new BinaryView(Length, _prefix, _bufferIndex + bufferOffset, _bufferOffset);
        }
    }
}
