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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Scalars
{
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct BinaryView : IEquatable<BinaryView>
    {
        public const int MaxInlineLength = 12;

        [FieldOffset(0)]
        public readonly int Length;

        [FieldOffset(4)]
        internal readonly int Prefix;

        [FieldOffset(8)]
        internal readonly int BufferIndex;

        [FieldOffset(12)]
        internal readonly int Offset;

        [FieldOffset(4)]
        internal fixed byte Inline[12];

        public unsafe BinaryView(ReadOnlySpan<byte> inlined) : this()
        {
            Length = inlined.Length;
            fixed (byte* dest = Inline)
            fixed (byte* src = inlined)
            {
                Buffer.MemoryCopy(src, dest, 12, inlined.Length);
            }
        }

        public BinaryView(int length, ReadOnlySpan<byte> prefix, int bufferIndex, int offset)
        {
            Debug.Assert(prefix.Length == 4);

            Length = length;
            BufferIndex = bufferIndex;
            Offset = offset;
            Prefix = prefix.CastTo<int>()[0];
        }

        private BinaryView(int length, int prefix, int bufferIndex, int offset)
        {
            Length = length;
            Prefix = prefix;
            BufferIndex = bufferIndex;
            Offset = offset;
        }

        public bool IsInline => Length <= MaxInlineLength;

        public override int GetHashCode() => Length ^ Prefix ^ BufferIndex ^ Offset;

        public override bool Equals(object obj)
        {
            BinaryView? other = obj as BinaryView?;
            return other != null && Equals(other.Value);
        }

        public bool Equals(BinaryView other) => Length == other.Length && Prefix == other.Prefix && BufferIndex == other.BufferIndex && Offset == other.Offset;

        internal BinaryView AdjustBufferIndex(int bufferOffset)
        {
            return new BinaryView(Length, Prefix, BufferIndex + bufferOffset, Offset);
        }
    }
}
