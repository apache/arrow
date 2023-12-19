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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Scalars
{
    [StructLayout(LayoutKind.Explicit)]
    public struct BinaryView : IEquatable<BinaryView>
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
        internal readonly int Data0;

        [FieldOffset(8)]
        internal readonly int Data1;

        [FieldOffset(12)]
        internal readonly int Data2;

        public unsafe BinaryView(int length, byte[] inlined) : this()
        {
            Length = length;
            fixed (int* dest = &Data0)
            fixed (byte* src = inlined)
            {
                Buffer.MemoryCopy(src, dest, 12, length);
            }
        }

        public unsafe BinaryView(int length, byte[] prefix, int bufferIndex, int offset)
        {
            Length = length;
            BufferIndex = bufferIndex;
            Offset = offset;
            fixed (int* dest = &Data0)
            fixed (byte* src = prefix)
            {
                *dest = *(int*)src;
            }
        }

        private BinaryView(int length, int prefix, int bufferIndex, int offset)
        {
            Length = length;
            Prefix = prefix;
            BufferIndex = bufferIndex;
            Offset = offset;
        }

        public bool IsInline => Length <= MaxInlineLength;

        public override int GetHashCode() => Length ^ Data0 ^ Data1 ^ Data2;

        public override bool Equals(object obj)
        {
            BinaryView? other = obj as BinaryView?;
            return other != null && Equals(other.Value);
        }

        public bool Equals(BinaryView other) => Length == other.Length && Data0 == other.Data0 && Data1 == other.Data1 && Data2 == other.Data2;

        internal BinaryView AdjustBufferIndex(int bufferOffset)
        {
            return new BinaryView(Length, Prefix, BufferIndex + bufferOffset, Offset);
        }
    }
}
