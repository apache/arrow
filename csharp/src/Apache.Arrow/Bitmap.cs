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

using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    public struct Bitmap
    {
        public ArrowBuffer Buffer { get; }

        public int Length => Buffer.Size;

        public Bitmap(ArrowBuffer buffer)
        {
            Buffer = buffer;
        }

        public static implicit operator Bitmap(ArrowBuffer buffer)
        {
            return new Bitmap(buffer);
        }

        public static implicit operator ArrowBuffer(Bitmap bitmap)
        {
            return bitmap.Buffer;
        }

        public static Bitmap Allocate(int bitCount, MemoryPool memoryPool = default)
        {
            var size = bitCount / 8 + (bitCount % 8 > 0 ? 1 : 0);
            var remainder = size % 64;
            var len = (remainder == 0) ? size : size + 64 - remainder;
            
            // Allocate buffer from memory pool and enable all bits

            var buffer = ArrowBuffer.Allocate(len, memoryPool);
            var span = buffer.GetSpan<byte>();

            span.Fill(0xff);

            return new Bitmap(buffer);
        }

        public void Clear(int index)
        {
            BitUtility.ClearBit(
                Buffer.GetSpan<byte>(), index);
        }

        public void Set(int index)
        {
            BitUtility.SetBit(
                Buffer.GetSpan<byte>(), index);
        }

        public bool IsSet(int index)
        {
            return BitUtility.GetBit(
                Buffer.GetSpan<byte>(), index);
        }
    }
}
