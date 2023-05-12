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
using Apache.Arrow.Builder;
using Xunit;

namespace Apache.Arrow.Tests.Builder
{
    public class BufferBuilderTests
    {
        [Fact]
        public void BufferBuilder_Should_AppendBytes()
        {
            var builder = new BufferBuilder(8);

            builder.AppendByte(0x01);

            Assert.Equal(1, builder.ByteLength);
            Assert.Equal(0, builder.BitOffset);

            builder.AppendBytes(new byte[] { 0x02, 0x03 });

            Assert.Equal(3, builder.ByteLength);
            Assert.Equal(0, builder.BitOffset);

            var built = builder.Build();

            Assert.Equal(0x01, built.Span[0]);
            Assert.Equal(0x02, built.Span[1]);
            Assert.Equal(0x03, built.Span[2]);
            Assert.Equal(0x00, built.Span[3]);

            Assert.Equal(64, built.Length);
        }

        [Fact]
        public void BufferBuilder_Should_AppendBits()
        {
            var builder = new BufferBuilder(8);

            builder.AppendBit(true);

            Assert.Equal(0, builder.ByteLength);
            Assert.Equal(1, builder.BitOffset);

            builder.AppendBits(new bool[] { true, false, true });

            Assert.Equal(0, builder.ByteLength);
            Assert.Equal(4, builder.BitOffset);

            builder.AppendBits(new bool[] { true, false, true, false, true, true });

            Assert.Equal(1, builder.ByteLength);
            Assert.Equal(2, builder.BitOffset);

            builder.AppendBits(new bool[] { false, false, true });

            Assert.Equal(1, builder.ByteLength);
            Assert.Equal(5, builder.BitOffset);

            var built = builder.Build();

            Assert.Equal(0b_01011011, built.Span[0]);
            Assert.Equal(0b_00010011, built.Span[1]);
            Assert.Equal(0b_00000000, built.Span[2]);

            Assert.True(BitUtility.GetBit(built.Span, 9));

            Assert.Equal(64, built.Length);
        }

        [Fact]
        public void BufferBuilder_Should_AppendBitsLargeRange()
        {
            var builder = new BufferBuilder(8);

            builder.AppendBit(true);
            builder.AppendBits(new bool[] { false, true, false, true, false, true });
            builder.AppendBits(new bool[] { false, true, false, true, false, true, false, true, false, true, false, true });

            var built = builder.Build();

            Assert.True(BitUtility.GetBit(built.Span, 0));
            Assert.False(BitUtility.GetBit(built.Span, 1));
            Assert.True(BitUtility.GetBit(built.Span, 2));
            Assert.False(BitUtility.GetBit(built.Span, 17));
            Assert.True(BitUtility.GetBit(built.Span, 18));
        }

        [Fact]
        public void BufferBuilder_Should_AppendBitRepetitionLarge()
        {
            var builder = new BufferBuilder(8);

            builder.AppendBits(true, 12);
            builder.AppendBit(false);

            var built = builder.Build();

            Assert.True(BitUtility.GetBit(built.Span, 4));
            Assert.True(BitUtility.GetBit(built.Span, 5));
            Assert.True(BitUtility.GetBit(built.Span, 6));
            Assert.True(BitUtility.GetBit(built.Span, 7));
            Assert.False(BitUtility.GetBit(built.Span, 12));
        }

        [Fact]
        public void BufferBuilder_Should_AppendBitRepetitionLarge_AfterAppend()
        {
            var builder = new BufferBuilder(8);

            builder.AppendBit(false);
            builder.AppendBit(false);
            builder.AppendBits(true, 12);
            builder.AppendBit(false);

            var built = builder.Build();

            Assert.False(BitUtility.GetBit(built.Span, 0));
            Assert.False(BitUtility.GetBit(built.Span, 1));
            Assert.True(BitUtility.GetBit(built.Span, 5));
            Assert.True(BitUtility.GetBit(built.Span, 6));
            Assert.True(BitUtility.GetBit(built.Span, 7));
            Assert.False(BitUtility.GetBit(built.Span, 14));
        }

        [Fact]
        public void BufferBuilder_Should_AppendBitRepetition()
        {
            var builder = new BufferBuilder(8);

            builder.AppendBit(true);
            builder.AppendBits(false, 3);
            builder.AppendBits(true, 4);

            var built = builder.Build();

            Assert.True(BitUtility.GetBit(built.Span, 0));
            Assert.False(BitUtility.GetBit(built.Span, 1));
            Assert.False(BitUtility.GetBit(built.Span, 2));
            Assert.False(BitUtility.GetBit(built.Span, 3));
            Assert.True(BitUtility.GetBit(built.Span, 4));
            Assert.True(BitUtility.GetBit(built.Span, 5));
            Assert.True(BitUtility.GetBit(built.Span, 6));
            Assert.True(BitUtility.GetBit(built.Span, 7));
        }

        [Fact]
        public void BufferBuilder_Should_AppendStructs()
        {
            var builder = new BufferBuilder(8);

            builder.AppendValue(123);
            builder.AppendValues<int>(new int[] { 0, -1 });

            var built = builder.Build();
            var results = built.Span.CastTo<int>();

            Assert.Equal(123, results[0]);
            Assert.Equal(0, results[1]);
            Assert.Equal(-1, results[2]);
            Assert.Equal(0, results[3]);

            Assert.Equal(64, built.Length);
        }
    }
}
