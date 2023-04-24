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
            Assert.Equal(0, builder.BitOverhead.Length);

            builder.AppendBytes(new byte[] { 0x02, 0x03 });

            Assert.Equal(3, builder.ByteLength);
            Assert.Equal(0, builder.BitOverhead.Length);

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
            Assert.Equal(1, builder.BitOverhead.Length);
            Assert.Equal(0b_10000000, builder.BitOverhead.ToByte);

            builder.AppendBits(new bool[] { true, false, true });

            Assert.Equal(0, builder.ByteLength);
            Assert.Equal(4, builder.BitOverhead.Length);
            Assert.Equal(0b_11010000, builder.BitOverhead.ToByte);

            builder.AppendBits(new bool[] { true, false, true, false, true, true });

            Assert.Equal(1, builder.ByteLength);
            Assert.Equal(2, builder.BitOverhead.Length);
            Assert.Equal(0b_11000000, builder.BitOverhead.ToByte);

            builder.AppendBits(new bool[] { false, false, true });

            Assert.Equal(1, builder.ByteLength);
            Assert.Equal(5, builder.BitOverhead.Length);
            Assert.Equal(0b_11001000, builder.BitOverhead.ToByte);

            var built = builder.Build();

            Assert.Equal(0b_11011010, built.Span[0]);
            Assert.Equal(0b_11001000, built.Span[1]);
            Assert.Equal(0b_00000000, built.Span[2]);

            Assert.Equal(64, built.Length);
        }
    }
}
