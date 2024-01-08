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
using Apache.Arrow.Scalars;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class BinaryViewTests
    {
        private static readonly byte[] empty = new byte[0];
        private static readonly byte[] oneByte = new byte[1];
        private static readonly byte[] fourBytes = new byte[] { 1, 2, 3, 4 };
        private static readonly byte[] fiveBytes = new byte[] { 5, 4, 3, 2, 1 };
        private static readonly byte[] twelveBytes = new byte[] { 1, 2, 3, 4, 8, 7, 6, 5, 9, 10, 11, 12 };
        private static readonly byte[] thirteenBytes = new byte[13];

        [Fact]
        public void Equality()
        {
            BinaryView one = new BinaryView(oneByte);
            BinaryView four = new BinaryView(fourBytes);
            BinaryView twelve = new BinaryView(twelveBytes);
            BinaryView twelvePlus = new BinaryView(13, fourBytes, 0, 0);
            Assert.Equal(one, one);
            Assert.NotEqual(one, four);
            Assert.NotEqual(four, twelve);
            Assert.NotEqual(four, twelvePlus);
        }

        [Fact]
        public void ConstructorThrows()
        {
            Assert.Throws<ArgumentException>(() => new BinaryView(thirteenBytes));
            Assert.Throws<ArgumentException>(() => new BinaryView(20, empty, 0, 0));
            Assert.Throws<ArgumentException>(() => new BinaryView(20, fiveBytes, 0, 0));
            Assert.Throws<ArgumentException>(() => new BinaryView(13, thirteenBytes, 0, 0));
            Assert.Throws<ArgumentException>(() => new BinaryView(4, fourBytes, 0, 0));
        }

        [Fact]
        public void ConstructInline()
        {
            BinaryView zero = new BinaryView(empty);
            Assert.Equal(-1, zero.BufferIndex);
            Assert.Equal(-1, zero.BufferOffset);
            Assert.Equal(0, zero.Length);
            Assert.Equal(0, zero.Bytes.Length);

            BinaryView one = new BinaryView(oneByte);
            Assert.Equal(-1, one.BufferIndex);
            Assert.Equal(-1, one.BufferOffset);
            Assert.Equal(1, one.Length);
            Assert.Equal(1, one.Bytes.Length);
            Assert.Equal((byte)0, one.Bytes[0]);

            BinaryView twelve = new BinaryView(twelveBytes);
            Assert.Equal(-1, one.BufferIndex);
            Assert.Equal(-1, one.BufferOffset);
            Assert.Equal(12, twelve.Length);
            Assert.Equal(12, twelve.Bytes.Length);
            Assert.Equal((byte)8, twelve.Bytes[4]);
        }

        [Fact]
        public void ConstructPrefix()
        {
            BinaryView four = new BinaryView(14, fourBytes, 2, 3);
            Assert.Equal(2, four.BufferIndex);
            Assert.Equal(3, four.BufferOffset);
            Assert.Equal(14, four.Length);
            Assert.Equal(4, four.Bytes.Length);
            Assert.Equal((byte)2, four.Bytes[1]);
        }
    }
}
