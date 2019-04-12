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

using Apache.Arrow.Tests.Fixtures;
using System;
using System.Runtime.InteropServices;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowBufferTests
    {
        public class Allocate : 
            IClassFixture<DefaultMemoryPoolFixture>
        {
            private readonly DefaultMemoryPoolFixture _memoryPoolFixture;

            public Allocate(DefaultMemoryPoolFixture memoryPoolFixture)
            {
                _memoryPoolFixture = memoryPoolFixture;
            }

            /// <summary>
            /// Ensure Arrow buffers are allocated in multiples of 64 bytes.
            /// </summary>
            /// <param name="size">number of bytes to allocate</param>
            /// <param name="expectedCapacity">expected buffer capacity after allocation</param>
            [Theory]
            [InlineData(1, 64)]
            [InlineData(8, 64)]
            [InlineData(9, 64)]
            [InlineData(65, 128)]
            public void AllocatesWithExpectedPadding(int size, int expectedCapacity)
            {
                var buffer = new ArrowBuffer.Builder<byte>(size).Build();

                Assert.Equal(buffer.Length, expectedCapacity);
            }

            /// <summary>
            /// Ensure allocated buffers are aligned to multiples of 64.
            /// </summary>
            [Theory]
            [InlineData(1)]
            [InlineData(8)]
            [InlineData(64)]
            [InlineData(128)]
            public unsafe void AllocatesAlignedToMultipleOf64(int size)
            {
                var buffer = new ArrowBuffer.Builder<byte>(size).Build();

                fixed (byte* ptr = &buffer.Span.GetPinnableReference())
                { 
                    Assert.True(new IntPtr(ptr).ToInt64() % 64 == 0);
                }
            }

            /// <summary>
            /// Ensure padding in arrow buffers is initialized with zeroes.
            /// </summary>
            [Fact]
            public void HasZeroPadding()
            {
                var buffer = new ArrowBuffer.Builder<byte>(10).Build();
                
                foreach (var b in buffer.Span)
                {
                    Assert.Equal(0, b);
                }
            }

        }

        [Fact]
        public void TestExternalMemoryWrappedAsArrowBuffer()
        {
            Memory<byte> memory = new byte[sizeof(int) * 3];
            Span<byte> spanOfBytes = memory.Span;
            var span = spanOfBytes.CastTo<int>();
            span[0] = 0;
            span[1] = 1;
            span[2] = 2;

            ArrowBuffer buffer = new ArrowBuffer(memory);
            Assert.Equal(2, buffer.Span.CastTo<int>()[2]);

            span[2] = 10;
            Assert.Equal(10, buffer.Span.CastTo<int>()[2]);
        }
    }
}
