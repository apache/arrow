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
using Apache.Arrow.Memory;
using Apache.Arrow.Tests.Fixtures;
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
            /// Ensure Arrow buffers are allocated in multiples of 8-bytes.
            /// </summary>
            /// <param name="size">number of bytes to allocate</param>
            /// <param name="expectedCapacity">expected buffer capacity after allocation</param>
            [Theory]
            [InlineData(1, 8)]
            [InlineData(8, 8)]
            [InlineData(9, 16)]
            [InlineData(16, 16)]
            public void AllocatesWithExpectedPadding(int size, int expectedCapacity)
            {
                var buffer = ArrowBuffer.Allocate(size, _memoryPoolFixture.MemoryPool);
                Assert.Equal(buffer.Capacity, expectedCapacity);
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
                var buffer = ArrowBuffer.Allocate(size, _memoryPoolFixture.MemoryPool);

                using (var pin = buffer.Memory.Pin())
                {
                    var ptr = new IntPtr(pin.Pointer);
                    Assert.True(ptr.ToInt64() % 64 == 0);
                }
            }

            /// <summary>
            /// Ensure padding in arrow buffers is initialized with zeroes.
            /// </summary>
            [Fact]
            public void HasZeroPadding()
            {
                var buffer = ArrowBuffer.Allocate(32, _memoryPoolFixture.MemoryPool);
                var span = buffer.GetSpan<byte>();

                foreach (var b in span)
                {
                    Assert.Equal(0, b);
                }
            }

        }
    }
}
