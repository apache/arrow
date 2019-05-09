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
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowBufferBuilderTests
    {
        public class Append
        {

            [Fact]
            public void DoesNotThrowWithNullParameters()
            {
                var builder = new ArrowBuffer.Builder<int>();

                builder.AppendRange(null);
                builder.Append((Func<IEnumerable<int>>) null);
            }

            [Fact]
            public void CapacityOnlyGrowsWhenLengthWillExceedCapacity()
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var capacity = builder.Capacity;

                builder.Append(1);

                Assert.Equal(capacity, builder.Capacity);
            }

            [Fact]
            public void CapacityGrowsAfterAppendWhenLengthExceedsCapacity()
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var capacity = builder.Capacity;

                builder.Append(1);
                builder.Append(2);

                Assert.True(builder.Capacity > capacity);
            }

            [Fact]
            public void CapacityGrowsAfterAppendSpan()
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var capacity = builder.Capacity;
                var data = Enumerable.Range(0, 10).Select(x => x).ToArray();

                builder.Append(data);

                Assert.True(builder.Capacity > capacity);
            }

            [Fact]
            public void LengthIncrementsAfterAppend()
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var length = builder.Length;

                builder.Append(1);

                Assert.Equal(length + 1, builder.Length);
            }

            [Fact]
            public void LengthGrowsBySpanLength()
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var data = Enumerable.Range(0, 10).Select(x => x).ToArray();

                builder.Append(data);

                Assert.Equal(10, builder.Length);
            }

            [Fact]
            public void BufferHasExpectedValues()
            {
                var builder = new ArrowBuffer.Builder<int>(1);

                builder.Append(10);
                builder.Append(20);

                var buffer = builder.Build();
                var span = buffer.Span.CastTo<int>();

                Assert.Equal(10, span[0]);
                Assert.Equal(20, span[1]);
                Assert.Equal(0, span[2]);
            }
        }

        public class AppendRange
        {
            [Fact]
            public void CapacityGrowsAfterAppendEnumerable()
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var capacity = builder.Capacity;
                var data = Enumerable.Range(0, 10).Select(x => x);

                builder.AppendRange(data);

                Assert.True(builder.Capacity > capacity);
            }

            [Fact]
            public void LengthGrowsByEnumerableCount()
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var length = builder.Length;
                var data = Enumerable.Range(0, 10).Select(x => x).ToArray();
                var count = data.Length;
                
                builder.AppendRange(data);

                Assert.Equal(length + count, builder.Length);
            }

            [Fact]
            public void BufferHasExpectedValues()
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var data = Enumerable.Range(0, 10).Select(x => x).ToArray();

                builder.AppendRange(data);

                var buffer = builder.Build();
                var span = buffer.Span.CastTo<int>();

                for (var i = 0; i < 10; i++)
                {
                    Assert.Equal(i, span[i]);
                }
            }
        }

        public class Clear
        {
            [Theory]
            [InlineData(10)]
            [InlineData(100)]
            public void SetsAllValuesToDefault(int sizeBeforeClear)
            {
                var builder = new ArrowBuffer.Builder<int>(1);
                var data = Enumerable.Range(0, sizeBeforeClear).Select(x => x).ToArray();

                builder.AppendRange(data);
                builder.Clear();
                builder.Append(0);

                var buffer = builder.Build();
                // No matter the sizeBeforeClear, we only appended a single 0,
                // so the buffer length should be the smallest possible.
                Assert.Equal(64, buffer.Length);

                // check all 16 int elements are default
                var zeros = Enumerable.Range(0, 16).Select(x => 0).ToArray();
                var values = buffer.Span.CastTo<int>().Slice(0, 16).ToArray();

                Assert.True(zeros.SequenceEqual(values));
            }
        }
    }
}
