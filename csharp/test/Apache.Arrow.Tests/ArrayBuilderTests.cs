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

using Apache.Arrow.Types;
using System;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrayBuilderTests
    {
        // TODO: Test various builder invariants (Append, AppendRange, Clear, Resize, Reserve, etc)

        [Fact]
        public void PrimitiveArrayBuildersProduceExpectedArray()
        {
            TestArrayBuilder<Int8Array, Int8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int16Array, Int16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int32Array, Int32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<Int64Array, Int64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt8Array, UInt8Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt16Array, UInt16Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt32Array, UInt32Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<UInt64Array, UInt64Array.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<FloatArray, FloatArray.Builder>(x => x.Append(10).Append(20).Append(30));
            TestArrayBuilder<DoubleArray, DoubleArray.Builder>(x => x.Append(10).Append(20).Append(30));
        }

        public class TimestampArrayBuilder
        {
            [Fact]
            public void ProducesExpectedArray()
            {
                var now = DateTimeOffset.UtcNow.ToLocalTime();
                var array = new TimestampArray.Builder(TimeUnit.Nanosecond, TimeZoneInfo.Local.Id)
                    .Append(now)
                    .Build();

                Assert.Equal(1, array.Length);
                Assert.NotNull(array.GetTimestamp(0));
                Assert.Equal(now.Truncate(TimeSpan.FromTicks(100)), array.GetTimestamp(0).Value);
            }
        }

        private static void TestArrayBuilder<TArray, TArrayBuilder>(Action<TArrayBuilder> action)
            where TArray: IArrowArray
            where TArrayBuilder: IArrowArrayBuilder<TArray>, new()
        {
            var builder = new TArrayBuilder();
            action(builder);
            var array = builder.Build(default);

            Assert.IsAssignableFrom<TArray>(array);
            Assert.NotNull(array);
            Assert.Equal(3, array.Length);
            Assert.Equal(0, array.NullCount);
        }
        
    }
}
