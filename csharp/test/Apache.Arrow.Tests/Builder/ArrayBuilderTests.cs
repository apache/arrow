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
using Apache.Arrow.Values;
using Xunit;

namespace Apache.Arrow.Tests.Builder
{
    public class ArrayBuilderTests
    {
        [Fact]
        public void ArrayBuilder_Should_InferFromCSharp()
        {
            var builder = new PrimitiveArrayBuilder<long>() as ArrayBuilder;

            builder
                .AppendValue(ScalarFactory.MakeScalar(123L))
                .AppendNull()
                .AppendValues(ScalarFactory.MakeScalar(new long?[] { 1L, null, -12L}).Values);

            var built = builder.Build() as Int64Array;

            Assert.Equal(5, built.Length);
            Assert.Equal(2, built.NullCount);

            Assert.Equal(123L, built.GetValue(0));
            Assert.Equal(1L, built.GetValue(2));
            Assert.Equal(-12L, built.GetValue(4));

            Assert.True(built.IsValid(0));
            Assert.False(built.IsValid(1));
            Assert.False(built.IsValid(3));
            Assert.True(built.IsValid(4));
        }
    }

    public class ValueArrayBuilderTests
    {
        [Fact]
        public void ValueArrayBuilder_Should_InferFromCSharp()
        {
            var builder = new PrimitiveArrayBuilder<int>();

            builder.AppendValue(123).AppendValues(new int?[] { 1, null, -12 }).AppendNull();

            var built = builder.Build() as Int32Array;

            Assert.Equal(5, built.Length);
            Assert.Equal(2, built.NullCount);

            Assert.Equal(123, built.GetValue(0));
            Assert.Equal(1, built.GetValue(1));
            Assert.Equal(-12, built.GetValue(3));

            Assert.True(built.IsValid(0));
            Assert.False(built.IsValid(2));
            Assert.False(built.IsValid(4));
        }
    }
}
