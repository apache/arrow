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

using Apache.Arrow.Arrays;
using Apache.Arrow.Builder;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests.Builder
{
    public class ArrayBuilderTests
    {
        [Fact]
        public void ArrayBuilder_Should_InferFromCSharp()
        {
            var builder = new PrimitiveArrayBuilder<long>() as ArrayBuilder;
            long[] values = new long[] { 0, 1, -12 };
            IArrowArray array = new PrimitiveArrayBuilder<long>(values.Length).AppendValues(values).Build();

            builder
                .AppendNull()
                .AppendValues(array);

            var built = builder.Build() as Int64Array;

            Assert.Equal(4, built.Length);
            Assert.Equal(1, built.NullCount);

            Assert.Equal(1L, built.GetValue(2));
            Assert.Equal(-12L, built.GetValue(3));

            Assert.False(built.IsValid(0));
            Assert.True(built.IsValid(2));
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

    public class BinaryArrayBuilderTests
    {
        [Fact]
        public void BinaryArrayBuilder_Should_Build()
        {
            var builder = new BinaryArrayBuilder();

            builder.AppendValue(123).AppendNull().AppendValue(new byte[] { 1, 0, 12 });

            var built = builder.Build() as BinaryArray;

            Assert.Equal(3, built.Length);
            Assert.Equal(1, built.NullCount);

            Assert.Equal(new byte[] { 123 }, built.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { }, built.GetBytes(1).ToArray());
            Assert.Equal(new byte[] { 1, 0, 12 }, built.GetBytes(2).ToArray());

            Assert.True(built.IsValid(0));
            Assert.False(built.IsValid(1));
        }

        [Fact]
        public void StringArrayBuilder_Should_Build()
        {
            var builder = new StringArrayBuilder();

            builder.AppendValue("").AppendNull().AppendValue("def");

            var built = builder.Build() as StringArray;

            Assert.Equal(3, built.Length);
            Assert.Equal(1, built.NullCount);

            Assert.Equal("", built.GetString(0));
            Assert.Null(built.GetString(1));
            Assert.Equal("def", built.GetString(2));

            Assert.True(built.IsValid(0));
            Assert.False(built.IsValid(1));
        }

        [Fact]
        public void FixedSizeBinary_Should_InsertInArray()
        {
            var builder = new FixedPrimitiveArrayBuilder<byte>(new FixedSizeBinaryType(4));

            var array = builder
                .AppendValue(new byte[] { 1, 2, 3, 4 })
                .AppendNull()
                .AppendValues(new byte[][]
                {
                    new byte[] { },
                    null,
                    new byte[] { 1, 5 }
                })
                .Build() as FixedSizeBinaryArray;

            Assert.Equal(5, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.Equal(new byte[] { 1, 2, 3, 4 }, array.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(1).ToArray());
            Assert.Equal(new byte[] { 1, 5, 0, 0 }, array.GetBytes(4).ToArray());

            Assert.True(array.IsValid(0));
            Assert.False(array.IsValid(1));
            Assert.True(array.IsValid(2));
            Assert.False(array.IsValid(3));
            Assert.True(array.IsValid(4));
        }
    }

    public class StructArrayBuilderTests
    {
        [Fact]
        public void StructArrayBuilder_Should_Build()
        {
            var builder = new StructArrayBuilder<TestStruct>();

            // Start new valid block, need to fill all builders
            builder.Append();
            builder.GetBuilderAs<StringArrayBuilder>(0).AppendValue("abc");
            builder.GetBuilderAs<PrimitiveArrayBuilder<int>>(1).AppendValue(1);
            // Append null block
            builder.AppendNull();

            StructArray built = builder.Build() as StructArray;

            Assert.IsType<StructArray>(built);
            Assert.Equal(2, built.Length);
            Assert.Equal(1, built.NullCount);

            Assert.True(built.IsValid(0));
            Assert.False(built.IsValid(1));
        }
    }

    public class ListArrayBuilderTests
    {
        [Fact]
        public void ListArrayBuilder_Should_Build()
        {
            var builder = new ListArrayBuilder(new ListType(new Int64Type()));

            ListArray built = builder
                .AppendValue<long>(new long[] { 1, 0, -1 })
                .AppendNull()
                .AppendValues(new long[][]
                {
                    new long[] { 12, 3 },
                    new long[] {  }
                })
                .Build() as ListArray;

            Assert.IsType<ListArray>(built);
            Assert.Equal(4, built.Length);
            Assert.Equal(1, built.NullCount);

            Assert.True(built.IsValid(0));
            Assert.False(built.IsValid(1));
            Assert.True(built.IsValid(2));
        }
    }

    public struct TestStruct
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }
}
