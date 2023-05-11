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
        public void ArrayBuilder_Should_AppendArrowArray()
        {
            var builder = new FixedBinaryArrayBuilder<long>() as ArrayBuilder;
            long[] values = new long[] { 0, 1, -12 };

            var arrayBuilder = new FixedBinaryArrayBuilder<long>(values.Length);
            arrayBuilder.AppendValues(values);

            builder.AppendNull();
            builder.AppendArray(arrayBuilder.Build());

            var array = builder.Build() as Int64Array;

            Assert.Equal(4, array.Length);
            Assert.Equal(1, array.NullCount);

            Assert.False(array.IsValid(0));
            Assert.True(array.IsValid(2));

            Assert.Equal(0L, array.GetValue(1));
            Assert.Equal(1L, array.GetValue(2));
            Assert.Equal(-12L, array.GetValue(3));
        }
    }

    public class ValueArrayBuilderTests
    {
        [Fact]
        public void ValueArrayBuilder_Should_InferFromCSharp()
        {
            var builder = new FixedBinaryArrayBuilder<int>();

            builder.AppendValue(123);
            builder.AppendValues(new int[] { 1, 0, -12 });
            builder.AppendNull();

            var array = builder.Build() as Int32Array;

            Assert.Equal(5, array.Length);
            Assert.Equal(1, array.NullCount);

            Assert.Equal(123, array.GetValue(0));
            Assert.Equal(1, array.GetValue(1));
            Assert.Equal(-12, array.GetValue(3));

            Assert.True(array.IsValid(0));
            Assert.True(array.IsValid(2));
            Assert.False(array.IsValid(4));
        }
    }

    public class BinaryArrayBuilderTests
    {
        [Fact]
        public void BinaryArrayBuilder_Should_Build()
        {
            var builder = new BinaryArrayBuilder();

            builder.AppendValue(123);
            builder.AppendNull();
            builder.AppendValue(new byte[] { 1, 0, 12 });

            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(1, array.NullCount);

            Assert.Equal(new byte[] { 123 }, array.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(1).ToArray());
            Assert.Equal(new byte[] { 1, 0, 12 }, array.GetBytes(2).ToArray());

            Assert.True(array.IsValid(0));
            Assert.False(array.IsValid(1));
        }

        [Fact]
        public void StringArrayBuilder_Should_Build()
        {
            var builder = new StringArrayBuilder();

            builder.AppendValue("");
            builder.AppendNull();
            builder.AppendValue("def");

            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(1, array.NullCount);

            Assert.Equal("", array.GetString(0));
            Assert.Null(array.GetString(1));
            Assert.Equal("def", array.GetString(2));

            Assert.True(array.IsValid(0));
            Assert.False(array.IsValid(1));
        }

        [Fact]
        public void FixedSizeBinary_Should_InsertInArray()
        {
            var builder = new FixedBinaryArrayBuilder(new FixedSizeBinaryType(4));

            builder.AppendValue(new byte[] { 1, 2, 3, 4, 5 });
            builder.AppendNull();
            builder.AppendBytes(new byte[][]
                {
                    new byte[] { },
                    null,
                    new byte[] { 1, 5 },
                    new byte[] { 9, 8, 7, 6, 5, 4 }
                });
            var array = builder.Build() as FixedSizeBinaryArray;

            Assert.Equal(6, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.Equal(new byte[] { 1, 2, 3, 4 }, array.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(1).ToArray());
            Assert.Equal(new byte[] { 0, 0, 0, 0 }, array.GetBytes(2).ToArray());
            Assert.Equal(new byte[] { 1, 5, 0, 0 }, array.GetBytes(4).ToArray());
            Assert.Equal(new byte[] { 9, 8, 7, 6 }, array.GetBytes(5).ToArray());

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
            builder.GetBuilderAs<FixedBinaryArrayBuilder<int>>(1).AppendValue(1);
            // Append null block
            builder.AppendNull();

            StructArray array = builder.Build();

            Assert.IsType<StructArray>(array);
            Assert.Equal(2, array.Length);
            Assert.Equal(1, array.NullCount);

            Assert.True(array.IsValid(0));
            Assert.False(array.IsValid(1));
        }

        [Fact]
        public void StructArrayBuilder_Should_AppendScalar()
        {
            var builder = new StructArrayBuilder<TestStruct>();
            var scalar = new StructScalar(builder.DataType, new IScalar[]
            {
                new StringScalar("abc"), new Int32Scalar(123)
            });

            builder.AppendNull();
            builder.AppendValue(scalar);
            builder.AppendNull();

            StructArray array = builder.Build();
            var names = array.Fields[0] as StringArray;
            var int32s = array.Fields[1] as Int32Array;

            Assert.Equal(3, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.Equal("abc", names.GetString(1));
            Assert.Equal(123, int32s.GetValue(1));

            Assert.False(array.IsValid(0));
            Assert.True(array.IsValid(1));
            Assert.False(array.IsValid(2));
        }
    }

    public class ListArrayBuilderTests
    {
        [Fact]
        public void ListArrayBuilder_Should_Build()
        {
            var builder = new ListArrayBuilder<long>();

            builder.AppendNulls(3);
            builder.AppendValue(new long[] { 1, 0, -1 });
            builder.AppendNull();
            builder.AppendValues(new long[][]
                {
                    new long[] { 12, 3 },
                    new long[] {  }
                });
            var array = builder.Build();

            Assert.Equal(7, array.Length);
            Assert.Equal(4, array.NullCount);

            Assert.False(array.IsValid(0));
            Assert.False(array.IsValid(1));
            Assert.False(array.IsValid(2));
            Assert.True(array.IsValid(3));
            Assert.False(array.IsValid(4));
            Assert.True(array.IsValid(5));
        }
    }

    public struct TestStruct
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }
}
