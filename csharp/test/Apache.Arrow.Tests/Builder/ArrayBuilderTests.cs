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

using System.Threading.Tasks;
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

        [Fact]
        public void ArrayBuilder_Should_AppendArrowArraySlice()
        {
            var builder = new FixedBinaryArrayBuilder<long>() as ArrayBuilder;
            long?[] values = new long?[] { 0, 1, -12, null, 965 };

            var arrayBuilder = new FixedBinaryArrayBuilder<long>(values.Length);
            arrayBuilder.AppendValues(values);

            builder.AppendNull();
            builder.AppendArray(arrayBuilder.Build().Slice(1, 3));

            var array = builder.Build() as Int64Array;

            Assert.Equal(4, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.False(array.IsValid(0));
            Assert.True(array.IsValid(2));

            Assert.Equal(1L, array.GetValue(1));
            Assert.Equal(-12L, array.GetValue(2));
            Assert.Null(array.GetValue(3));
        }
    }

    public class BooleanArrayBuilderTests
    {
        [Fact]
        public void BooleanBuilder_Should_AppendBool()
        {
            var builder = new BooleanArrayBuilder();

            builder.AppendValue(true);
            builder.AppendNull();
            builder.AppendValues(new bool[] { true, false, true });
            builder.AppendNulls(3);
            builder.AppendValue(false);

            var array = builder.Build();

            Assert.Equal(9, array.Length);
            Assert.Equal(4, array.NullCount);

            Assert.True(array.IsValid(0));
            Assert.False(array.IsValid(1));
            Assert.True(array.IsValid(2));
            Assert.True(array.IsValid(3));
            Assert.True(array.IsValid(4));
            Assert.False(array.IsValid(5));
            Assert.False(array.IsValid(6));
            Assert.False(array.IsValid(7));
            Assert.True(array.IsValid(8));

            Assert.True(array.GetValue(0));
            Assert.Null(array.GetValue(1));
            Assert.True(array.GetValue(2));
            Assert.False(array.GetValue(3));
            Assert.True(array.GetValue(4));
            Assert.Null(array.GetValue(5));
            Assert.Null(array.GetValue(6));
            Assert.Null(array.GetValue(7));
            Assert.False(array.GetValue(8));
        }

        [Fact]
        public void BooleanBuilder_Should_AppendArray()
        {
            var builder = new BooleanArrayBuilder();

            builder.AppendValue(true);

            var vb = new BooleanArrayBuilder();
            vb.AppendNull();
            vb.AppendValues(new bool[] { true, false, true });
            vb.AppendNulls(3);

            builder.AppendArray(vb.Build());
            builder.AppendValue(false);

            var array = builder.Build();

            Assert.Equal(9, array.Length);
            Assert.Equal(4, array.NullCount);

            Assert.True(array.IsValid(0));
            Assert.False(array.IsValid(1));
            Assert.True(array.IsValid(2));
            Assert.True(array.IsValid(3));
            Assert.True(array.IsValid(4));
            Assert.False(array.IsValid(5));
            Assert.False(array.IsValid(6));
            Assert.False(array.IsValid(7));
            Assert.True(array.IsValid(8));

            Assert.True(array.GetValue(0));
            Assert.Null(array.GetValue(1));
            Assert.True(array.GetValue(2));
            Assert.False(array.GetValue(3));
            Assert.True(array.GetValue(4));
            Assert.Null(array.GetValue(5));
            Assert.Null(array.GetValue(6));
            Assert.Null(array.GetValue(7));
            Assert.False(array.GetValue(8));
        }
    }

    public class BinaryArrayBuilderTests
    {
        [Fact]
        public void BinaryBuilder_Should_AppendBinary()
        {
            var builder = new BinaryArrayBuilder();

            builder.AppendValue(123);
            builder.AppendNull();
            builder.AppendValue(new byte[] { 1, 0, 12 });
            builder.AppendValues(new byte[][] {
                new byte[] {  },
                null,
                new byte[] { 9, 42 }
            });

            var array = builder.Build();

            Assert.Equal(6, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.Equal(new byte[] { 123 }, array.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(1).ToArray());
            Assert.Equal(new byte[] { 1, 0, 12 }, array.GetBytes(2).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(3).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(4).ToArray());
            Assert.Equal(new byte[] { 9, 42 }, array.GetBytes(5).ToArray());

            Assert.True(array.IsValid(0));
            Assert.False(array.IsValid(1));
            Assert.False(array.IsValid(4));
            Assert.True(array.IsValid(5));
        }

        [Fact]
        public void BinaryBuilder_Should_AppendArray()
        {
            var builder = new BinaryArrayBuilder();

            var a0b = new BinaryArrayBuilder();
            a0b.AppendValues(new byte[][] {
                new byte[] {  },
                null,
                new byte[] { 9, 42 }
            });
            var a0 = a0b.Build();

            var a1b = new BinaryArrayBuilder();
            a1b.AppendValues(new byte[][] {
                null,
                new byte[] { 1, 2, 3, 4 },
                new byte[] { 8, 7 }
            });
            var a1 = a1b.Build();

            builder.AppendNull();
            builder.AppendArray(a0);
            builder.AppendArray(a1);

            var array = builder.Build();

            Assert.Equal(7, array.Length);
            Assert.Equal(3, array.NullCount);

            Assert.False(array.IsValid(0));
            Assert.True(array.IsValid(1));
            Assert.False(array.IsValid(2));
            Assert.True(array.IsValid(3));
            Assert.False(array.IsValid(4));
            Assert.True(array.IsValid(5));
            Assert.True(array.IsValid(6));

            Assert.Equal(new byte[] { }, array.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(1).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(2).ToArray());
            Assert.Equal(new byte[] { 9, 42 }, array.GetBytes(3).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(4).ToArray());
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, array.GetBytes(5).ToArray());
            Assert.Equal(new byte[] { 8, 7 }, array.GetBytes(6).ToArray());
        }

        [Fact]
        public void BinaryBuilder_Should_AppendArraySlice()
        {
            var builder = new BinaryArrayBuilder();

            var a0b = new BinaryArrayBuilder();
            a0b.AppendValues(new byte[][] {
                new byte[] {  },
                null,
                new byte[] { 9, 42 },
                new byte[] { 1, 2, 32, 4 }
            });
            var a0 = a0b.Build();

            var a1b = new BinaryArrayBuilder();
            a1b.AppendValues(new byte[][] {
                null,
                new byte[] { 1, 2, 3, 4 },
                new byte[] { 8, 7 },
                null
            });
            var a1 = a1b.Build();

            builder.AppendNull();
            builder.AppendArray(a0.Slice(2, 1));
            builder.AppendArray(a1.Slice(1, 3));

            var array = builder.Build();

            Assert.Equal(5, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.False(array.IsValid(0));
            Assert.True(array.IsValid(1));
            Assert.True(array.IsValid(2));
            Assert.True(array.IsValid(3));
            Assert.False(array.IsValid(4));

            Assert.Equal(new byte[] { }, array.GetBytes(0).ToArray());
            Assert.Equal(new byte[] { 9, 42 }, array.GetBytes(1).ToArray());
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, array.GetBytes(2).ToArray());
            Assert.Equal(new byte[] { 8, 7 }, array.GetBytes(3).ToArray());
            Assert.Equal(new byte[] { }, array.GetBytes(4).ToArray());
        }
    }

    public class StringArrayBuilderTests
    {
        [Fact]
        public void StringBuilder_Should_AppendStrings()
        {
            var builder = new StringArrayBuilder();
            string[] values = new string[] { "abc123", "", null, "965" };
            builder.AppendValue("0");
            builder.AppendValues(values);
            builder.AppendNull();
            var array = builder.Build();

            Assert.Equal(6, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.False(array.IsValid(3));
            Assert.True(array.IsValid(2));

            Assert.Equal("0", array.GetString(0));
            Assert.Equal("abc123", array.GetString(1));
            Assert.Equal("", array.GetString(2));
            Assert.Null(array.GetString(3));
            Assert.Null(array.GetString(5));
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
        public void StringBuilder_Should_AppendArrowArray()
        {
            var builder = new StringArrayBuilder();
            string[] values = new string[] { "0", "abc123", "", null, "965" };

            var arrayBuilder = new StringArrayBuilder(values.Length);
            arrayBuilder.AppendValues(values);

            builder.AppendNull();
            builder.AppendArray(arrayBuilder.Build());

            var array = builder.Build();

            Assert.Equal(6, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.False(array.IsValid(0));
            Assert.True(array.IsValid(2));

            Assert.Equal("0", array.GetString(1));
            Assert.Equal("abc123", array.GetString(2));
            Assert.Equal("", array.GetString(3));
            Assert.Null(array.GetString(4));
        }

        [Fact]
        public void StringBuilder_Should_AppendArrowArraySlice()
        {
            var builder = new StringArrayBuilder();
            string[] values = new string[] { "0", "abc123", "", null, "965" };

            var arrayBuilder = new StringArrayBuilder(values.Length);
            arrayBuilder.AppendValues(values);

            builder.AppendNull();
            builder.AppendArray(arrayBuilder.Build().Slice(1, 3));

            var array = builder.Build();

            Assert.Equal(4, array.Length);
            Assert.Equal(2, array.NullCount);

            Assert.False(array.IsValid(0));
            Assert.True(array.IsValid(2));

            Assert.Equal("abc123", array.GetString(1));
            Assert.Equal("", array.GetString(2));
            Assert.Null(array.GetString(3));
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

    public class FixedBinaryArrayBuilderTests
    {
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

        [Fact]
        public void StructArrayBuilder_Should_BuildRecordBatch()
        {
            var builder = new StructArrayBuilder<TestStruct>();
            var scalar = new StructScalar(builder.DataType, new IScalar[]
            {
                new StringScalar("abc"), new Int32Scalar(123)
            });

            builder.AppendNull();
            builder.AppendValue(scalar);
            builder.AppendNull();

            var batch = builder.BuildRecordBatch();
            var names = batch.Column(0) as StringArray;
            var int32s = batch.Column(1) as Int32Array;

            Assert.Equal(3, batch.Length);

            Assert.Equal("abc", names.GetString(1));
            Assert.Equal(123, int32s.GetValue(1));
        }

        [Fact]
        public async Task StructArrayBuilder_Should_BuildRecordBatchAsync()
        {
            var builder = new StructArrayBuilder<TestStruct>();
            var scalar = new StructScalar(builder.DataType, new IScalar[]
            {
                new StringScalar("abc"), new Int32Scalar(123)
            });

            builder.AppendNull();
            builder.AppendValue(scalar);
            builder.AppendNull();

            var batch = await builder.BuildRecordBatchAsync();
            var names = batch.Column(0) as StringArray;
            var int32s = batch.Column(1) as Int32Array;

            Assert.Equal(3, batch.Length);

            Assert.Equal("abc", names.GetString(1));
            Assert.Equal(123, int32s.GetValue(1));
        }

        private static RecordBatch GetRecordBatch()
        {
            var builder = new StructArrayBuilder<TestStruct>();
            var scalar = new StructScalar(builder.DataType, new IScalar[]
            {
                new StringScalar("abc"), new Int32Scalar(123)
            });

            builder.AppendNull();
            builder.AppendValue(scalar);
            builder.AppendNull();

            return builder.BuildRecordBatch();
        }

        [Fact]
        public void StructArrayBuilder_Should_AppendRecordBatch()
        {
            var builder = new StructArrayBuilder<TestStruct>();

            builder.AppendNull();
            builder.AppendBatch(GetRecordBatch());
            builder.AppendNull();

            var batch = builder.BuildRecordBatch();
            var names = batch.Column(0) as StringArray;
            var int32s = batch.Column(1) as Int32Array;

            Assert.Equal(5, batch.Length);

            Assert.Equal("abc", names.GetString(2));
            Assert.Equal(123, int32s.GetValue(2));
        }

        [Fact]
        public async Task StructArrayBuilder_Should_AppendRecordBatchAsync()
        {
            var builder = new StructArrayBuilder<TestStruct>();

            builder.AppendNull();
            builder.AppendBatch(GetRecordBatch());
            builder.AppendNull();

            var batch = await builder.BuildRecordBatchAsync();
            var names = batch.Column(0) as StringArray;
            var int32s = batch.Column(1) as Int32Array;

            Assert.Equal(5, batch.Length);

            Assert.Equal("abc", names.GetString(2));
            Assert.Equal(123, int32s.GetValue(2));
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
