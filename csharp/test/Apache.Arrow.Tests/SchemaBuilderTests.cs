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
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class SchemaBuilderTests
    {
        public class Build
        {
            [Fact]
            public void FieldsAreNullableByDefault()
            {
                var b = new Schema.Builder();
                
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Build();

                Assert.True(schema.Fields["f0"].IsNullable);
            }

            [Fact]
            public void FieldsHaveNullTypeByDefault()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0"))
                    .Build();

                Assert.True(schema.Fields["f0"].DataType.GetType() == typeof(NullType));
            }

            [Fact]
            public void FieldNameIsRequired()
            {
                Assert.Throws<ArgumentNullException>(() =>
                {
                    var schema = new Schema.Builder()
                        .Field(f => f.DataType(Int32Type.Default))
                        .Build();
                });
            }

            [Fact]
            public void GetFieldIndex()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Field(f => f.Name("f1").DataType(Int8Type.Default))
                    .Build();
                Assert.True(schema.GetFieldIndex("f0") == 0 && schema.GetFieldIndex("f1") == 1);
            }


            [Fact]
            public void GetFieldByName()
            {
                Field f0 = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
                Field f1 = new Field.Builder().Name("f1").DataType(Int8Type.Default).Build();

                var schema = new Schema.Builder()
                    .Field(f0)
                    .Field(f1)
                    .Build();
                Assert.True(schema.GetFieldByName("f0") == f0 && schema.GetFieldByName("f1") == f1);
            }

            [Fact]
            public void MetadataConstruction()
            {

                var metadata0 = new Dictionary<string, string> { { "foo", "bar" }, { "bizz", "buzz" } };
                var metadata1 = new Dictionary<string, string> { { "foo", "bar" } };
                var metadata0Copy = new Dictionary<string, string>(metadata0);
                var metadata1Copy = new Dictionary<string, string>(metadata1);
                Field f0 = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
                Field f1 = new Field.Builder().Name("f1").DataType(UInt8Type.Default).Nullable(false).Build();
                Field f2 = new Field.Builder().Name("f2").DataType(StringType.Default).Build();
                Field f3 = new Field.Builder().Name("f2").DataType(StringType.Default).Metadata(metadata1Copy).Build();

                var schema0 = new Schema.Builder()
                    .Field(f0)
                    .Field(f1)
                    .Field(f2)
                    .Metadata(metadata0)
                    .Build();
                var schema1 = new Schema.Builder()
                    .Field(f0)
                    .Field(f1)
                    .Field(f2)
                    .Metadata(metadata1)
                    .Build();
                var schema2 = new Schema.Builder()
                    .Field(f0)
                    .Field(f1)
                    .Field(f2)
                    .Metadata(metadata0Copy)
                    .Build();
                var schema3 = new Schema.Builder()
                    .Field(f0)
                    .Field(f1)
                    .Field(f3)
                    .Metadata(metadata0Copy)
                    .Build();

                Assert.True(metadata0.Keys.SequenceEqual(schema0.Metadata.Keys) && metadata0.Values.SequenceEqual(schema0.Metadata.Values));
                Assert.True(metadata1.Keys.SequenceEqual(schema1.Metadata.Keys) && metadata1.Values.SequenceEqual(schema1.Metadata.Values));
                Assert.True(metadata0.Keys.SequenceEqual(schema2.Metadata.Keys) && metadata0.Values.SequenceEqual(schema2.Metadata.Values));
                Assert.True(SchemaComparer.Equals(schema0, schema2));
                Assert.False(SchemaComparer.Equals(schema0, schema1));
                Assert.False(SchemaComparer.Equals(schema2, schema1));
                Assert.False(SchemaComparer.Equals(schema2, schema3));
            }

            [Theory]
            [MemberData(nameof(SampleSchema1))]
            public void FieldsHaveExpectedValues(string name, IArrowType type, bool nullable)
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name(name).DataType(type).Nullable(nullable))
                    .Build();

                var field = schema.Fields[name];

                Assert.Equal(name, field.Name);
                Assert.Equal(type.Name, field.DataType.Name);
                Assert.Equal(nullable, field.IsNullable);
            }

            public static IEnumerable<object[]> SampleSchema1()
            {
                yield return new object[] {"f0", Int32Type.Default, true};
                yield return new object[] {"f1", DoubleType.Default, true};
                yield return new object[] {"f2", Int64Type.Default, false};
            }
        }
    }
}
