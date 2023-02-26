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
using Xunit.Sdk;

namespace Apache.Arrow.Tests
{
    public class SchemaBuilderTests
    {
        public class Build
        {
            [Fact]
            public void FieldsAreNullableByDefault()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Build();

                Assert.True(schema.FieldsList[0].IsNullable);
            }

            [Fact]
            public void FieldsHaveNullTypeByDefault()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0"))
                    .Build();

                Assert.Equal(typeof(NullType), schema.FieldsList[0].DataType.GetType());
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
            public void FieldNamesAreCaseSensitive()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Field(f => f.Name("F0").DataType(Int8Type.Default))
                    .Build();

                Assert.Equal(2, schema.FieldsList.Count);
            }

            [Fact]
            public void FieldNamesCanBeTheSame()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Field(f => f.Name("f0").DataType(Int8Type.Default))
                    .Build();

                Assert.Equal(2, schema.FieldsList.Count);
            }

            [Fact]
            public void GetFieldIndex()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Field(f => f.Name("f1").DataType(Int8Type.Default))
                    .Build();

                Assert.Equal(0, schema.GetFieldIndex("f0"));
                Assert.Equal(1, schema.GetFieldIndex("f1"));
            }

            [Fact]
            public void GetFieldIndexIsCaseSensitive()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Field(f => f.Name("F0").DataType(Int8Type.Default))
                    .Build();

                Assert.Equal(0, schema.GetFieldIndex("f0"));
                Assert.Equal(1, schema.GetFieldIndex("F0"));
            }

            [Fact]
            public void GetFieldIndexGetsFirstMatch()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Field(f => f.Name("f0").DataType(Int8Type.Default))
                    .Field(f => f.Name("f1").DataType(Int32Type.Default))
                    .Field(f => f.Name("f1").DataType(Int8Type.Default))
                    .Build();

                Assert.Equal(0, schema.GetFieldIndex("f0"));
                Assert.Equal(2, schema.GetFieldIndex("f1"));
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

                Assert.Equal(f0, schema.GetFieldByName("f0"));
                Assert.Equal(f1, schema.GetFieldByName("f1"));
            }

            [Fact]
            public void GetFieldByNameIsCaseSensitive()
            {
                var f0 = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
                var f0Uppercase = new Field.Builder().Name("F0").DataType(Int8Type.Default).Build();

                var schema = new Schema.Builder()
                    .Field(f0)
                    .Field(f0Uppercase)
                    .Build();

                Assert.Equal(f0, schema.GetFieldByName("f0"));
                Assert.Equal(f0Uppercase, schema.GetFieldByName("F0"));
            }

            [Fact]
            public void GetFieldByNameGetsFirstMatch()
            {
                var f0First = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
                var f0Second = new Field.Builder().Name("f0").DataType(Int8Type.Default).Build();
                var f1First = new Field.Builder().Name("f1").DataType(Int32Type.Default).Build();
                var f1Second = new Field.Builder().Name("f1").DataType(Int8Type.Default).Build();

                var schema = new Schema.Builder()
                    .Field(f0First)
                    .Field(f0Second)
                    .Field(f1First)
                    .Field(f1Second)
                    .Build();

                Assert.Equal(f0First, schema.GetFieldByName("f0"));
                Assert.Equal(f1First, schema.GetFieldByName("f1"));
            }

            [Fact]
            public void GetFieldByNameReturnsNullWhenSchemaIsEmpty()
            {
                var schema = new Schema.Builder()
                    .Build();

                Assert.Null(schema.GetFieldByName("f0"));
            }

            [Fact]
            public void GetFieldByNameReturnsNullWhenFieldDoesNotExist()
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name("f0").DataType(Int32Type.Default))
                    .Build();

                Assert.Null(schema.GetFieldByName("f1"));
            }

            [Fact]
            public void SquareBracketOperatorWithStringReturnsGetFieldByName()
            {
                Field f0 = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
                Field f1 = new Field.Builder().Name("f1").DataType(Int8Type.Default).Build();

                var schema = new Schema.Builder()
                    .Field(f0)
                    .Field(f1)
                    .Build();

                Assert.Equal(schema.GetFieldByName("f0"), schema["f0"]);
                Assert.Equal(schema.GetFieldByName("f1"), schema["f1"]);
            }

            [Fact]
            public void SquareBracketOperatorWithIntReturnsGetFieldByIndex()
            {
                Field f0 = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
                Field f1 = new Field.Builder().Name("f1").DataType(Int8Type.Default).Build();

                var schema = new Schema.Builder()
                    .Field(f0)
                    .Field(f1)
                    .Build();

                Assert.Equal(schema.GetFieldByIndex(0), schema[0]);
                Assert.Equal(schema.GetFieldByIndex(1), schema[1]);
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
                SchemaComparer.Compare(schema0, schema2);
                Assert.Throws<EqualException>(() => SchemaComparer.Compare(schema0, schema1));
                Assert.Throws<EqualException>(() => SchemaComparer.Compare(schema2, schema1));
                Assert.Throws<EqualException>(() => SchemaComparer.Compare(schema2, schema3));
            }

            [Theory]
            [MemberData(nameof(SampleSchema1))]
            public void FieldsHaveExpectedValues(string name, IArrowType type, bool nullable)
            {
                var schema = new Schema.Builder()
                    .Field(f => f.Name(name).DataType(type).Nullable(nullable))
                    .Build();

                var field = schema.FieldsList[0];

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
