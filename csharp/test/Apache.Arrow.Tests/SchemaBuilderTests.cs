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
