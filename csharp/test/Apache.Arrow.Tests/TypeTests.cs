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
    public class TypeTests
    {
        [Fact]
        public void Basics()
        {
            Field.Builder fb = new Field.Builder();
            Field f0_nullable = fb.Name("f0").DataType(Int32Type.Default).Build();
            Field f0_nonnullable = fb.Name("f0").DataType(Int32Type.Default).Nullable(false).Build();

            Assert.True(f0_nullable.Name == "f0");
            Assert.True(f0_nullable.DataType.Name == Int32Type.Default.Name);

            Assert.True(f0_nullable.IsNullable);
            Assert.False(f0_nonnullable.IsNullable);
        }

        [Fact]
        public void Equality()
        {
            Field f0_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            Field f0_nonnullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Nullable(false).Build();
            Field f0_other = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            Field f0_with_meta = new Field.Builder().Name("f0").DataType(Int32Type.Default).Nullable(true).Metadata("a", "1").Metadata("b", "2").Build();

            Assert.True(FieldComparer.Equals(f0_nullable, f0_other));
            Assert.False(FieldComparer.Equals(f0_nullable, f0_nonnullable));
            Assert.False(FieldComparer.Equals(f0_nullable, f0_with_meta));
        }

        [Fact]
        public void TestMetadataConstruction()
        {
            var metadata = new Dictionary<string, string> { { "foo", "bar" }, { "bizz", "buzz" } };
            var metadata1 = new Dictionary<string, string>(metadata);
            Field f0_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Metadata(metadata).Build();
            Field f1_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Metadata(metadata1).Build();
            Assert.True(metadata.Keys.SequenceEqual(f0_nullable.Metadata.Keys) && metadata.Values.SequenceEqual(f0_nullable.Metadata.Values));
            Assert.True(FieldComparer.Equals(f0_nullable, f1_nullable));
        }

        [Fact]
        public void TestStructBasics()
        {

            Field f0_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            Field f1_nullable = new Field.Builder().Name("f1").DataType(StringType.Default).Build();
            Field f2_nullable = new Field.Builder().Name("f2").DataType(UInt8Type.Default).Build();

            List<Field> fields = new List<Field>() { f0_nullable, f1_nullable, f2_nullable };
            StructType struct_type = new StructType(fields);

            var structFields = struct_type.Fields;
            Assert.True(FieldComparer.Equals(structFields.ElementAt(0), f0_nullable));
            Assert.True(FieldComparer.Equals(structFields.ElementAt(1), f1_nullable));
            Assert.True(FieldComparer.Equals(structFields.ElementAt(2), f2_nullable));
        }

        [Fact]
        public void TestStructGetFieldByName()
        {

            Field f0_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            Field f1_nullable = new Field.Builder().Name("f1").DataType(StringType.Default).Build();
            Field f2_nullable = new Field.Builder().Name("f2").DataType(UInt8Type.Default).Build();

            List<Field> fields = new List<Field>() { f0_nullable, f1_nullable, f2_nullable };
            StructType struct_type = new StructType(fields);

            var structFields = struct_type.Fields;
            Assert.True(FieldComparer.Equals(struct_type.GetFieldByName("f0"), f0_nullable));
            Assert.True(FieldComparer.Equals(struct_type.GetFieldByName("f1"), f1_nullable));
            Assert.True(FieldComparer.Equals(struct_type.GetFieldByName("f2"), f2_nullable));
            Assert.True(struct_type.GetFieldByName("not_found") == null);
        }

        // Todo: StructType::GetFieldIndexDuplicate test


    }
}
