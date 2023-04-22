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
using Apache.Arrow.Types;
using Xunit;
using Xunit.Sdk;

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

            FieldComparer.Compare(f0_nullable, f0_other);
            Assert.Throws<EqualException>(() => FieldComparer.Compare(f0_nullable, f0_nonnullable));
            Assert.Throws<EqualException>(() => FieldComparer.Compare(f0_nullable, f0_with_meta));
        }

        [Fact]
        public void TestMetadataConstruction()
        {
            var metadata = new Dictionary<string, string> { { "foo", "bar" }, { "bizz", "buzz" } };
            var metadata1 = new Dictionary<string, string>(metadata);
            Field f0_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Metadata(metadata).Build();
            Field f1_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Metadata(metadata1).Build();
            Assert.True(metadata.Keys.SequenceEqual(f0_nullable.Metadata.Keys) && metadata.Values.SequenceEqual(f0_nullable.Metadata.Values));
            FieldComparer.Compare(f0_nullable, f1_nullable);
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
            FieldComparer.Compare(structFields.ElementAt(0), f0_nullable);
            FieldComparer.Compare(structFields.ElementAt(1), f1_nullable);
            FieldComparer.Compare(structFields.ElementAt(2), f2_nullable);
        }

        [Fact]
        public void TestStructGetFieldByName()
        {

            Field f0_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            Field f1_nullable = new Field.Builder().Name("f1").DataType(StringType.Default).Build();
            Field f2_nullable = new Field.Builder().Name("f2").DataType(UInt8Type.Default).Build();

            List<Field> fields = new List<Field>() { f0_nullable, f1_nullable, f2_nullable };
            StructType struct_type = new StructType(fields);

            FieldComparer.Compare(struct_type.GetFieldByName("f0"), f0_nullable);
            FieldComparer.Compare(struct_type.GetFieldByName("f1"), f1_nullable);
            FieldComparer.Compare(struct_type.GetFieldByName("f2"), f2_nullable);
            Assert.True(struct_type.GetFieldByName("not_found") == null);
        }

        [Fact]
        public void TestStructGetFieldIndex()
        {
            Field f0_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            Field f1_nullable = new Field.Builder().Name("f1").DataType(StringType.Default).Build();
            Field f2_nullable = new Field.Builder().Name("f2").DataType(UInt8Type.Default).Build();

            StructType struct_type = new StructType(new[] { f0_nullable, f1_nullable, f2_nullable });

            Assert.Equal(0, struct_type.GetFieldIndex("f0"));
            Assert.Equal(1, struct_type.GetFieldIndex("f1"));
            Assert.Equal(2, struct_type.GetFieldIndex("F2", StringComparer.OrdinalIgnoreCase));
            Assert.Equal(-1, struct_type.GetFieldIndex("F2"));
            Assert.Equal(-1, struct_type.GetFieldIndex("F2", StringComparer.Ordinal));
            Assert.Equal(-1, struct_type.GetFieldIndex("not_found"));
        }

        [Fact]
        public void TestListTypeConstructor()
        {
            var stringField = new Field.Builder().Name("item").DataType(StringType.Default).Build();
            var stringType1 = new ListType(stringField);
            var stringType2 = new ListType(StringType.Default);

            FieldComparer.Compare(stringType1.ValueField, stringType2.ValueField);
            Assert.Equal(stringType1.ValueDataType.TypeId, stringType2.ValueDataType.TypeId);
        }

        // Todo: StructType::GetFieldIndexDuplicate test

        [Fact]
        public void TestPrimitiveTypesEquality()
        {
            int i = 0;
            foreach (IArrowType compareFrom in GetPrimitiveTypes())
            {
                i++;
                int j = 0;
                foreach (IArrowType compareTo in GetPrimitiveTypes())
                {
                    j++;
                    if (i == j)
                    {
                        Assert.True(compareFrom.Equals(compareTo), $"{compareFrom} and {compareTo} are not .Equals while they should be");
                        Assert.True(compareFrom.GetHashCode() == compareTo.GetHashCode(), $"{compareFrom} and {compareTo} do not have the same hash codes while they should have");
                    }
                    else
                    {
                        Assert.False(compareFrom.Equals(compareTo), $"{compareFrom} and {compareTo} are .Equals while they should not be");
                        Assert.False(compareFrom.GetHashCode() == compareTo.GetHashCode(), $"{compareFrom} and {compareTo} have the same hash codes while they should not have");
                    }
                        
                }
            }
        }

        [Fact]
        public void TestNestedTypesEquality()
        {
            int i = 0;
            foreach (IArrowType compareFrom in GetNestedTypes())
            {
                i++;
                int j = 0;
                foreach (IArrowType compareTo in GetNestedTypes())
                {
                    j++;
                    if (i == j)
                    {
                        Assert.True(compareFrom.Equals(compareTo), $"{compareFrom} and {compareTo} are not .Equals while they should be");
                        Assert.True(compareFrom.GetHashCode() == compareTo.GetHashCode(), $"{compareFrom} and {compareTo} do not have the same hash codes while they should have");
                    }
                    else
                    {
                        Assert.False(compareFrom.Equals(compareTo), $"{compareFrom} and {compareTo} are .Equals while they should not be");
                        Assert.False(compareFrom.GetHashCode() == compareTo.GetHashCode(), $"{compareFrom} and {compareTo} have the same hash codes while they should not have");
                    }

                }
            }
        }

        private IEnumerable<NestedType> GetNestedTypes()
        {
            foreach (IArrowType p in GetPrimitiveTypes())
            {
                yield return new ListType(p);
            }

            yield return new StructType(new Field[]
            {
                new Field("str", new StringType(), true),
            });

            yield return new StructType(new Field[]
            {
                new Field("str", new StringType(), true),
                new Field("int", new Int32Type(), true),
            });

            yield return new StructType(new Field[]
            {
                new Field("str", new StringType(), false),
                new Field("int", new Int32Type(), true),
            });

            yield return new StructType(new Field[]
            {
                new Field("long", new Int64Type(), true),
                new Field("int", new Int32Type(), true),
            });

            yield return new StructType(new Field[]
            {
                new Field("int", new Int32Type(), true),
                new Field("long", new Int64Type(), true),
            });
        }

        private IEnumerable<IArrowType> GetPrimitiveTypes()
        {
            yield return new FixedSizeBinaryType(4);
            yield return new FixedSizeBinaryType(8);

            yield return new BinaryType();
            yield return new StringType();
            yield return new BooleanType();

            yield return new Int8Type();
            yield return new Int16Type();
            yield return new Int32Type();
            yield return new Int64Type();
            yield return new UInt8Type();
            yield return new UInt16Type();
            yield return new UInt32Type();
            yield return new UInt64Type();

            yield return new Decimal128Type(15, 5);
            yield return new Decimal128Type(23, 12);

            yield return new Decimal256Type(15, 5);
            yield return new Decimal256Type(23, 12);

            yield return new FloatType();
#if NET5_0_OR_GREATER
            yield return new HalfFloatType();
#endif
            yield return new DoubleType();

            foreach (DateType d in GetDateTypes())
                yield return d;

            foreach (TimeType d in GetTimeTypes())
                yield return d;

            foreach (TimestampType d in GetTimestampTypes())
                yield return d;
        }

        private IEnumerable<DateType> GetDateTypes()
        {
            yield return new Date32Type();
            yield return new Date64Type();
        }

        private IEnumerable<TimeType> GetTimeTypes()
        {
            foreach (TimeUnit unit in Enum.GetValues(typeof(TimeUnit)))
            {
                yield return new Time32Type(unit);
                yield return new Time64Type(unit);
            }
        }

        private IEnumerable<TimestampType> GetTimestampTypes()
        {
            foreach (TimeUnit unit in Enum.GetValues(typeof(TimeUnit)))
            {
                yield return new TimestampType(unit, (string)null);
                yield return new TimestampType(unit, "+00:00");
                yield return new TimestampType(unit, "+12:34");
            }
        }
    }
}
