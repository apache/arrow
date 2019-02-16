using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Apache.Arrow.Types;
using System.Linq;

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

            Assert.True(f0_nullable.Equals(f0_other));
            Assert.False(f0_nullable.Equals(f0_nonnullable));
            Assert.False(f0_nullable.Equals(f0_with_meta));
        }

        [Fact]
        public void TestMetadataConstruction()
        {
            var metadata = new Dictionary<string, string> { { "foo", "bar" }, { "bizz", "buzz" } };
            var metadata1 = new Dictionary<string, string>(metadata);
            Field f0_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Metadata(metadata).Build();
            Field f1_nullable = new Field.Builder().Name("f0").DataType(Int32Type.Default).Metadata(metadata1).Build();
            Assert.True(metadata.Keys.SequenceEqual(f0_nullable.Metadata.Keys) && metadata.Values.SequenceEqual(f0_nullable.Metadata.Values));
            Assert.True(f0_nullable.Equals(f1_nullable));
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
            Assert.True(structFields.ElementAt(0).Equals(f0_nullable));
            Assert.True(structFields.ElementAt(1).Equals(f1_nullable));
            Assert.True(structFields.ElementAt(2).Equals(f2_nullable));
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
            Assert.True(struct_type.GetFieldByName("f0").Equals(f0_nullable));
            Assert.True(struct_type.GetFieldByName("f1").Equals(f1_nullable));
            Assert.True(struct_type.GetFieldByName("f2").Equals(f2_nullable));
            Assert.True(struct_type.GetFieldByName("not_found") == null);
        }

        // Todo: StructType::GetFieldIndexDuplicate test


    }
}
