using System;
using System.Collections.Generic;
using System.Text;
using Xunit;
using Apache.Arrow.Types;

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
            Field.Builder fb = new Field.Builder();
            Field f0_nullable = fb.Name("f0").DataType(Int32Type.Default).Build();
            Field f0_nonnullable = fb.Name("f0").DataType(Int32Type.Default).Nullable(false).Build();
            Field f0_other = fb.Name("f0").DataType(Int32Type.Default).Build();
            Field f0_with_meta = fb.Name("f0").DataType(Int32Type.Default).Nullable(true).Metadata("a", "1").Metadata("b", "2").Build();

            Assert.True(f0_nullable.Equals(f0_other));
            Assert.False(f0_nullable.Equals(f0_nonnullable));
            Assert.False(f0_nullable.Equals(f0_with_meta));
            Assert.True(f0_nullable.Equals(f0_with_meta, false));
        }
    }
//TEST(TestField, Equals) {
//  auto meta = key_value_metadata({{"a", "1"}, {"b", "2"}});

//  Field f0("f0", int32());
//  Field f0_nn("f0", int32(), false);
//  Field f0_other("f0", int32());
//  Field f0_with_meta("f0", int32(), true, meta);

//  ASSERT_TRUE(f0.Equals(f0_other));
//  ASSERT_FALSE(f0.Equals(f0_nn));
//  ASSERT_FALSE(f0.Equals(f0_with_meta));
//  ASSERT_TRUE(f0.Equals(f0_with_meta, false));
//}
    
}
