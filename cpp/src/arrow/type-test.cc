// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Unit tests for DataType (and subclasses), Field, and Schema

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/type.h"

using std::shared_ptr;
using std::vector;

namespace arrow {

TEST(TestField, Basics) {
  Field f0("f0", int32());
  Field f0_nn("f0", int32(), false);

  ASSERT_EQ(f0.name, "f0");
  ASSERT_EQ(f0.type->ToString(), int32()->ToString());

  ASSERT_TRUE(f0.nullable);
  ASSERT_FALSE(f0_nn.nullable);
}

TEST(TestField, Equals) {
  Field f0("f0", int32());
  Field f0_nn("f0", int32(), false);
  Field f0_other("f0", int32());

  ASSERT_TRUE(f0.Equals(f0_other));
  ASSERT_FALSE(f0.Equals(f0_nn));
}

class TestSchema : public ::testing::Test {
 public:
  void SetUp() {}
};

TEST_F(TestSchema, Basics) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f1_optional = field("f1", uint8());

  auto f2 = field("f2", utf8());

  vector<shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = std::make_shared<Schema>(fields);

  ASSERT_EQ(3, schema->num_fields());
  ASSERT_TRUE(f0->Equals(schema->field(0)));
  ASSERT_TRUE(f1->Equals(schema->field(1)));
  ASSERT_TRUE(f2->Equals(schema->field(2)));

  auto schema2 = std::make_shared<Schema>(fields);

  vector<shared_ptr<Field>> fields3 = {f0, f1_optional, f2};
  auto schema3 = std::make_shared<Schema>(fields3);
  ASSERT_TRUE(schema->Equals(*schema2));
  ASSERT_FALSE(schema->Equals(*schema3));
}

TEST_F(TestSchema, ToString) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  vector<shared_ptr<Field>> fields = {f0, f1, f2, f3};
  auto schema = std::make_shared<Schema>(fields);

  std::string result = schema->ToString();
  std::string expected = R"(f0: int32
f1: uint8 not null
f2: string
f3: list<item: int16>)";

  ASSERT_EQ(expected, result);
}

TEST_F(TestSchema, GetFieldByName) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8(), false);
  auto f2 = field("f2", utf8());
  auto f3 = field("f3", list(int16()));

  vector<shared_ptr<Field>> fields = {f0, f1, f2, f3};
  auto schema = std::make_shared<Schema>(fields);

  std::shared_ptr<Field> result;

  result = schema->GetFieldByName("f1");
  ASSERT_TRUE(f1->Equals(result));

  result = schema->GetFieldByName("f3");
  ASSERT_TRUE(f3->Equals(result));

  result = schema->GetFieldByName("not-found");
  ASSERT_TRUE(result == nullptr);
}

#define PRIMITIVE_TEST(KLASS, ENUM, NAME)        \
  TEST(TypesTest, TestPrimitive_##ENUM) {        \
    KLASS tp;                                    \
                                                 \
    ASSERT_EQ(tp.type, Type::ENUM);              \
    ASSERT_EQ(tp.ToString(), std::string(NAME)); \
  }

PRIMITIVE_TEST(Int8Type, INT8, "int8");
PRIMITIVE_TEST(Int16Type, INT16, "int16");
PRIMITIVE_TEST(Int32Type, INT32, "int32");
PRIMITIVE_TEST(Int64Type, INT64, "int64");
PRIMITIVE_TEST(UInt8Type, UINT8, "uint8");
PRIMITIVE_TEST(UInt16Type, UINT16, "uint16");
PRIMITIVE_TEST(UInt32Type, UINT32, "uint32");
PRIMITIVE_TEST(UInt64Type, UINT64, "uint64");

PRIMITIVE_TEST(FloatType, FLOAT, "float");
PRIMITIVE_TEST(DoubleType, DOUBLE, "double");

PRIMITIVE_TEST(BooleanType, BOOL, "bool");

TEST(TestBinaryType, ToString) {
  BinaryType t1;
  BinaryType e1;
  StringType t2;
  EXPECT_TRUE(t1.Equals(e1));
  EXPECT_FALSE(t1.Equals(t2));
  ASSERT_EQ(t1.type, Type::BINARY);
  ASSERT_EQ(t1.ToString(), std::string("binary"));
}

TEST(TestStringType, ToString) {
  StringType str;
  ASSERT_EQ(str.type, Type::STRING);
  ASSERT_EQ(str.ToString(), std::string("string"));
}

TEST(TestFixedSizeBinaryType, ToString) {
  auto t = fixed_size_binary(10);
  ASSERT_EQ(t->type, Type::FIXED_SIZE_BINARY);
  ASSERT_EQ("fixed_size_binary[10]", t->ToString());
}

TEST(TestFixedSizeBinaryType, Equals) {
  auto t1 = fixed_size_binary(10);
  auto t2 = fixed_size_binary(10);
  auto t3 = fixed_size_binary(3);

  ASSERT_TRUE(t1->Equals(t1));
  ASSERT_TRUE(t1->Equals(t2));
  ASSERT_FALSE(t1->Equals(t3));
}

TEST(TestListType, Basics) {
  std::shared_ptr<DataType> vt = std::make_shared<UInt8Type>();

  ListType list_type(vt);
  ASSERT_EQ(list_type.type, Type::LIST);

  ASSERT_EQ("list", list_type.name());
  ASSERT_EQ("list<item: uint8>", list_type.ToString());

  ASSERT_EQ(list_type.value_type()->type, vt->type);
  ASSERT_EQ(list_type.value_type()->type, vt->type);

  std::shared_ptr<DataType> st = std::make_shared<StringType>();
  std::shared_ptr<DataType> lt = std::make_shared<ListType>(st);
  ASSERT_EQ("list<item: string>", lt->ToString());

  ListType lt2(lt);
  ASSERT_EQ("list<item: list<item: string>>", lt2.ToString());
}

TEST(TestDateTypes, Attrs) {
  auto t1 = date32();
  auto t2 = date64();

  ASSERT_EQ("date32[day]", t1->ToString());
  ASSERT_EQ("date64[ms]", t2->ToString());

  ASSERT_EQ(32, static_cast<const FixedWidthType&>(*t1).bit_width());
  ASSERT_EQ(64, static_cast<const FixedWidthType&>(*t2).bit_width());
}

TEST(TestTimeType, Equals) {
  Time32Type t0;
  Time32Type t1(TimeUnit::SECOND);
  Time32Type t2(TimeUnit::MILLI);
  Time64Type t3(TimeUnit::MICRO);
  Time64Type t4(TimeUnit::NANO);
  Time64Type t5(TimeUnit::MICRO);

  ASSERT_EQ(32, t0.bit_width());
  ASSERT_EQ(64, t3.bit_width());

  ASSERT_TRUE(t0.Equals(t2));
  ASSERT_TRUE(t1.Equals(t1));
  ASSERT_FALSE(t1.Equals(t3));
  ASSERT_FALSE(t3.Equals(t4));
  ASSERT_TRUE(t3.Equals(t5));
}

TEST(TestTimeType, ToString) {
  auto t1 = time32(TimeUnit::MILLI);
  auto t2 = time64(TimeUnit::NANO);
  auto t3 = time32(TimeUnit::SECOND);
  auto t4 = time64(TimeUnit::MICRO);

  ASSERT_EQ("time32[ms]", t1->ToString());
  ASSERT_EQ("time64[ns]", t2->ToString());
  ASSERT_EQ("time32[s]", t3->ToString());
  ASSERT_EQ("time64[us]", t4->ToString());
}

TEST(TestTimestampType, Equals) {
  TimestampType t1;
  TimestampType t2;
  TimestampType t3(TimeUnit::NANO);
  TimestampType t4(TimeUnit::NANO);

  ASSERT_TRUE(t1.Equals(t2));
  ASSERT_FALSE(t1.Equals(t3));
  ASSERT_TRUE(t3.Equals(t4));
}

TEST(TestTimestampType, ToString) {
  auto t1 = timestamp(TimeUnit::MILLI);
  auto t2 = timestamp(TimeUnit::NANO, "US/Eastern");
  auto t3 = timestamp(TimeUnit::SECOND);
  auto t4 = timestamp(TimeUnit::MICRO);

  ASSERT_EQ("timestamp[ms]", t1->ToString());
  ASSERT_EQ("timestamp[ns, tz=US/Eastern]", t2->ToString());
  ASSERT_EQ("timestamp[s]", t3->ToString());
  ASSERT_EQ("timestamp[us]", t4->ToString());
}

TEST(TestNestedType, Equals) {
  auto create_struct = [](
      std::string inner_name, std::string struct_name) -> shared_ptr<Field> {
    auto f_type = field(inner_name, int32());
    vector<shared_ptr<Field>> fields = {f_type};
    auto s_type = std::make_shared<StructType>(fields);
    return field(struct_name, s_type);
  };

  auto create_union = [](
      std::string inner_name, std::string union_name) -> shared_ptr<Field> {
    auto f_type = field(inner_name, int32());
    vector<shared_ptr<Field>> fields = {f_type};
    vector<uint8_t> codes = {Type::INT32};
    auto u_type = std::make_shared<UnionType>(fields, codes, UnionMode::SPARSE);
    return field(union_name, u_type);
  };

  auto s0 = create_struct("f0", "s0");
  auto s0_other = create_struct("f0", "s0");
  auto s0_bad = create_struct("f1", "s0");
  auto s1 = create_struct("f1", "s1");

  ASSERT_TRUE(s0->Equals(s0_other));
  ASSERT_FALSE(s0->Equals(s1));
  ASSERT_FALSE(s0->Equals(s0_bad));

  auto u0 = create_union("f0", "u0");
  auto u0_other = create_union("f0", "u0");
  auto u0_bad = create_union("f1", "u0");
  auto u1 = create_union("f1", "u1");

  ASSERT_TRUE(u0->Equals(u0_other));
  ASSERT_FALSE(u0->Equals(u1));
  ASSERT_FALSE(u0->Equals(u0_bad));
}

TEST(TestStructType, Basics) {
  auto f0_type = int32();
  auto f0 = field("f0", f0_type);

  auto f1_type = utf8();
  auto f1 = field("f1", f1_type);

  auto f2_type = uint8();
  auto f2 = field("f2", f2_type);

  vector<std::shared_ptr<Field>> fields = {f0, f1, f2};

  StructType struct_type(fields);

  ASSERT_TRUE(struct_type.child(0)->Equals(f0));
  ASSERT_TRUE(struct_type.child(1)->Equals(f1));
  ASSERT_TRUE(struct_type.child(2)->Equals(f2));

  ASSERT_EQ(struct_type.ToString(), "struct<f0: int32, f1: string, f2: uint8>");

  // TODO(wesm): out of bounds for field(...)
}

}  // namespace arrow
