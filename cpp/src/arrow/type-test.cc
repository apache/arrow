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

#include "arrow/schema.h"
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
  ASSERT_TRUE(schema->Equals(schema2));
  ASSERT_FALSE(schema->Equals(schema3));

  ASSERT_TRUE(schema->Equals(*schema2.get()));
  ASSERT_FALSE(schema->Equals(*schema3.get()));
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

TEST(TestFixedWidthBinaryType, ToString) {
  auto t = fixed_width_binary(10);
  ASSERT_EQ(t->type, Type::FIXED_WIDTH_BINARY);
  ASSERT_EQ("fixed_width_binary[10]", t->ToString());
}

TEST(TestFixedWidthBinaryType, Equals) {
  auto t1 = fixed_width_binary(10);
  auto t2 = fixed_width_binary(10);
  auto t3 = fixed_width_binary(3);

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

TEST(TestDateTypes, ToString) {
  auto t1 = date32();
  auto t2 = date64();

  ASSERT_EQ("date32[day]", t1->ToString());
  ASSERT_EQ("date64[ms]", t2->ToString());
}

TEST(TestTimeType, Equals) {
  Time32Type t0;
  Time32Type t1(TimeUnit::SECOND);
  Time32Type t2(TimeUnit::MILLI);
  Time64Type t3(TimeUnit::MICRO);
  Time64Type t4(TimeUnit::NANO);
  Time64Type t5(TimeUnit::MICRO);

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

}  // namespace arrow
