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

#include <string>

#include <gtest/gtest.h>

#include "arrow/array.h"
// TODO ipc shouldn't be included here
#include "arrow/ipc/test-common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

TEST(TestUnionArray, TestSliceEquals) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(ipc::test::MakeUnion(&batch));

  auto CheckUnion = [](std::shared_ptr<Array> array) {
    const int64_t size = array->length();
    std::shared_ptr<Array> slice, slice2;
    slice = array->Slice(2);
    ASSERT_EQ(size - 2, slice->length());

    slice2 = array->Slice(2);
    ASSERT_EQ(size - 2, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(2, array->length(), 0, slice));

    // Chained slices
    slice2 = array->Slice(1)->Slice(1);
    ASSERT_TRUE(slice->Equals(slice2));

    slice = array->Slice(1, 5);
    slice2 = array->Slice(1, 5);
    ASSERT_EQ(5, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(1, 6, 0, slice));

    AssertZeroPadded(*array);
    TestInitialized(*array);
  };

  CheckUnion(batch->column(1));
  CheckUnion(batch->column(2));
}

// -------------------------------------------------------------------------
// Tests for MakeDense and MakeSparse

class TestUnionArrayFactories : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    ArrayFromVector<Int8Type>({0, 1, 2, 0, 1, 3, 2, 0, 2, 1}, &type_ids_);
  }

  void CheckUnionArray(const UnionArray& array, UnionMode::type mode,
                       const std::vector<std::string>& field_names,
                       const std::vector<uint8_t>& type_codes) {
    ASSERT_EQ(mode, array.mode());
    CheckFieldNames(array, field_names);
    CheckTypeCodes(array, type_codes);
  }

  void CheckFieldNames(const UnionArray& array, const std::vector<std::string>& names) {
    const auto& type = checked_cast<const UnionType&>(*array.type());
    ASSERT_EQ(type.num_children(), names.size());
    for (int i = 0; i < type.num_children(); ++i) {
      ASSERT_EQ(type.child(i)->name(), names[i]);
    }
  }

  void CheckTypeCodes(const UnionArray& array, const std::vector<uint8_t>& codes) {
    const auto& type = checked_cast<const UnionType&>(*array.type());
    ASSERT_EQ(codes, type.type_codes());
  }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<Array> type_ids_;
};

TEST_F(TestUnionArrayFactories, TestMakeDense) {
  std::shared_ptr<Array> value_offsets;
  ArrayFromVector<Int32Type, int32_t>({0, 0, 0, 1, 1, 0, 1, 2, 1, 2}, &value_offsets);

  auto children = std::vector<std::shared_ptr<Array>>(4);
  ArrayFromVector<StringType, std::string>({"abc", "def", "xyz"}, &children[0]);
  ArrayFromVector<UInt8Type>({10, 20, 30}, &children[1]);
  ArrayFromVector<DoubleType>({1.618, 2.718, 3.142}, &children[2]);
  ArrayFromVector<Int8Type>({-12}, &children[3]);

  std::vector<std::string> field_names = {"str", "int1", "real", "int2"};
  std::vector<uint8_t> type_codes = {1, 2, 4, 8};

  std::shared_ptr<Array> result;

  // without field names and type codes
  ASSERT_OK(UnionArray::MakeDense(*type_ids_, *value_offsets, children, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::DENSE,
                  {"0", "1", "2", "3"}, {0, 1, 2, 3});

  // with field name
  ASSERT_RAISES(Invalid, UnionArray::MakeDense(*type_ids_, *value_offsets, children,
                                               {"one"}, &result));
  ASSERT_OK(
      UnionArray::MakeDense(*type_ids_, *value_offsets, children, field_names, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::DENSE, field_names,
                  {0, 1, 2, 3});

  // with type codes
  ASSERT_RAISES(Invalid, UnionArray::MakeDense(*type_ids_, *value_offsets, children,
                                               std::vector<uint8_t>{0}, &result));
  ASSERT_OK(
      UnionArray::MakeDense(*type_ids_, *value_offsets, children, type_codes, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::DENSE,
                  {"0", "1", "2", "3"}, type_codes);

  // with field names and type codes
  ASSERT_RAISES(Invalid, UnionArray::MakeDense(*type_ids_, *value_offsets, children,
                                               {"one"}, type_codes, &result));
  ASSERT_OK(UnionArray::MakeDense(*type_ids_, *value_offsets, children, field_names,
                                  type_codes, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::DENSE, field_names,
                  type_codes);
}

TEST_F(TestUnionArrayFactories, TestMakeSparse) {
  auto children = std::vector<std::shared_ptr<Array>>(4);
  ArrayFromVector<StringType, std::string>(
      {"abc", "", "", "def", "", "", "", "xyz", "", ""}, &children[0]);
  ArrayFromVector<UInt8Type>({0, 10, 0, 0, 20, 0, 0, 0, 0, 30}, &children[1]);
  ArrayFromVector<DoubleType>({0.0, 0.0, 1.618, 0.0, 0.0, 0.0, 2.718, 0.0, 3.142, 0.0},
                              &children[2]);
  ArrayFromVector<Int8Type>({0, 0, 0, 0, 0, -12, 0, 0, 0, 0}, &children[3]);

  std::vector<std::string> field_names = {"str", "int1", "real", "int2"};
  std::vector<uint8_t> type_codes = {1, 2, 4, 8};

  std::shared_ptr<Array> result;

  // without field names and type codes
  ASSERT_OK(UnionArray::MakeSparse(*type_ids_, children, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::SPARSE,
                  {"0", "1", "2", "3"}, {0, 1, 2, 3});

  // with field names
  ASSERT_RAISES(Invalid, UnionArray::MakeSparse(*type_ids_, children, {"one"}, &result));
  ASSERT_OK(UnionArray::MakeSparse(*type_ids_, children, field_names, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::SPARSE, field_names,
                  {0, 1, 2, 3});

  // with type codes
  ASSERT_RAISES(Invalid, UnionArray::MakeSparse(*type_ids_, children,
                                                std::vector<uint8_t>{0}, &result));
  ASSERT_OK(UnionArray::MakeSparse(*type_ids_, children, type_codes, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::SPARSE,
                  {"0", "1", "2", "3"}, type_codes);

  // with field names and type codes
  ASSERT_RAISES(Invalid, UnionArray::MakeSparse(*type_ids_, children, {"one"}, type_codes,
                                                &result));
  ASSERT_OK(
      UnionArray::MakeSparse(*type_ids_, children, field_names, type_codes, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::SPARSE, field_names,
                  type_codes);
}

template <typename B>
class UnionBuilderTest : public ::testing::Test {
 public:
  uint8_t I8 = 8, STR = 13, DBL = 7;

  virtual void AppendInt(int8_t i) {
    expected_types_vector.push_back(I8);
    ASSERT_OK(union_builder->Append(I8));
    ASSERT_OK(i8_builder->Append(i));
  }

  virtual void AppendString(const std::string& str) {
    expected_types_vector.push_back(STR);
    ASSERT_OK(union_builder->Append(STR));
    ASSERT_OK(str_builder->Append(str));
  }

  virtual void AppendDouble(double dbl) {
    expected_types_vector.push_back(DBL);
    ASSERT_OK(union_builder->Append(DBL));
    ASSERT_OK(dbl_builder->Append(dbl));
  }

  void AppendBasics() {
    AppendInt(33);
    AppendString("abc");
    AppendDouble(1.0);
    AppendDouble(-1.0);
    AppendString("");
    AppendInt(10);
    AppendString("def");
    AppendInt(-10);
    AppendDouble(0.5);
    ASSERT_OK(union_builder->Finish(&actual));
    ArrayFromVector<Int8Type, uint8_t>(expected_types_vector, &expected_types);
  }

  void AppendInferred() {
    I8 = union_builder->AppendChild(i8_builder, "i8");
    ASSERT_EQ(I8, 0);
    AppendInt(33);
    AppendInt(10);

    STR = union_builder->AppendChild(str_builder, "str");
    ASSERT_EQ(STR, 1);
    AppendString("abc");
    AppendString("");
    AppendString("def");
    AppendInt(-10);

    DBL = union_builder->AppendChild(dbl_builder, "dbl");
    ASSERT_EQ(DBL, 2);
    AppendDouble(1.0);
    AppendDouble(-1.0);
    AppendDouble(0.5);
    ASSERT_OK(union_builder->Finish(&actual));
    ArrayFromVector<Int8Type, uint8_t>(expected_types_vector, &expected_types);

    ASSERT_EQ(I8, 0);
    ASSERT_EQ(STR, 1);
    ASSERT_EQ(DBL, 2);
  }

  void AppendListOfInferred(std::shared_ptr<ListArray>* actual) {
    ListBuilder list_builder(default_memory_pool(), union_builder);

    ASSERT_OK(list_builder.Append());
    I8 = union_builder->AppendChild(i8_builder, "i8");
    ASSERT_EQ(I8, 0);
    AppendInt(10);

    ASSERT_OK(list_builder.Append());
    STR = union_builder->AppendChild(str_builder, "str");
    ASSERT_EQ(STR, 1);
    AppendString("abc");
    AppendInt(-10);

    ASSERT_OK(list_builder.Append());
    DBL = union_builder->AppendChild(dbl_builder, "dbl");
    ASSERT_EQ(DBL, 2);
    AppendDouble(0.5);

    ASSERT_OK(list_builder.Finish(actual));
    ArrayFromVector<Int8Type, uint8_t>(expected_types_vector, &expected_types);

    ASSERT_EQ(I8, 0);
    ASSERT_EQ(STR, 1);
    ASSERT_EQ(DBL, 2);
  }

  std::vector<uint8_t> expected_types_vector;
  std::shared_ptr<Array> expected_types;
  std::shared_ptr<Int8Builder> i8_builder = std::make_shared<Int8Builder>();
  std::shared_ptr<StringBuilder> str_builder = std::make_shared<StringBuilder>();
  std::shared_ptr<DoubleBuilder> dbl_builder = std::make_shared<DoubleBuilder>();
  std::shared_ptr<B> union_builder{new B(default_memory_pool())};
  std::shared_ptr<UnionArray> actual;
};

class DenseUnionBuilderTest : public UnionBuilderTest<DenseUnionBuilder> {};
class SparseUnionBuilderTest : public UnionBuilderTest<SparseUnionBuilder> {
 public:
  using Base = UnionBuilderTest<SparseUnionBuilder>;

  void AppendInt(int8_t i) override {
    Base::AppendInt(i);
    ASSERT_OK(str_builder->AppendNull());
    ASSERT_OK(dbl_builder->AppendNull());
  }

  void AppendString(const std::string& str) override {
    Base::AppendString(str);
    ASSERT_OK(i8_builder->AppendNull());
    ASSERT_OK(dbl_builder->AppendNull());
  }

  void AppendDouble(double dbl) override {
    Base::AppendDouble(dbl);
    ASSERT_OK(i8_builder->AppendNull());
    ASSERT_OK(str_builder->AppendNull());
  }
};

TEST_F(DenseUnionBuilderTest, Basics) {
  union_builder.reset(new DenseUnionBuilder(
      default_memory_pool(), {i8_builder, str_builder, dbl_builder},
      union_({field("i8", int8()), field("str", utf8()), field("dbl", float64())},
             {I8, STR, DBL}, UnionMode::DENSE)));
  AppendBasics();

  auto expected_i8 = ArrayFromJSON(int8(), "[33, 10, -10]");
  auto expected_str = ArrayFromJSON(utf8(), R"(["abc", "", "def"])");
  auto expected_dbl = ArrayFromJSON(float64(), "[1.0, -1.0, 0.5]");

  auto expected_offsets = ArrayFromJSON(int32(), "[0, 0, 0, 1, 1, 1, 2, 2, 2]");

  std::shared_ptr<Array> expected;
  ASSERT_OK(UnionArray::MakeDense(*expected_types, *expected_offsets,
                                  {expected_i8, expected_str, expected_dbl},
                                  {"i8", "str", "dbl"}, {I8, STR, DBL}, &expected));

  ASSERT_EQ(expected->type()->ToString(), actual->type()->ToString());
  ASSERT_ARRAYS_EQUAL(*expected, *actual);
}

TEST_F(DenseUnionBuilderTest, InferredType) {
  AppendInferred();

  auto expected_i8 = ArrayFromJSON(int8(), "[33, 10, -10]");
  auto expected_str = ArrayFromJSON(utf8(), R"(["abc", "", "def"])");
  auto expected_dbl = ArrayFromJSON(float64(), "[1.0, -1.0, 0.5]");

  auto expected_offsets = ArrayFromJSON(int32(), "[0, 1, 0, 1, 2, 2, 0, 1, 2]");

  std::shared_ptr<Array> expected;
  ASSERT_OK(UnionArray::MakeDense(*expected_types, *expected_offsets,
                                  {expected_i8, expected_str, expected_dbl},
                                  {"i8", "str", "dbl"}, {I8, STR, DBL}, &expected));

  ASSERT_EQ(expected->type()->ToString(), actual->type()->ToString());
  ASSERT_ARRAYS_EQUAL(*expected, *actual);
}

TEST_F(DenseUnionBuilderTest, ListOfInferredType) {
  std::shared_ptr<ListArray> actual;
  AppendListOfInferred(&actual);

  auto expected_type =
      list(union_({field("i8", int8()), field("str", utf8()), field("dbl", float64())},
                  {I8, STR, DBL}, UnionMode::DENSE));
  ASSERT_EQ(expected_type->ToString(), actual->type()->ToString());
}

TEST_F(SparseUnionBuilderTest, Basics) {
  union_builder.reset(new SparseUnionBuilder(
      default_memory_pool(), {i8_builder, str_builder, dbl_builder},
      union_({field("i8", int8()), field("str", utf8()), field("dbl", float64())},
             {I8, STR, DBL}, UnionMode::SPARSE)));

  AppendBasics();

  auto expected_i8 =
      ArrayFromJSON(int8(), "[33, null, null, null, null, 10, null, -10, null]");
  auto expected_str =
      ArrayFromJSON(utf8(), R"([null, "abc", null, null, "",  null, "def", null, null])");
  auto expected_dbl =
      ArrayFromJSON(float64(), "[null, null, 1.0, -1.0, null, null, null, null, 0.5]");

  std::shared_ptr<Array> expected;
  ASSERT_OK(UnionArray::MakeSparse(*expected_types,
                                   {expected_i8, expected_str, expected_dbl},
                                   {"i8", "str", "dbl"}, {I8, STR, DBL}, &expected));

  ASSERT_EQ(expected->type()->ToString(), actual->type()->ToString());
  ASSERT_ARRAYS_EQUAL(*expected, *actual);
}

TEST_F(SparseUnionBuilderTest, InferredType) {
  AppendInferred();

  auto expected_i8 =
      ArrayFromJSON(int8(), "[33, 10, null, null, null, -10, null, null, null]");
  auto expected_str =
      ArrayFromJSON(utf8(), R"([null, null, "abc", "", "def",  null, null, null, null])");
  auto expected_dbl =
      ArrayFromJSON(float64(), "[null, null, null, null, null, null, 1.0, -1.0, 0.5]");

  std::shared_ptr<Array> expected;
  ASSERT_OK(UnionArray::MakeSparse(*expected_types,
                                   {expected_i8, expected_str, expected_dbl},
                                   {"i8", "str", "dbl"}, {I8, STR, DBL}, &expected));

  ASSERT_EQ(expected->type()->ToString(), actual->type()->ToString());
  ASSERT_ARRAYS_EQUAL(*expected, *actual);
}
}  // namespace arrow
