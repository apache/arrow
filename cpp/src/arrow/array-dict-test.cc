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

#include <array>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Dictionary tests

template <typename Type>
class TestDictionaryBuilder : public TestBuilder {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType>
    PrimitiveDictionaries;

TYPED_TEST_CASE(TestDictionaryBuilder, PrimitiveDictionaries);

TYPED_TEST(TestDictionaryBuilder, Basic) {
  using c_type = typename TypeParam::c_type;

  DictionaryBuilder<TypeParam> builder;
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.AppendNull());

  ASSERT_EQ(builder.length(), 4);
  ASSERT_EQ(builder.null_count(), 1);

  // Build expected data
  auto value_type = std::make_shared<TypeParam>();
  auto dict_type = dictionary(int8(), value_type);

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  DictionaryArray expected(dict_type, ArrayFromJSON(int8(), "[0, 1, 0, null]"),
                           ArrayFromJSON(value_type, "[1, 2]"));
  ASSERT_TRUE(expected.Equals(result));
}

TYPED_TEST(TestDictionaryBuilder, ArrayInit) {
  using c_type = typename TypeParam::c_type;

  auto value_type = std::make_shared<TypeParam>();
  auto dict_array = ArrayFromJSON(value_type, "[1, 2]");
  auto dict_type = dictionary(int8(), value_type);

  DictionaryBuilder<TypeParam> builder(dict_array);
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.AppendNull());

  ASSERT_EQ(builder.length(), 4);
  ASSERT_EQ(builder.null_count(), 1);

  // Build expected data

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  auto indices = ArrayFromJSON(int8(), "[0, 1, 0, null]");
  DictionaryArray expected(dict_type, indices, dict_array);

  AssertArraysEqual(expected, *result);
}

TYPED_TEST(TestDictionaryBuilder, MakeBuilder) {
  using c_type = typename TypeParam::c_type;

  auto value_type = std::make_shared<TypeParam>();
  auto dict_array = ArrayFromJSON(value_type, "[1, 2]");
  auto dict_type = dictionary(int8(), value_type);
  std::unique_ptr<ArrayBuilder> boxed_builder;
  ASSERT_OK(MakeBuilder(default_memory_pool(), dict_type, &boxed_builder));
  auto& builder = checked_cast<DictionaryBuilder<TypeParam>&>(*boxed_builder);

  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.AppendNull());

  ASSERT_EQ(builder.length(), 4);
  ASSERT_EQ(builder.null_count(), 1);

  // Build expected data

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  auto int_array = ArrayFromJSON(int8(), "[0, 1, 0, null]");
  DictionaryArray expected(dict_type, int_array, dict_array);

  AssertArraysEqual(expected, *result);
}

TYPED_TEST(TestDictionaryBuilder, ArrayConversion) {
  auto type = std::make_shared<TypeParam>();

  auto intermediate_result = ArrayFromJSON(type, "[1, 2, 1]");
  DictionaryBuilder<TypeParam> dictionary_builder;
  ASSERT_OK(dictionary_builder.AppendArray(*intermediate_result));
  std::shared_ptr<Array> result;
  ASSERT_OK(dictionary_builder.Finish(&result));

  // Build expected data
  auto dict_array = ArrayFromJSON(type, "[1, 2]");
  auto dict_type = dictionary(int8(), type);

  auto int_array = ArrayFromJSON(int8(), "[0, 1, 0]");
  DictionaryArray expected(dict_type, int_array, dict_array);

  ASSERT_TRUE(expected.Equals(result));
}

TYPED_TEST(TestDictionaryBuilder, DoubleTableSize) {
  using Scalar = typename TypeParam::c_type;
  // Skip this test for (u)int8
  if (sizeof(Scalar) > 1) {
    // Build the dictionary Array
    DictionaryBuilder<TypeParam> builder;
    // Build expected data
    NumericBuilder<TypeParam> dict_builder;
    Int16Builder int_builder;

    // Fill with 1024 different values
    for (int64_t i = 0; i < 1024; i++) {
      ASSERT_OK(builder.Append(static_cast<Scalar>(i)));
      ASSERT_OK(dict_builder.Append(static_cast<Scalar>(i)));
      ASSERT_OK(int_builder.Append(static_cast<uint16_t>(i)));
    }
    // Fill with an already existing value
    for (int64_t i = 0; i < 1024; i++) {
      ASSERT_OK(builder.Append(static_cast<Scalar>(1)));
      ASSERT_OK(int_builder.Append(1));
    }

    // Finalize result
    std::shared_ptr<Array> result;
    FinishAndCheckPadding(&builder, &result);

    // Finalize expected data
    std::shared_ptr<Array> dict_array;
    ASSERT_OK(dict_builder.Finish(&dict_array));

    auto dtype = dictionary(int16(), dict_array->type());
    std::shared_ptr<Array> int_array;
    ASSERT_OK(int_builder.Finish(&int_array));

    DictionaryArray expected(dtype, int_array, dict_array);
    ASSERT_TRUE(expected.Equals(result));
  }
}

TYPED_TEST(TestDictionaryBuilder, DeltaDictionary) {
  using c_type = typename TypeParam::c_type;
  auto type = std::make_shared<TypeParam>();

  DictionaryBuilder<TypeParam> builder;

  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  // Build expected data for the initial dictionary
  auto ex_dict = ArrayFromJSON(type, "[1, 2]");
  auto dict_type1 = dictionary(int8(), type);
  DictionaryArray expected(dict_type1, ArrayFromJSON(int8(), "[0, 1, 0, 1]"), ex_dict);

  ASSERT_TRUE(expected.Equals(result));

  // extend the dictionary builder with new data
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(3)));
  ASSERT_OK(builder.Append(static_cast<c_type>(3)));
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(3)));

  std::shared_ptr<Array> result_delta;
  ASSERT_OK(builder.Finish(&result_delta));

  // Build expected data for the delta dictionary
  auto ex_dict2 = ArrayFromJSON(type, "[3]");
  auto dict_type2 = dictionary(int8(), type);
  DictionaryArray expected_delta(dict_type2, ArrayFromJSON(int8(), "[1, 2, 2, 0, 2]"),
                                 ex_dict2);

  ASSERT_TRUE(expected_delta.Equals(result_delta));
}

TYPED_TEST(TestDictionaryBuilder, DoubleDeltaDictionary) {
  using c_type = typename TypeParam::c_type;
  auto type = std::make_shared<TypeParam>();
  auto dict_type = dictionary(int8(), type);

  DictionaryBuilder<TypeParam> builder;

  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  // Build expected data for the initial dictionary
  auto ex_dict1 = ArrayFromJSON(type, "[1, 2]");
  DictionaryArray expected(dict_type, ArrayFromJSON(int8(), "[0, 1, 0, 1]"), ex_dict1);

  ASSERT_TRUE(expected.Equals(result));

  // extend the dictionary builder with new data
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(3)));
  ASSERT_OK(builder.Append(static_cast<c_type>(3)));
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(3)));

  std::shared_ptr<Array> result_delta1;
  ASSERT_OK(builder.Finish(&result_delta1));

  // Build expected data for the delta dictionary
  auto ex_dict2 = ArrayFromJSON(type, "[3]");
  DictionaryArray expected_delta1(dict_type, ArrayFromJSON(int8(), "[1, 2, 2, 0, 2]"),
                                  ex_dict2);

  ASSERT_TRUE(expected_delta1.Equals(result_delta1));

  // extend the dictionary builder with new data again
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(3)));
  ASSERT_OK(builder.Append(static_cast<c_type>(4)));
  ASSERT_OK(builder.Append(static_cast<c_type>(5)));

  std::shared_ptr<Array> result_delta2;
  ASSERT_OK(builder.Finish(&result_delta2));

  // Build expected data for the delta dictionary again
  auto ex_dict3 = ArrayFromJSON(type, "[4, 5]");
  DictionaryArray expected_delta2(dict_type, ArrayFromJSON(int8(), "[0, 1, 2, 3, 4]"),
                                  ex_dict3);

  ASSERT_TRUE(expected_delta2.Equals(result_delta2));
}

TYPED_TEST(TestDictionaryBuilder, Dictionary32_BasicPrimitive) {
  using c_type = typename TypeParam::c_type;
  auto type = std::make_shared<TypeParam>();
  auto dict_type = dictionary(int32(), type);

  Dictionary32Builder<TypeParam> builder;

  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  ASSERT_OK(builder.Append(static_cast<c_type>(1)));
  ASSERT_OK(builder.Append(static_cast<c_type>(2)));
  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  // Build expected data for the initial dictionary
  auto ex_dict1 = ArrayFromJSON(type, "[1, 2]");
  DictionaryArray expected(dict_type, ArrayFromJSON(int32(), "[0, 1, 0, 1]"), ex_dict1);
  ASSERT_TRUE(expected.Equals(result));
}

TEST(TestStringDictionaryBuilder, Basic) {
  // Build the dictionary Array
  StringDictionaryBuilder builder;
  ASSERT_OK(builder.Append("test"));
  ASSERT_OK(builder.Append("test2"));
  ASSERT_OK(builder.Append("test"));

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  // Build expected data
  auto ex_dict = ArrayFromJSON(utf8(), "[\"test\", \"test2\"]");
  auto dtype = dictionary(int8(), utf8());
  auto int_array = ArrayFromJSON(int8(), "[0, 1, 0]");
  DictionaryArray expected(dtype, int_array, ex_dict);

  ASSERT_TRUE(expected.Equals(result));
}

template <typename BuilderType, typename IndexType, typename AppendCType>
void TestStringDictionaryAppendIndices() {
  auto index_type = TypeTraits<IndexType>::type_singleton();

  auto ex_dict = ArrayFromJSON(utf8(), R"(["c", "a", "b", "d"])");
  auto invalid_dict = ArrayFromJSON(binary(), R"(["e", "f"])");

  BuilderType builder;
  ASSERT_OK(builder.InsertMemoValues(*ex_dict));

  // Inserting again should have no effect
  ASSERT_OK(builder.InsertMemoValues(*ex_dict));

  // Type mismatch
  ASSERT_RAISES(Invalid, builder.InsertMemoValues(*invalid_dict));

  std::vector<AppendCType> raw_indices = {0, 1, 2, -1, 3};
  std::vector<uint8_t> is_valid = {1, 1, 1, 0, 1};
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(builder.AppendIndices(
        raw_indices.data(), static_cast<int64_t>(raw_indices.size()), is_valid.data()));
  }

  ASSERT_EQ(10, builder.length());

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  auto ex_indices = ArrayFromJSON(index_type, R"([0, 1, 2, null, 3, 0, 1, 2, null, 3])");
  auto dtype = dictionary(index_type, utf8());
  DictionaryArray expected(dtype, ex_indices, ex_dict);
  ASSERT_TRUE(expected.Equals(result));
}

TEST(TestStringDictionaryBuilder, AppendIndices) {
  // Currently AdaptiveIntBuilder only accepts int64_t in bulk appends
  TestStringDictionaryAppendIndices<StringDictionaryBuilder, Int8Type, int64_t>();

  TestStringDictionaryAppendIndices<StringDictionary32Builder, Int32Type, int32_t>();
}

TEST(TestStringDictionaryBuilder, ArrayInit) {
  auto dict_array = ArrayFromJSON(utf8(), R"(["test", "test2"])");
  auto int_array = ArrayFromJSON(int8(), "[0, 1, 0]");

  // Build the dictionary Array
  StringDictionaryBuilder builder(dict_array);
  ASSERT_OK(builder.Append("test"));
  ASSERT_OK(builder.Append("test2"));
  ASSERT_OK(builder.Append("test"));

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  // Build expected data
  DictionaryArray expected(dictionary(int8(), utf8()), int_array, dict_array);

  AssertArraysEqual(expected, *result);
}

TEST(TestStringDictionaryBuilder, MakeBuilder) {
  auto dict_array = ArrayFromJSON(utf8(), R"(["test", "test2"])");
  auto dict_type = dictionary(int8(), utf8());
  auto int_array = ArrayFromJSON(int8(), "[0, 1, 0]");
  std::unique_ptr<ArrayBuilder> boxed_builder;
  ASSERT_OK(MakeBuilder(default_memory_pool(), dict_type, &boxed_builder));
  auto& builder = checked_cast<StringDictionaryBuilder&>(*boxed_builder);

  // Build the dictionary Array
  ASSERT_OK(builder.Append("test"));
  ASSERT_OK(builder.Append("test2"));
  ASSERT_OK(builder.Append("test"));

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  // Build expected data
  DictionaryArray expected(dict_type, int_array, dict_array);

  AssertArraysEqual(expected, *result);
}

// ARROW-4367
TEST(TestStringDictionaryBuilder, OnlyNull) {
  // Build the dictionary Array
  StringDictionaryBuilder builder;
  ASSERT_OK(builder.AppendNull());

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  // Build expected data
  auto dict = ArrayFromJSON(utf8(), "[]");
  auto dtype = dictionary(int8(), utf8());
  auto int_array = ArrayFromJSON(int8(), "[null]");
  DictionaryArray expected(dtype, int_array, dict);

  ASSERT_TRUE(expected.Equals(result));
}

TEST(TestStringDictionaryBuilder, DoubleTableSize) {
  // Build the dictionary Array
  StringDictionaryBuilder builder;
  // Build expected data
  StringBuilder str_builder;
  Int16Builder int_builder;

  // Fill with 1024 different values
  for (int64_t i = 0; i < 1024; i++) {
    std::stringstream ss;
    ss << "test" << i;
    ASSERT_OK(builder.Append(ss.str()));
    ASSERT_OK(str_builder.Append(ss.str()));
    ASSERT_OK(int_builder.Append(static_cast<uint16_t>(i)));
  }
  // Fill with an already existing value
  for (int64_t i = 0; i < 1024; i++) {
    ASSERT_OK(builder.Append("test1"));
    ASSERT_OK(int_builder.Append(1));
  }

  // Finalize result
  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  // Finalize expected data
  std::shared_ptr<Array> str_array;
  ASSERT_OK(str_builder.Finish(&str_array));
  auto dtype = dictionary(int16(), utf8());
  std::shared_ptr<Array> int_array;
  ASSERT_OK(int_builder.Finish(&int_array));

  DictionaryArray expected(dtype, int_array, str_array);
  ASSERT_TRUE(expected.Equals(result));
}

TEST(TestStringDictionaryBuilder, DeltaDictionary) {
  // Build the dictionary Array
  StringDictionaryBuilder builder;
  ASSERT_OK(builder.Append("test"));
  ASSERT_OK(builder.Append("test2"));
  ASSERT_OK(builder.Append("test"));

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  // Build expected data
  auto dict = ArrayFromJSON(utf8(), "[\"test\", \"test2\"]");
  auto dtype = dictionary(int8(), utf8());
  auto int_array = ArrayFromJSON(int8(), "[0, 1, 0]");
  DictionaryArray expected(dtype, int_array, dict);

  ASSERT_TRUE(expected.Equals(result));

  // build a delta dictionary
  ASSERT_OK(builder.Append("test2"));
  ASSERT_OK(builder.Append("test3"));
  ASSERT_OK(builder.Append("test2"));

  std::shared_ptr<Array> result_delta;
  FinishAndCheckPadding(&builder, &result_delta);

  // Build expected data
  DictionaryArray expected_delta(dtype, ArrayFromJSON(int8(), "[1, 2, 1]"),
                                 ArrayFromJSON(utf8(), "[\"test3\"]"));

  ASSERT_TRUE(expected_delta.Equals(result_delta));
}

TEST(TestStringDictionaryBuilder, BigDeltaDictionary) {
  constexpr int16_t kTestLength = 2048;
  // Build the dictionary Array
  StringDictionaryBuilder builder;

  StringBuilder str_builder1;
  Int16Builder int_builder1;

  for (int16_t idx = 0; idx < kTestLength; ++idx) {
    std::stringstream sstream;
    sstream << "test" << idx;
    ASSERT_OK(builder.Append(sstream.str()));
    ASSERT_OK(str_builder1.Append(sstream.str()));
    ASSERT_OK(int_builder1.Append(idx));
  }

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  std::shared_ptr<Array> str_array1;
  ASSERT_OK(str_builder1.Finish(&str_array1));

  auto dtype1 = dictionary(int16(), utf8());

  std::shared_ptr<Array> int_array1;
  ASSERT_OK(int_builder1.Finish(&int_array1));

  DictionaryArray expected(dtype1, int_array1, str_array1);
  ASSERT_TRUE(expected.Equals(result));

  // build delta 1
  StringBuilder str_builder2;
  Int16Builder int_builder2;

  for (int16_t idx = 0; idx < kTestLength; ++idx) {
    ASSERT_OK(builder.Append("test1"));
    ASSERT_OK(int_builder2.Append(1));
  }

  for (int16_t idx = 0; idx < kTestLength; ++idx) {
    ASSERT_OK(builder.Append("test_new_value1"));
    ASSERT_OK(int_builder2.Append(kTestLength));
  }
  ASSERT_OK(str_builder2.Append("test_new_value1"));

  std::shared_ptr<Array> result2;
  ASSERT_OK(builder.Finish(&result2));

  std::shared_ptr<Array> str_array2;
  ASSERT_OK(str_builder2.Finish(&str_array2));
  auto dtype2 = dictionary(int16(), utf8());

  std::shared_ptr<Array> int_array2;
  ASSERT_OK(int_builder2.Finish(&int_array2));

  DictionaryArray expected2(dtype2, int_array2, str_array2);
  ASSERT_TRUE(expected2.Equals(result2));

  // build delta 2
  StringBuilder str_builder3;
  Int16Builder int_builder3;

  for (int16_t idx = 0; idx < kTestLength; ++idx) {
    ASSERT_OK(builder.Append("test2"));
    ASSERT_OK(int_builder3.Append(2));
  }

  for (int16_t idx = 0; idx < kTestLength; ++idx) {
    ASSERT_OK(builder.Append("test_new_value2"));
    ASSERT_OK(int_builder3.Append(kTestLength + 1));
  }
  ASSERT_OK(str_builder3.Append("test_new_value2"));

  std::shared_ptr<Array> result3;
  ASSERT_OK(builder.Finish(&result3));

  std::shared_ptr<Array> str_array3;
  ASSERT_OK(str_builder3.Finish(&str_array3));
  auto dtype3 = dictionary(int16(), utf8());

  std::shared_ptr<Array> int_array3;
  ASSERT_OK(int_builder3.Finish(&int_array3));

  DictionaryArray expected3(dtype3, int_array3, str_array3);
  ASSERT_TRUE(expected3.Equals(result3));
}

TEST(TestFixedSizeBinaryDictionaryBuilder, Basic) {
  // Build the dictionary Array
  DictionaryBuilder<FixedSizeBinaryType> builder(arrow::fixed_size_binary(4));
  std::vector<uint8_t> test{12, 12, 11, 12};
  std::vector<uint8_t> test2{12, 12, 11, 11};
  ASSERT_OK(builder.Append(test.data()));
  ASSERT_OK(builder.Append(test2.data()));
  ASSERT_OK(builder.Append(test.data()));

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  // Build expected data
  auto value_type = arrow::fixed_size_binary(4);
  FixedSizeBinaryBuilder fsb_builder(value_type);
  ASSERT_OK(fsb_builder.Append(test.data()));
  ASSERT_OK(fsb_builder.Append(test2.data()));
  std::shared_ptr<Array> fsb_array;
  ASSERT_OK(fsb_builder.Finish(&fsb_array));

  auto dtype = dictionary(int8(), value_type);

  Int8Builder int_builder;
  ASSERT_OK(int_builder.Append(0));
  ASSERT_OK(int_builder.Append(1));
  ASSERT_OK(int_builder.Append(0));
  std::shared_ptr<Array> int_array;
  ASSERT_OK(int_builder.Finish(&int_array));

  DictionaryArray expected(dtype, int_array, fsb_array);
  ASSERT_TRUE(expected.Equals(result));
}

TEST(TestFixedSizeBinaryDictionaryBuilder, ArrayInit) {
  // Build the dictionary Array
  auto value_type = fixed_size_binary(4);
  auto dict_array = ArrayFromJSON(value_type, R"(["abcd", "wxyz"])");
  util::string_view test = "abcd", test2 = "wxyz";
  DictionaryBuilder<FixedSizeBinaryType> builder(dict_array);
  ASSERT_OK(builder.Append(test));
  ASSERT_OK(builder.Append(test2));
  ASSERT_OK(builder.Append(test));

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  // Build expected data
  auto indices = ArrayFromJSON(int8(), "[0, 1, 0]");
  DictionaryArray expected(dictionary(int8(), value_type), indices, dict_array);
  AssertArraysEqual(expected, *result);
}

TEST(TestFixedSizeBinaryDictionaryBuilder, MakeBuilder) {
  // Build the dictionary Array
  auto value_type = fixed_size_binary(4);
  auto dict_array = ArrayFromJSON(value_type, R"(["abcd", "wxyz"])");
  auto dict_type = dictionary(int8(), value_type);

  std::unique_ptr<ArrayBuilder> boxed_builder;
  ASSERT_OK(MakeBuilder(default_memory_pool(), dict_type, &boxed_builder));
  auto& builder = checked_cast<DictionaryBuilder<FixedSizeBinaryType>&>(*boxed_builder);
  util::string_view test = "abcd", test2 = "wxyz";
  ASSERT_OK(builder.Append(test));
  ASSERT_OK(builder.Append(test2));
  ASSERT_OK(builder.Append(test));

  std::shared_ptr<Array> result;
  FinishAndCheckPadding(&builder, &result);

  // Build expected data
  auto indices = ArrayFromJSON(int8(), "[0, 1, 0]");
  DictionaryArray expected(dict_type, indices, dict_array);
  AssertArraysEqual(expected, *result);
}

TEST(TestFixedSizeBinaryDictionaryBuilder, DeltaDictionary) {
  // Build the dictionary Array
  auto value_type = arrow::fixed_size_binary(4);
  auto dict_type = dictionary(int8(), value_type);

  DictionaryBuilder<FixedSizeBinaryType> builder(value_type);
  std::vector<uint8_t> test{12, 12, 11, 12};
  std::vector<uint8_t> test2{12, 12, 11, 11};
  std::vector<uint8_t> test3{12, 12, 11, 10};

  ASSERT_OK(builder.Append(test.data()));
  ASSERT_OK(builder.Append(test2.data()));
  ASSERT_OK(builder.Append(test.data()));

  std::shared_ptr<Array> result1;
  FinishAndCheckPadding(&builder, &result1);

  // Build expected data
  FixedSizeBinaryBuilder fsb_builder1(value_type);
  ASSERT_OK(fsb_builder1.Append(test.data()));
  ASSERT_OK(fsb_builder1.Append(test2.data()));
  std::shared_ptr<Array> fsb_array1;
  ASSERT_OK(fsb_builder1.Finish(&fsb_array1));

  Int8Builder int_builder1;
  ASSERT_OK(int_builder1.Append(0));
  ASSERT_OK(int_builder1.Append(1));
  ASSERT_OK(int_builder1.Append(0));
  std::shared_ptr<Array> int_array1;
  ASSERT_OK(int_builder1.Finish(&int_array1));

  DictionaryArray expected1(dict_type, int_array1, fsb_array1);
  ASSERT_TRUE(expected1.Equals(result1));

  // build delta dictionary
  ASSERT_OK(builder.Append(test.data()));
  ASSERT_OK(builder.Append(test2.data()));
  ASSERT_OK(builder.Append(test3.data()));

  std::shared_ptr<Array> result2;
  FinishAndCheckPadding(&builder, &result2);

  // Build expected data
  FixedSizeBinaryBuilder fsb_builder2(value_type);
  ASSERT_OK(fsb_builder2.Append(test3.data()));
  std::shared_ptr<Array> fsb_array2;
  ASSERT_OK(fsb_builder2.Finish(&fsb_array2));

  Int8Builder int_builder2;
  ASSERT_OK(int_builder2.Append(0));
  ASSERT_OK(int_builder2.Append(1));
  ASSERT_OK(int_builder2.Append(2));
  std::shared_ptr<Array> int_array2;
  ASSERT_OK(int_builder2.Finish(&int_array2));

  DictionaryArray expected2(dict_type, int_array2, fsb_array2);
  ASSERT_TRUE(expected2.Equals(result2));
}

TEST(TestFixedSizeBinaryDictionaryBuilder, DoubleTableSize) {
  // Build the dictionary Array
  auto value_type = arrow::fixed_size_binary(4);
  auto dict_type = dictionary(int16(), value_type);

  DictionaryBuilder<FixedSizeBinaryType> builder(value_type);
  // Build expected data
  FixedSizeBinaryBuilder fsb_builder(value_type);
  Int16Builder int_builder;

  // Fill with 1024 different values
  for (int64_t i = 0; i < 1024; i++) {
    std::vector<uint8_t> value{12, 12, static_cast<uint8_t>(i / 128),
                               static_cast<uint8_t>(i % 128)};
    ASSERT_OK(builder.Append(value.data()));
    ASSERT_OK(fsb_builder.Append(value.data()));
    ASSERT_OK(int_builder.Append(static_cast<uint16_t>(i)));
  }
  // Fill with an already existing value
  std::vector<uint8_t> known_value{12, 12, 0, 1};
  for (int64_t i = 0; i < 1024; i++) {
    ASSERT_OK(builder.Append(known_value.data()));
    ASSERT_OK(int_builder.Append(1));
  }

  // Finalize result
  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  // Finalize expected data
  std::shared_ptr<Array> fsb_array;
  ASSERT_OK(fsb_builder.Finish(&fsb_array));
  std::shared_ptr<Array> int_array;
  ASSERT_OK(int_builder.Finish(&int_array));

  DictionaryArray expected(dict_type, int_array, fsb_array);
  ASSERT_TRUE(expected.Equals(result));
}

TEST(TestFixedSizeBinaryDictionaryBuilder, InvalidTypeAppend) {
  // Build the dictionary Array
  auto value_type = arrow::fixed_size_binary(4);
  DictionaryBuilder<FixedSizeBinaryType> builder(value_type);
  // Build an array with different byte width
  FixedSizeBinaryBuilder fsb_builder(arrow::fixed_size_binary(5));
  std::vector<uint8_t> value{100, 1, 1, 1, 1};
  ASSERT_OK(fsb_builder.Append(value.data()));
  std::shared_ptr<Array> fsb_array;
  ASSERT_OK(fsb_builder.Finish(&fsb_array));

  ASSERT_RAISES(Invalid, builder.AppendArray(*fsb_array));
}

TEST(TestDecimalDictionaryBuilder, Basic) {
  // Build the dictionary Array
  auto decimal_type = arrow::decimal(2, 0);
  DictionaryBuilder<FixedSizeBinaryType> builder(decimal_type);

  // Test data
  std::vector<Decimal128> test{12, 12, 11, 12};
  for (const auto& value : test) {
    ASSERT_OK(builder.Append(value.ToBytes().data()));
  }

  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  // Build expected data
  DictionaryArray expected(dictionary(int8(), decimal_type),
                           ArrayFromJSON(int8(), "[0, 0, 1, 0]"),
                           ArrayFromJSON(decimal_type, "[\"12\", \"11\"]"));

  ASSERT_TRUE(expected.Equals(result));
}

TEST(TestDecimalDictionaryBuilder, DoubleTableSize) {
  const auto& decimal_type = arrow::decimal(21, 0);

  // Build the dictionary Array
  DictionaryBuilder<FixedSizeBinaryType> builder(decimal_type);

  // Build expected data
  FixedSizeBinaryBuilder fsb_builder(decimal_type);
  Int16Builder int_builder;

  // Fill with 1024 different values
  for (int64_t i = 0; i < 1024; i++) {
    const uint8_t bytes[] = {0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             0,
                             12,
                             12,
                             static_cast<uint8_t>(i / 128),
                             static_cast<uint8_t>(i % 128)};
    ASSERT_OK(builder.Append(bytes));
    ASSERT_OK(fsb_builder.Append(bytes));
    ASSERT_OK(int_builder.Append(static_cast<uint16_t>(i)));
  }
  // Fill with an already existing value
  const uint8_t known_value[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 12, 0, 1};
  for (int64_t i = 0; i < 1024; i++) {
    ASSERT_OK(builder.Append(known_value));
    ASSERT_OK(int_builder.Append(1));
  }

  // Finalize result
  std::shared_ptr<Array> result;
  ASSERT_OK(builder.Finish(&result));

  // Finalize expected data
  std::shared_ptr<Array> fsb_array;
  ASSERT_OK(fsb_builder.Finish(&fsb_array));

  std::shared_ptr<Array> int_array;
  ASSERT_OK(int_builder.Finish(&int_array));

  DictionaryArray expected(dictionary(int16(), decimal_type), int_array, fsb_array);
  ASSERT_TRUE(expected.Equals(result));
}

// ----------------------------------------------------------------------
// DictionaryArray tests

TEST(TestDictionary, Equals) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};
  std::shared_ptr<Array> dict, dict2, indices, indices2, indices3;

  dict = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");
  std::shared_ptr<DataType> dict_type = dictionary(int16(), utf8());

  dict2 = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\", \"qux\"]");
  std::shared_ptr<DataType> dict2_type = dictionary(int16(), utf8());

  std::vector<int16_t> indices_values = {1, 2, -1, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices_values, &indices);

  std::vector<int16_t> indices2_values = {1, 2, 0, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices2_values, &indices2);

  std::vector<int16_t> indices3_values = {1, 1, 0, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid, indices3_values, &indices3);

  auto array = std::make_shared<DictionaryArray>(dict_type, indices, dict);
  auto array2 = std::make_shared<DictionaryArray>(dict_type, indices2, dict);
  auto array3 = std::make_shared<DictionaryArray>(dict2_type, indices, dict2);
  auto array4 = std::make_shared<DictionaryArray>(dict_type, indices3, dict);

  ASSERT_TRUE(array->Equals(array));

  // Equal, because the unequal index is masked by null
  ASSERT_TRUE(array->Equals(array2));

  // Unequal dictionaries
  ASSERT_FALSE(array->Equals(array3));

  // Unequal indices
  ASSERT_FALSE(array->Equals(array4));

  // RangeEquals
  ASSERT_TRUE(array->RangeEquals(3, 6, 3, array4));
  ASSERT_FALSE(array->RangeEquals(1, 3, 1, array4));

  // ARROW-33 Test slices
  const int64_t size = array->length();

  std::shared_ptr<Array> slice, slice2;
  slice = array->Array::Slice(2);
  slice2 = array->Array::Slice(2);
  ASSERT_EQ(size - 2, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(2, array->length(), 0, slice));

  // Chained slices
  slice2 = array->Array::Slice(1)->Array::Slice(1);
  ASSERT_TRUE(slice->Equals(slice2));

  slice = array->Slice(1, 3);
  slice2 = array->Slice(1, 3);
  ASSERT_EQ(3, slice->length());

  ASSERT_TRUE(slice->Equals(slice2));
  ASSERT_TRUE(array->RangeEquals(1, 4, 0, slice));
}

TEST(TestDictionary, Validate) {
  auto dict = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");
  auto dict_type = dictionary(int16(), utf8());

  auto indices = ArrayFromJSON(int16(), "[1, 2, null, 0, 2, 0]");
  std::shared_ptr<Array> arr =
      std::make_shared<DictionaryArray>(dict_type, indices, dict);

  // Only checking index type for now
  ASSERT_OK(ValidateArray(*arr));

  ASSERT_DEATH(
      {
        std::shared_ptr<Array> null_dict_arr =
            std::make_shared<DictionaryArray>(dict_type, indices, nullptr);
      },
      "");
}

TEST(TestDictionary, FromArray) {
  auto dict = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");
  auto dict_type = dictionary(int16(), utf8());

  auto indices1 = ArrayFromJSON(int16(), "[1, 2, 0, 0, 2, 0]");
  auto indices2 = ArrayFromJSON(int16(), "[1, 2, 0, 3, 2, 0]");

  // Invalid index is masked by null
  std::shared_ptr<Array> indices3;
  std::vector<bool> is_valid3 = {true, true, false, true, true, true};
  std::vector<int16_t> indices_values3 = {1, 2, -1, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(is_valid3, indices_values3, &indices3);

  // Index out of bounds
  auto indices4 = ArrayFromJSON(int16(), "[1, 2, null, 3, 2, 0]");

  std::shared_ptr<Array> arr1, arr2, arr3, arr4;
  ASSERT_OK(DictionaryArray::FromArrays(dict_type, indices1, dict, &arr1));
  ASSERT_RAISES(Invalid, DictionaryArray::FromArrays(dict_type, indices2, dict, &arr2));
  ASSERT_OK(DictionaryArray::FromArrays(dict_type, indices3, dict, &arr3));
  ASSERT_RAISES(Invalid, DictionaryArray::FromArrays(dict_type, indices4, dict, &arr4));
}

TEST(TestDictionary, TransposeBasic) {
  std::shared_ptr<Array> arr, out, expected;

  auto dict = ArrayFromJSON(utf8(), "[\"A\", \"B\", \"C\"]");
  auto dict_type = dictionary(int16(), utf8());
  auto indices = ArrayFromJSON(int16(), "[1, 2, 0, 0]");
  // ["B", "C", "A", "A"]
  ASSERT_OK(DictionaryArray::FromArrays(dict_type, indices, dict, &arr));

  // Transpose to same index type
  {
    auto out_dict = ArrayFromJSON(utf8(), "[\"Z\", \"A\", \"C\", \"B\"]");

    const std::vector<int32_t> transpose_map{1, 3, 2};
    ASSERT_OK(internal::checked_cast<const DictionaryArray&>(*arr).Transpose(
        default_memory_pool(), dict_type, out_dict, transpose_map, &out));

    auto expected_indices = ArrayFromJSON(int16(), "[3, 2, 1, 1]");
    ASSERT_OK(
        DictionaryArray::FromArrays(dict_type, expected_indices, out_dict, &expected));
    AssertArraysEqual(*out, *expected);
  }

  // Transpose to other type
  {
    auto out_dict = ArrayFromJSON(utf8(), "[\"Z\", \"A\", \"C\", \"B\"]");
    auto out_dict_type = dictionary(int8(), utf8());

    const std::vector<int32_t> transpose_map{1, 3, 2};
    ASSERT_OK(internal::checked_cast<const DictionaryArray&>(*arr).Transpose(
        default_memory_pool(), out_dict_type, out_dict, transpose_map, &out));

    auto expected_indices = ArrayFromJSON(int8(), "[3, 2, 1, 1]");
    ASSERT_OK(DictionaryArray::FromArrays(out_dict_type, expected_indices, out_dict,
                                          &expected));
    AssertArraysEqual(*expected, *out);
  }
}

TEST(TestDictionary, TransposeNulls) {
  std::shared_ptr<Array> arr, out, expected;

  auto dict = ArrayFromJSON(utf8(), "[\"A\", \"B\", \"C\"]");
  auto dict_type = dictionary(int16(), utf8());
  auto indices = ArrayFromJSON(int16(), "[1, 2, null, 0]");
  // ["B", "C", null, "A"]
  ASSERT_OK(DictionaryArray::FromArrays(dict_type, indices, dict, &arr));

  auto out_dict = ArrayFromJSON(utf8(), "[\"Z\", \"A\", \"C\", \"B\"]");
  auto out_dict_type = dictionary(int16(), utf8());

  const std::vector<int32_t> transpose_map{1, 3, 2};
  ASSERT_OK(internal::checked_cast<const DictionaryArray&>(*arr).Transpose(
      default_memory_pool(), out_dict_type, out_dict, transpose_map, &out));

  auto expected_indices = ArrayFromJSON(int16(), "[3, 2, null, 1]");
  ASSERT_OK(
      DictionaryArray::FromArrays(out_dict_type, expected_indices, out_dict, &expected));
  AssertArraysEqual(*expected, *out);
}

TEST(TestDictionary, DISABLED_ListOfDictionary) {
  std::unique_ptr<ArrayBuilder> root_builder;
  ASSERT_OK(MakeBuilder(default_memory_pool(), list(dictionary(int8(), utf8())),
                        &root_builder));
  auto list_builder = checked_cast<ListBuilder*>(root_builder.get());
  auto dict_builder =
      checked_cast<DictionaryBuilder<StringType>*>(list_builder->value_builder());

  ASSERT_OK(list_builder->Append());
  std::vector<std::string> expected;
  for (char a : "abc") {
    for (char d : "def") {
      for (char g : "ghi") {
        for (char j : "jkl") {
          for (char m : "mno") {
            for (char p : "pqr") {
              if ((static_cast<int>(a) + d + g + j + m + p) % 16 == 0) {
                ASSERT_OK(list_builder->Append());
              }
              // 3**6 distinct strings; too large for int8
              char str[6] = {a, d, g, j, m, p};
              ASSERT_OK(dict_builder->Append(str));
              expected.push_back(str);
            }
          }
        }
      }
    }
  }
  std::shared_ptr<Array> expected_dict;
  ArrayFromVector<StringType, std::string>(expected, &expected_dict);

  std::shared_ptr<Array> array;
  ASSERT_OK(root_builder->Finish(&array));
  ASSERT_OK(ValidateArray(*array));

  auto expected_type = list(dictionary(int16(), utf8()));
  ASSERT_EQ(array->type()->ToString(), expected_type->ToString());

  auto list_array = checked_cast<const ListArray*>(array.get());
  auto actual_dict =
      checked_cast<const DictionaryArray&>(*list_array->values()).dictionary();
  ASSERT_ARRAYS_EQUAL(*expected_dict, *actual_dict);
}

}  // namespace arrow
