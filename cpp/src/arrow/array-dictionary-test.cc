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

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <numeric>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

namespace arrow {

TEST(TestDictionary, Basics) {
  std::vector<int32_t> values = {100, 1000, 10000, 100000};
  std::shared_ptr<Array> dict;
  ArrayFromVector<Int32Type, int32_t>(int32(), values, &dict);

  std::shared_ptr<DictionaryType> type1 =
      std::dynamic_pointer_cast<DictionaryType>(dictionary(int16(), dict));
  DictionaryType type2(int16(), dict);

  ASSERT_TRUE(int16()->Equals(type1->index_type()));
  ASSERT_TRUE(type1->dictionary()->Equals(dict));

  ASSERT_TRUE(int16()->Equals(type2.index_type()));
  ASSERT_TRUE(type2.dictionary()->Equals(dict));

  ASSERT_EQ("dictionary<values=int32, indices=int16>", type1->ToString());
}

TEST(TestDictionary, Equals) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(utf8(), dict_values, &dict);
  std::shared_ptr<DataType> dict_type = dictionary(int16(), dict);

  std::shared_ptr<Array> dict2;
  std::vector<std::string> dict2_values = {"foo", "bar", "baz", "qux"};
  ArrayFromVector<StringType, std::string>(utf8(), dict2_values, &dict2);
  std::shared_ptr<DataType> dict2_type = dictionary(int16(), dict2);

  std::shared_ptr<Array> indices;
  std::vector<int16_t> indices_values = {1, 2, -1, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(int16(), is_valid, indices_values, &indices);

  std::shared_ptr<Array> indices2;
  std::vector<int16_t> indices2_values = {1, 2, 0, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(int16(), is_valid, indices2_values, &indices2);

  std::shared_ptr<Array> indices3;
  std::vector<int16_t> indices3_values = {1, 1, 0, 0, 2, 0};
  ArrayFromVector<Int16Type, int16_t>(int16(), is_valid, indices3_values, &indices3);

  auto arr = std::make_shared<DictionaryArray>(dict_type, indices);
  auto arr2 = std::make_shared<DictionaryArray>(dict_type, indices2);
  auto arr3 = std::make_shared<DictionaryArray>(dict2_type, indices);
  auto arr4 = std::make_shared<DictionaryArray>(dict_type, indices3);

  ASSERT_TRUE(arr->Equals(arr));

  // Equal, because the unequal index is masked by null
  ASSERT_TRUE(arr->Equals(arr2));

  // Unequal dictionaries
  ASSERT_FALSE(arr->Equals(arr3));

  // Unequal indices
  ASSERT_FALSE(arr->Equals(arr4));

  // RangeEquals
  ASSERT_TRUE(arr->RangeEquals(3, 6, 3, arr4));
  ASSERT_FALSE(arr->RangeEquals(1, 3, 1, arr4));
}

TEST(TestDictionary, Validate) {
  std::vector<bool> is_valid = {true, true, false, true, true, true};

  std::shared_ptr<Array> dict;
  std::vector<std::string> dict_values = {"foo", "bar", "baz"};
  ArrayFromVector<StringType, std::string>(utf8(), dict_values, &dict);
  std::shared_ptr<DataType> dict_type = dictionary(int16(), dict);

  std::shared_ptr<Array> indices;
  std::vector<uint8_t> indices_values = {1, 2, 0, 0, 2, 0};
  ArrayFromVector<UInt8Type, uint8_t>(uint8(), is_valid, indices_values, &indices);

  std::shared_ptr<Array> indices2;
  std::vector<float> indices2_values = {1., 2., 0., 0., 2., 0.};
  ArrayFromVector<FloatType, float>(float32(), is_valid, indices2_values, &indices2);

  std::shared_ptr<Array> indices3;
  std::vector<int64_t> indices3_values = {1, 2, 0, 0, 2, 0};
  ArrayFromVector<Int64Type, int64_t>(int64(), is_valid, indices3_values, &indices3);

  std::shared_ptr<Array> arr = std::make_shared<DictionaryArray>(dict_type, indices);
  std::shared_ptr<Array> arr2 = std::make_shared<DictionaryArray>(dict_type, indices2);
  std::shared_ptr<Array> arr3 = std::make_shared<DictionaryArray>(dict_type, indices3);

  // Only checking index type for now
  ASSERT_OK(arr->Validate());
  ASSERT_RAISES(Invalid, arr2->Validate());
  ASSERT_OK(arr3->Validate());
}

}  // namespace arrow
