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

TEST(TestDictionaryType, Basics) {
  std::vector<int32_t> values = {100, 1000, 10000, 100000};
  std::shared_ptr<Array> dict;
  ArrayFromVector<Int32Type, int32_t>(int32(), values, &dict);

  std::shared_ptr<DictionaryType> type1 =
      std::dynamic_pointer_cast<DictionaryType>(dictionary(dict, Type::INT16));
  DictionaryType type2(dict, Type::INT16);

  ASSERT_EQ(Type::INT16, type1->index_type());
  ASSERT_TRUE(type1->dictionary()->Equals(dict));

  ASSERT_EQ(Type::INT16, type2.index_type());
  ASSERT_TRUE(type2.dictionary()->Equals(dict));

  ASSERT_EQ("dictionary<int32>", type1->ToString());
}

}  // namespace arrow
