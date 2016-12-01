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
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/pretty_print.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/struct.h"

namespace arrow {

class TestArrayPrinter : public ::testing::Test {
 public:
  void SetUp() {}

  void Print(const Array& array) {}

 private:
  std::ostringstream sink_;
};

template <typename TYPE, typename C_TYPE>
void CheckPrimitive(const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
    const char* expected) {
  std::ostringstream sink;

  MemoryPool* pool = default_memory_pool();
  typename TypeTraits<TYPE>::BuilderType builder(pool, std::make_shared<TYPE>());

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder.Append(values[i]));
    } else {
      ASSERT_OK(builder.AppendNull());
    }
  }

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  ASSERT_OK(PrettyPrint(*array.get(), &sink));

  std::string result = sink.str();
  ASSERT_EQ(std::string(expected, strlen(expected)), result);
}

TEST_F(TestArrayPrinter, PrimitiveType) {
  std::vector<bool> is_valid = {true, true, false, true, false};

  std::vector<int32_t> values = {0, 1, 2, 3, 4};
  static const char* expected = R"expected([0, 1, null, 3, null])expected";
  CheckPrimitive<Int32Type, int32_t>(is_valid, values, expected);

  std::vector<std::string> values2 = {"foo", "bar", "", "baz", ""};
  static const char* ex2 = R"expected(["foo", "bar", null, "baz", null])expected";
  CheckPrimitive<StringType, std::string>(is_valid, values2, ex2);
}

}  // namespace arrow
