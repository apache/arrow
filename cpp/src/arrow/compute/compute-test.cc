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
#include <sstream>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compare.h"
#include "arrow/ipc/test-common.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/test-common.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

#include "arrow/compute/cast.h"
#include "arrow/compute/context.h"

using std::vector;

namespace arrow {
namespace compute {

void AssertArraysEqual(const Array& left, const Array& right) {
  bool are_equal = false;
  ASSERT_OK(ArrayEquals(left, right, &are_equal));

  if (!are_equal) {
    std::stringstream ss;

    ss << "Left: ";
    EXPECT_OK(PrettyPrint(left, 0, &ss));
    ss << "\nRight: ";
    EXPECT_OK(PrettyPrint(right, 0, &ss));
    FAIL() << ss.str();
  }
}

class ComputeFixture {
 public:
  ComputeFixture() : pool_(default_memory_pool()), ctx_(pool_) {}

 protected:
  MemoryPool* pool_;
  FunctionContext ctx_;
};

// ----------------------------------------------------------------------
// Cast

class TestCast : public ComputeFixture, public ::testing::Test {
 public:
  void CheckPass(const Array& input, const Array& expected,
                 const std::shared_ptr<DataType>& out_type, const CastOptions& options) {
    std::shared_ptr<Array> result;
    ASSERT_OK(Cast(&ctx_, input, out_type, options, &result));
    AssertArraysEqual(expected, *result);
  }

  void CheckFail(const Array& input, const Array& expected,
                 const std::shared_ptr<DataType>& out_type, const CastOptions& options) {}

  template <typename InType, typename I_TYPE>
  void CheckFails(const std::shared_ptr<DataType>& in_type,
                  const std::vector<I_TYPE>& in_values, const std::vector<bool>& is_valid,
                  const std::shared_ptr<DataType>& out_type, const CastOptions& options) {
    std::shared_ptr<Array> input, result;
    if (is_valid.size() > 0) {
      ArrayFromVector<InType, I_TYPE>(in_type, is_valid, in_values, &input);
    } else {
      ArrayFromVector<InType, I_TYPE>(in_type, in_values, &input);
    }
    ASSERT_RAISES(Invalid, Cast(&ctx_, *input, out_type, options, &result));
  }

  template <typename InType, typename I_TYPE, typename OutType, typename O_TYPE>
  void CheckCase(const std::shared_ptr<DataType>& in_type,
                 const std::vector<I_TYPE>& in_values, const std::vector<bool>& is_valid,
                 const std::shared_ptr<DataType>& out_type,
                 const std::vector<O_TYPE>& out_values, const CastOptions& options) {
    std::shared_ptr<Array> input, expected;
    if (is_valid.size() > 0) {
      ArrayFromVector<InType, I_TYPE>(in_type, is_valid, in_values, &input);
      ArrayFromVector<OutType, O_TYPE>(out_type, is_valid, out_values, &expected);
    } else {
      ArrayFromVector<InType, I_TYPE>(in_type, in_values, &input);
      ArrayFromVector<OutType, O_TYPE>(out_type, out_values, &expected);
    }
    CheckPass(*input, *expected, out_type, options);
  }
};

TEST_F(TestCast, ToBoolean) {
  CastOptions options;

  vector<bool> is_valid = {true, false, true, true, true};

  vector<int8_t> v1 = {0, 1, 127, -1, 0};
  vector<bool> e1 = {false, true, true, true, false};
  CheckCase<Int8Type, int8_t, BooleanType, bool>(int8(), v1, is_valid, boolean(), e1,
                                                 options);
}

}  // namespace compute
}  // namespace arrow
