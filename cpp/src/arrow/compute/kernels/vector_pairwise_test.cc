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

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "gmock/gmock.h"

namespace arrow::compute {

Result<std::shared_ptr<DataType>> GetOutputType(
    const std::shared_ptr<DataType> input_type) {
  switch (input_type->id()) {
    case Type::TIMESTAMP: {
      return duration(checked_cast<const TimestampType&>(*input_type).unit());
    }
    case Type::TIME32: {
      return duration(checked_cast<const Time32Type&>(*input_type).unit());
    }
    case Type::TIME64: {
      return duration(checked_cast<const Time64Type&>(*input_type).unit());
    }
    case Type::DATE32: {
      return duration(TimeUnit::SECOND);
    }
    case Type::DATE64: {
      return duration(TimeUnit::MILLI);
    }
    case Type::DECIMAL128: {
      const auto& real_type = checked_cast<const Decimal128Type&>(*input_type);
      return Decimal128Type::Make(real_type.precision() + 1, real_type.scale());
    }
    case Type::DECIMAL256: {
      const auto& real_type = checked_cast<const Decimal256Type&>(*input_type);
      return Decimal256Type::Make(real_type.precision() + 1, real_type.scale());
    }
    default: {
      return input_type;
    }
  }
}

class TestPairwiseDiff : public ::testing::Test {
 public:
  void SetUp() override {
    test_numerical_types_ = NumericTypes();
    test_temporal_types_ = TemporalTypes();
    test_decimal_types_ = {decimal(4, 2), decimal(70, 10)};

    test_input_types_.insert(test_input_types_.end(), test_numerical_types_.begin(),
                             test_numerical_types_.end());
    test_input_types_.insert(test_input_types_.end(), test_temporal_types_.begin(),
                             test_temporal_types_.end());
    test_input_types_.insert(test_input_types_.end(), test_decimal_types_.begin(),
                             test_decimal_types_.end());
  }

 protected:
  std::vector<std::shared_ptr<DataType>> test_numerical_types_;
  std::vector<std::shared_ptr<DataType>> test_temporal_types_;
  std::vector<std::shared_ptr<DataType>> test_decimal_types_;
  std::vector<std::shared_ptr<DataType>> test_input_types_;
};

TEST_F(TestPairwiseDiff, Empty) {
  for (int64_t period = -2; period <= 2; ++period) {
    PairwiseOptions options(period);
    for (auto input_type : test_input_types_) {
      ASSERT_OK_AND_ASSIGN(auto output_type, GetOutputType(input_type));
      auto input = ArrayFromJSON(input_type, "[]");
      auto output = ArrayFromJSON(output_type, "[]");
      CheckVectorUnary("pairwise_diff", input, output, &options);
    }
  }
}

TEST_F(TestPairwiseDiff, AllNull) {
  for (int64_t period = -2; period <= 2; ++period) {
    PairwiseOptions options(period);
    for (auto input_type : test_input_types_) {
      ASSERT_OK_AND_ASSIGN(auto output_type, GetOutputType(input_type));
      auto input = ArrayFromJSON(input_type, "[null, null, null]");
      auto output = ArrayFromJSON(output_type, "[null, null, null]");
      CheckVectorUnary("pairwise_diff", input, output, &options);
    }
  }
}

TEST_F(TestPairwiseDiff, Numeric) {
  {
    PairwiseOptions options(1);
    for (auto input_type : test_numerical_types_) {
      ASSERT_OK_AND_ASSIGN(auto output_type, GetOutputType(input_type));
      auto input = ArrayFromJSON(input_type, "[null, 1, 2, null, 4, 5, 6]");
      auto output = ArrayFromJSON(output_type, "[null, null, 1, null, null, 1, 1]");
      CheckVectorUnary("pairwise_diff", input, output, &options);
    }
  }

  {
    PairwiseOptions options(2);
    for (auto input_type : test_numerical_types_) {
      ASSERT_OK_AND_ASSIGN(auto output_type, GetOutputType(input_type));
      auto input = ArrayFromJSON(input_type, "[null, 1, 2, null, 4, 5, 6]");
      auto output = ArrayFromJSON(output_type, "[null, null, null, null, 2, null, 2]");
      CheckVectorUnary("pairwise_diff", input, output, &options);
    }
  }

  {
    PairwiseOptions options(-1);
    for (auto input_type : test_numerical_types_) {
      ASSERT_OK_AND_ASSIGN(auto output_type, GetOutputType(input_type));
      auto input = ArrayFromJSON(input_type, "[6, 5, 4, null, 2, 1, null]");
      auto output = ArrayFromJSON(output_type, "[1, 1, null, null, 1, null, null]");
      CheckVectorUnary("pairwise_diff", input, output, &options);
    }
  }

  {
    PairwiseOptions options(-2);
    for (auto input_type : test_numerical_types_) {
      ASSERT_OK_AND_ASSIGN(auto output_type, GetOutputType(input_type));
      auto input = ArrayFromJSON(input_type, "[6, 5, 4, null, 2, 1, null]");
      auto output = ArrayFromJSON(output_type, "[2, null, 2, null, null, null, null]");
      CheckVectorUnary("pairwise_diff", input, output, &options);
    }
  }
}

TEST_F(TestPairwiseDiff, Overflow) {
  {
    PairwiseOptions options(1);
    auto input = ArrayFromJSON(uint8(), "[3, 2, 1]");
    auto output = ArrayFromJSON(uint8(), "[null, 255, 255]");
    CheckVectorUnary("pairwise_diff", input, output, &options);
  }

  {
    PairwiseOptions options(1);
    auto input = ArrayFromJSON(uint8(), "[3, 2, 1]");
    auto output = ArrayFromJSON(uint8(), "[null, 255, 255]");
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, testing::HasSubstr("overflow"),
        CallFunction("pairwise_diff_checked", {input}, &options));
  }
}

TEST_F(TestPairwiseDiff, Temporal) {
  {
    PairwiseOptions options(1);
    for (auto input_type : test_temporal_types_) {
      ASSERT_OK_AND_ASSIGN(auto output_type, GetOutputType(input_type));
      auto input = ArrayFromJSON(input_type, "[null, 5, 1, null, 9, 6, 37]");
      auto output = ArrayFromJSON(
          output_type,
          input_type->id() != Type::DATE32  // Subtract date32 results in seconds
              ? "[null, null, -4, null, null, -3, 31]"
              : "[null, null, -345600, null, null, -259200, 2678400]");
      CheckVectorUnary("pairwise_diff", input, output, &options);
    }
  }
}

TEST_F(TestPairwiseDiff, Decimal) {
  {
    PairwiseOptions options(1);
    auto input = ArrayFromJSON(decimal(4, 2), R"(["11.00", "22.11", "-10.25", "33.45"])");
    auto output = ArrayFromJSON(decimal(5, 2), R"([null, "11.11", "-32.36", "43.70"])");
    CheckVectorUnary("pairwise_diff", input, output, &options);
  }

  {
    PairwiseOptions options(-1);
    auto input = ArrayFromJSON(
        decimal(40, 30),
        R"(["1111111111.222222222222222222222222222222", "2222222222.333333333333333333333333333333"])");
    auto output = ArrayFromJSON(
        decimal(41, 30), R"(["-1111111111.111111111111111111111111111111", null])");
    CheckVectorUnary("pairwise_diff", input, output, &options);
  }

  {  /// Out of range decimal precision
    PairwiseOptions options(1);
    auto input = ArrayFromJSON(decimal(38, 0), R"(["1e38"])");

    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    testing::HasSubstr("Decimal precision out of range"),
                                    CallFunction("pairwise_diff", {input}, &options));
  }
}
}  // namespace arrow::compute
