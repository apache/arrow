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

#include "arrow/testing/gtest_util.h"

#include "arrow/array/concatenate.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/compute/kernels/vector_run_end_selection.h"
#include "arrow/util/logging.h"

namespace arrow {

using compute::internal::REEFilterExec;
using internal::checked_cast;

namespace compute {

const auto kEmitNulls = FilterOptions{FilterOptions::EMIT_NULL};
const auto kDropNulls = FilterOptions{FilterOptions::DROP};

std::unique_ptr<REEFilterExec> MakeREEFilterExec(const ArraySpan& values,
                                                 const ArraySpan& filter,
                                                 const FilterOptions& options) {
  auto* pool = default_memory_pool();
  Result<std::unique_ptr<REEFilterExec>> result;
  if (values.type->id() == Type::RUN_END_ENCODED) {
    if (filter.type->id() == Type::RUN_END_ENCODED) {
      result = internal::MakeREExREEFilterExec(pool, values, filter, options);
    } else {
      result = internal::MakeREExPlainFilterExec(pool, values, filter, options);
    }
  } else {
    DCHECK_EQ(filter.type->id(), Type::RUN_END_ENCODED);
    result = internal::MakePlainxREEFilterExec(pool, values, filter, options);
  }
  EXPECT_OK_AND_ASSIGN(auto exec, std::move(result));
  return exec;
}

Result<std::shared_ptr<Array>> REEFromJson(const std::shared_ptr<DataType>& run_end_type,
                                           const std::shared_ptr<DataType>& value_type,
                                           const std::string& json) {
  auto array = ArrayFromJSON(value_type, json);
  ARROW_ASSIGN_OR_RAISE(auto datum,
                        RunEndEncode(array, RunEndEncodeOptions{run_end_type}));
  return datum.make_array();
}

Result<std::shared_ptr<Array>> FilterFromJson(
    const std::shared_ptr<DataType>& filter_type, const std::string& json) {
  if (filter_type->id() == Type::RUN_END_ENCODED) {
    auto& ree_type = checked_cast<const RunEndEncodedType&>(*filter_type);
    return REEFromJson(ree_type.run_end_type(), ree_type.value_type(), json);
  } else {
    return ArrayFromJSON(filter_type, json);
  }
}

void DoAssertFilterOutputSize(const std::shared_ptr<Array>& values,
                              const std::shared_ptr<Array>& filter,
                              const FilterOptions& null_options,
                              int64_t expected_logical_output_size,
                              int64_t expected_physical_output_size) {
  auto values_span = ArraySpan(*values->data());
  auto filter_span = ArraySpan(*filter->data());
  auto filter_exec = MakeREEFilterExec(values_span, filter_span, null_options);

  EXPECT_OK_AND_ASSIGN(auto calculated_output_size, filter_exec->CalculateOutputSize());
  if (values_span.type->id() == Type::RUN_END_ENCODED) {
    ASSERT_EQ(calculated_output_size, expected_physical_output_size);
  } else {
    ASSERT_EQ(calculated_output_size, expected_logical_output_size);
  }
}

void DoAssertFilterSlicedOutputSize(const std::shared_ptr<Array>& values,
                                    const std::shared_ptr<Array>& filter,
                                    const FilterOptions& null_options,
                                    int64_t expected_logical_output_size,
                                    int64_t expected_physical_output_size) {
  constexpr auto M = 3;
  constexpr auto N = 2;
  // Check slicing: add M dummy values at the start and end of `values`,
  // add N dummy values at the start and end of `filter`.
  ARROW_SCOPED_TRACE("for sliced values and filter");
  ASSERT_OK_AND_ASSIGN(auto values_filler, MakeArrayOfNull(values->type(), M));
  ASSERT_OK_AND_ASSIGN(auto filter_filler,
                       FilterFromJson(filter->type(), "[true, false]"));
  ASSERT_OK_AND_ASSIGN(auto values_with_filler,
                       Concatenate({values_filler, values, values_filler}));
  ASSERT_OK_AND_ASSIGN(auto filter_with_filler,
                       Concatenate({filter_filler, filter, filter_filler}));
  auto values_sliced = values_with_filler->Slice(M, values->length());
  auto filter_sliced = filter_with_filler->Slice(N, filter->length());
  DoAssertFilterOutputSize(values_sliced, filter_sliced, null_options,
                           expected_logical_output_size, expected_physical_output_size);
}

void DoAssertOutputSize(const std::shared_ptr<Array>& values,
                        const std::shared_ptr<Array>& filter,
                        const FilterOptions& null_options,
                        int64_t expected_logical_output_size,
                        int64_t expected_physical_output_size = -1) {
  ARROW_SCOPED_TRACE("assert output size");
  ARROW_SCOPED_TRACE(null_options.null_selection_behavior ==
                             FilterOptions::NullSelectionBehavior::DROP
                         ? "while dropping nulls"
                         : "while emitting nulls");
  {
    ARROW_SCOPED_TRACE("for full values and filter");
    DoAssertFilterOutputSize(values, filter, null_options, expected_logical_output_size,
                             expected_physical_output_size);
  }
  DoAssertFilterSlicedOutputSize(values, filter, null_options,
                                 expected_logical_output_size,
                                 expected_physical_output_size);
}

template <typename RunEndTypes>
struct REExREEFilterTest : public ::testing::Test {
  using ValueRunEndType = typename RunEndTypes::ValueRunEndType;
  using FilterRunEndType = typename RunEndTypes::FilterRunEndType;

  std::shared_ptr<DataType> _value_run_end_type;
  std::shared_ptr<DataType> _filter_run_end_type;

  REExREEFilterTest() {
    _value_run_end_type = TypeTraits<ValueRunEndType>::type_singleton();
    _filter_run_end_type = TypeTraits<FilterRunEndType>::type_singleton();
  }

  void AssertOutputSize(const std::shared_ptr<DataType>& value_type,
                        const std::string& values_json, const std::string& filter_json,
                        std::pair<int64_t, int64_t> expected_output_size_drop_nulls,
                        std::pair<int64_t, int64_t> expected_output_size_emit_nulls = {
                            -1, -1}) {
    ASSERT_OK_AND_ASSIGN(auto values,
                         REEFromJson(_value_run_end_type, value_type, values_json));
    ASSERT_OK_AND_ASSIGN(auto filter,
                         REEFromJson(_filter_run_end_type, boolean(), filter_json));
    DoAssertOutputSize(values, filter, kDropNulls, expected_output_size_drop_nulls.first,
                       expected_output_size_drop_nulls.second);
    if (expected_output_size_emit_nulls == std::pair<int64_t, int64_t>{-1, -1}) {
      expected_output_size_emit_nulls = expected_output_size_drop_nulls;
    }
    DoAssertOutputSize(values, filter, kEmitNulls, expected_output_size_emit_nulls.first,
                       expected_output_size_emit_nulls.second);
  }
};
TYPED_TEST_SUITE_P(REExREEFilterTest);

const std::vector<std::shared_ptr<DataType>> all_types = {
    null(),
    boolean(),
    int8(),
    int16(),
    int32(),
    int64(),
    uint8(),
    uint16(),
    uint32(),
    uint64(),
    float32(),
    float64(),
    decimal128(10, 2),
    decimal256(20, 4),
    time32(TimeUnit::MILLI),
    time64(TimeUnit::NANO),
    timestamp(TimeUnit::MILLI, "UTC"),
    date64(),
    date32(),
    duration(TimeUnit::MILLI),
    day_time_interval(),
    month_day_nano_interval(),
    month_interval(),
    fixed_size_binary(10),
    utf8(),
    large_utf8(),
    binary(),
    large_binary(),
};

TYPED_TEST_P(REExREEFilterTest, SizeOutputWithNulls) {
  const std::string one_null = "[null]";
  const std::string four_nulls = "[null, null, null, null]";
  for (auto& data_type : all_types) {
    // Since all values are null, the physical output size is always 0 or 1 as
    // the filtering process combines all the equal values into a single run.

    this->AssertOutputSize(data_type, "[]", "[]", {0, 0});
    this->AssertOutputSize(data_type, one_null, "[1]", {1, 1});
    this->AssertOutputSize(data_type, one_null, "[0]", {0, 0});
    this->AssertOutputSize(data_type, one_null, "[null]", {0, 0}, {1, 1});

    this->AssertOutputSize(data_type, four_nulls, "[0, 1, 1, 0]", {2, 1});
    this->AssertOutputSize(data_type, four_nulls, "[1, 1, 0, 1]", {3, 1});

    this->AssertOutputSize(data_type, four_nulls, "[null, 0, 1, 0]", {1, 1}, {2, 1});
    this->AssertOutputSize(data_type, four_nulls, "[1, 1, 0, null]", {2, 1}, {3, 1});

    this->AssertOutputSize(data_type, four_nulls, "[0, 0, 1, null]", {1, 1}, {2, 1});
    this->AssertOutputSize(data_type, four_nulls, "[null, 1, 0, 1]", {2, 1}, {3, 1});
    this->AssertOutputSize(data_type, four_nulls, "[null, 1, null, 1]", {2, 1}, {4, 1});

    this->AssertOutputSize(data_type,
                           "[null, null, null, null, null, null, null, null, null, null]",
                           "[null, 1, 0, 1, null, 0, null, 1, 0, null]", {3, 1}, {7, 1});
  }
}

TYPED_TEST_P(REExREEFilterTest, SizeOutputWithBooleans) {
  auto data_type = boolean();
  this->AssertOutputSize(data_type, "[false]", "[1]", {1, 1});
  this->AssertOutputSize(data_type, "[false]", "[0]", {0, 0});
  this->AssertOutputSize(data_type, "[true]", "[1]", {1, 1});
  this->AssertOutputSize(data_type, "[true]", "[0]", {0, 0});
  this->AssertOutputSize(data_type, "[false]", "[null]", {0, 0}, {1, 1});
  this->AssertOutputSize(data_type, "[true]", "[null]", {0, 0}, {1, 1});

  this->AssertOutputSize(data_type, "[true, false, true, false]", "[0, 1, 1, 0]", {2, 2});
  this->AssertOutputSize(data_type, "[false, true, false, true]", "[1, 1, 0, 1]", {3, 2});

  this->AssertOutputSize(data_type, "[true, true, true, false]", "[null, 0, 1, 0]",
                         {1, 1}, {2, 2});
  this->AssertOutputSize(data_type, "[false, true, true, true]", "[1, 1, 0, null]",
                         {2, 2}, {3, 3});

  this->AssertOutputSize(data_type,  // linebreak for alignment
                         "[   1, 0,    0, 1, 1,    1, null, 1, 0, 1, 0]",
                         "[null, 0, null, 1, 0, null,    1, 1, 1, 0, 1]", {5, 4}, {8, 5});
}

REGISTER_TYPED_TEST_SUITE_P(REExREEFilterTest, SizeOutputWithNulls,
                            SizeOutputWithBooleans);

template <typename V, typename F>
struct RunEndTypes {
  using ValueRunEndType = V;
  using FilterRunEndType = V;
};

using RunEndTypePairs =
    testing::Types<RunEndTypes<Int16Type, Int16Type>, RunEndTypes<Int16Type, Int32Type>,
                   RunEndTypes<Int16Type, Int64Type>, RunEndTypes<Int32Type, Int16Type>,
                   RunEndTypes<Int32Type, Int32Type>, RunEndTypes<Int32Type, Int64Type>,
                   RunEndTypes<Int64Type, Int16Type>, RunEndTypes<Int64Type, Int32Type>,
                   RunEndTypes<Int64Type, Int64Type>>;
INSTANTIATE_TYPED_TEST_SUITE_P(REExREEFilterTest, REExREEFilterTest, RunEndTypePairs);

}  // namespace compute
}  // namespace arrow
