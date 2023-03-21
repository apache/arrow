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

namespace arrow {

using internal::checked_cast;

namespace compute {

const auto kEmitNulls = FilterOptions{FilterOptions::EMIT_NULL};
const auto kDropNulls = FilterOptions{FilterOptions::DROP};

template <typename RunEndTypes>
struct CalculateREExREEFilterOutputSizeTest : public ::testing::Test {
  using ValueRunEndType = typename RunEndTypes::ValueRunEndType;
  using FilterRunEndType = typename RunEndTypes::FilterRunEndType;

  std::shared_ptr<DataType> _value_run_end_type;
  std::shared_ptr<DataType> _filter_run_end_type;

  CalculateREExREEFilterOutputSizeTest() {
    _value_run_end_type = TypeTraits<ValueRunEndType>::type_singleton();
    _filter_run_end_type = TypeTraits<FilterRunEndType>::type_singleton();
  }

  Result<std::shared_ptr<Array>> REEFromJson(
      const std::shared_ptr<DataType>& run_end_type,
      const std::shared_ptr<DataType>& value_type, const std::string& json) {
    auto array = ArrayFromJSON(value_type, json);
    ARROW_ASSIGN_OR_RAISE(auto datum,
                          RunEndEncode(array, RunEndEncodeOptions{_value_run_end_type}));
    return datum.make_array();
  }

  /// \param expected_output_size The expected physical size of the output REE array
  void DoAssertFilterOutputSize(const std::shared_ptr<Array>& values,
                                const std::shared_ptr<Array>& filter,
                                const FilterOptions& null_options,
                                int64_t expected_output_size) {
    auto values_span = ArraySpan(*values->data());
    auto filter_span = ArraySpan(*filter->data());
    const auto actual_output_size = internal::CalculateREExREEFilterOutputSize(
        values_span, filter_span, null_options.null_selection_behavior);
    ASSERT_EQ(actual_output_size, expected_output_size);
  }

  void AssertFilterSlicedOutputSize(const std::shared_ptr<Array>& values,
                                    const std::shared_ptr<Array>& filter,
                                    const FilterOptions& null_options,
                                    int64_t expected_output_size) {
    // Check slicing: add M(=3) dummy values at the start and end of `values`,
    // add N(=2) dummy values at the start and end of `filter`.
    ARROW_SCOPED_TRACE("for sliced values and filter");
    ASSERT_OK_AND_ASSIGN(auto values_filler, MakeArrayOfNull(values->type(), 3));
    ASSERT_OK_AND_ASSIGN(auto filter_filler,
                         REEFromJson(_filter_run_end_type, boolean(), "[true, false]"));
    ASSERT_OK_AND_ASSIGN(auto values_sliced,
                         Concatenate({values_filler, values, values_filler}));
    ASSERT_OK_AND_ASSIGN(auto filter_sliced,
                         Concatenate({filter_filler, filter, filter_filler}));
    values_sliced = values_sliced->Slice(3, values->length());
    filter_sliced = filter_sliced->Slice(2, filter->length());
    DoAssertFilterOutputSize(values_sliced, filter_sliced, null_options,
                             expected_output_size);
  }

  void AssertOutputSize(const std::shared_ptr<Array>& values,
                        const std::shared_ptr<Array>& filter,
                        const FilterOptions& null_options, int64_t expected_output_size) {
    ARROW_SCOPED_TRACE("assert output size");
    {
      ARROW_SCOPED_TRACE("for full values and filter");
      DoAssertFilterOutputSize(values, filter, null_options, expected_output_size);
    }
    AssertFilterSlicedOutputSize(values, filter, null_options, expected_output_size);
  }

  void AssertOutputSize(const std::shared_ptr<DataType>& value_type,
                        const std::string& values_json, const std::string& filter_json,
                        int64_t expected_output_size_emit_nulls,
                        int64_t expected_output_size_drop_nulls = -1) {
    if (expected_output_size_drop_nulls == -1) {
      expected_output_size_drop_nulls = expected_output_size_emit_nulls;
    }
    ASSERT_OK_AND_ASSIGN(auto values,
                         REEFromJson(_value_run_end_type, value_type, values_json));
    ASSERT_OK_AND_ASSIGN(auto filter,
                         REEFromJson(_filter_run_end_type, boolean(), filter_json));
    AssertOutputSize(values, filter, kEmitNulls, expected_output_size_emit_nulls);
    AssertOutputSize(values, filter, kDropNulls, expected_output_size_drop_nulls);
  }
};
TYPED_TEST_SUITE_P(CalculateREExREEFilterOutputSizeTest);

TYPED_TEST_P(CalculateREExREEFilterOutputSizeTest, SizeOutputWithNulls) {
  const std::string one_null = "[null]";
  const std::string four_nulls = "[null, null, null, null]";
  for (auto& data_type : {boolean(), null()}) {
    // Since all values are null, the physical output size is always 0 or 1 as
    // the filtering process combines all the equal values into a single run.

    this->AssertOutputSize(data_type, "[]", "[]", 0);
    this->AssertOutputSize(data_type, one_null, "[1]", 1);
    this->AssertOutputSize(data_type, one_null, "[0]", 0);
    this->AssertOutputSize(data_type, one_null, "[null]", 1, 0);

    this->AssertOutputSize(data_type, four_nulls, "[0, 1, 1, 0]", 1);
    this->AssertOutputSize(data_type, four_nulls, "[1, 1, 0, 1]", 1);

    this->AssertOutputSize(data_type, four_nulls, "[null, 0, 1, 0]", 1);
    this->AssertOutputSize(data_type, four_nulls, "[1, 1, 0, null]", 1);

    this->AssertOutputSize(data_type, four_nulls, "[0, 0, 1, null]", 1);
    this->AssertOutputSize(data_type, four_nulls, "[null, 1, 0, 1]", 1);
    this->AssertOutputSize(data_type, four_nulls, "[null, 1, null, 1]", 1);

    this->AssertOutputSize(data_type,
                           "[null, null, null, null, null, null, null, null, null, null]",
                           "[null, 1, 0, 1, null, 0, null, 1, 0, null]", 1);
  }
}

TYPED_TEST_P(CalculateREExREEFilterOutputSizeTest, SizeOutputWithBooleans) {
  auto data_type = boolean();
  this->AssertOutputSize(data_type, "[false]", "[1]", 1);
  this->AssertOutputSize(data_type, "[false]", "[0]", 0);
  this->AssertOutputSize(data_type, "[true]", "[1]", 1);
  this->AssertOutputSize(data_type, "[true]", "[0]", 0);
  this->AssertOutputSize(data_type, "[false]", "[null]", 1, 0);
  this->AssertOutputSize(data_type, "[true]", "[null]", 1, 0);

  this->AssertOutputSize(data_type, "[true, false, true, false]", "[0, 1, 1, 0]", 2);
  this->AssertOutputSize(data_type, "[false, true, false, true]", "[1, 1, 0, 1]", 2);

  this->AssertOutputSize(data_type, "[true, true, true, false]", "[null, 0, 1, 0]", 2, 1);
  this->AssertOutputSize(data_type, "[false, true, true, true]", "[1, 1, 0, null]", 3, 2);

  this->AssertOutputSize(data_type,  // linebreak for alignment
                         "[   1, 0,    0, 1, 1,    1, null, 1, 0, 1, 0]",
                         "[null, 0, null, 1, 0, null,    1, 1, 1, 0, 1]", 5, 4);
}

REGISTER_TYPED_TEST_SUITE_P(CalculateREExREEFilterOutputSizeTest, SizeOutputWithNulls,
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
INSTANTIATE_TYPED_TEST_SUITE_P(CalculateREExREEFilterOutputSizeTest,
                               CalculateREExREEFilterOutputSizeTest, RunEndTypePairs);

}  // namespace compute
}  // namespace arrow
