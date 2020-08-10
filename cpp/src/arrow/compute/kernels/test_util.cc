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

#include "arrow/compute/kernels/test_util.h"

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

namespace {

template <typename T>
std::vector<Datum> GetDatums(const std::vector<T>& inputs) {
  std::vector<Datum> datums;
  for (const auto& input : inputs) {
    datums.emplace_back(input);
  }
  return datums;
}

void CheckScalarNonRecursive(const std::string& func_name, const ArrayVector& inputs,
                             const std::shared_ptr<Array>& expected,
                             const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, GetDatums(inputs), options));
  std::shared_ptr<Array> actual = std::move(out).make_array();
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

template <typename... SliceArgs>
ArrayVector SliceAll(const ArrayVector& inputs, SliceArgs... slice_args) {
  ArrayVector sliced;
  for (const auto& input : inputs) {
    sliced.push_back(input->Slice(slice_args...));
  }
  return sliced;
}

ScalarVector GetScalars(const ArrayVector& inputs, int64_t index) {
  ScalarVector scalars;
  for (const auto& input : inputs) {
    scalars.push_back(*input->GetScalar(index));
  }
  return scalars;
}

void CheckScalar(std::string func_name, const ScalarVector& inputs,
                 std::shared_ptr<Scalar> expected, const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, GetDatums(inputs), options));
  AssertScalarsEqual(*expected, *out.scalar(), /*verbose=*/true);
}

void CheckScalar(std::string func_name, const ArrayVector& inputs,
                 std::shared_ptr<Array> expected, const FunctionOptions* options) {
  CheckScalarNonRecursive(func_name, inputs, expected, options);

  // Check all the input scalars
  for (int64_t i = 0; i < inputs[0]->length(); ++i) {
    CheckScalar(func_name, GetScalars(inputs, i), *expected->GetScalar(i), options);
  }

  // Since it's a scalar function, calling it on sliced inputs should
  // result in the sliced expected output.
  const auto slice_length = inputs[0]->length() / 3;
  if (slice_length > 0) {
    CheckScalarNonRecursive(func_name, SliceAll(inputs, 0, slice_length),
                            expected->Slice(0, slice_length), options);

    CheckScalarNonRecursive(func_name, SliceAll(inputs, slice_length, slice_length),
                            expected->Slice(slice_length, slice_length), options);

    CheckScalarNonRecursive(func_name, SliceAll(inputs, 2 * slice_length),
                            expected->Slice(2 * slice_length), options);
  }

  // Ditto with ChunkedArray inputs
  if (slice_length > 0) {
    std::vector<std::shared_ptr<ChunkedArray>> chunked_inputs;
    chunked_inputs.reserve(inputs.size());
    for (const auto& input : inputs) {
      chunked_inputs.push_back(std::make_shared<ChunkedArray>(
          ArrayVector{input->Slice(0, slice_length), input->Slice(slice_length)}));
    }
    ArrayVector expected_chunks{expected->Slice(0, slice_length),
                                expected->Slice(slice_length)};

    ASSERT_OK_AND_ASSIGN(Datum out,
                         CallFunction(func_name, GetDatums(chunked_inputs), options));
    ASSERT_OK(out.chunked_array()->ValidateFull());
    AssertDatumsEqual(std::make_shared<ChunkedArray>(expected_chunks), out);
  }
}

}  // namespace

void CheckScalarUnary(std::string func_name, std::shared_ptr<Array> input,
                      std::shared_ptr<Array> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {input}, expected, options);
}

void CheckScalarUnary(std::string func_name, std::shared_ptr<DataType> in_ty,
                      std::string json_input, std::shared_ptr<DataType> out_ty,
                      std::string json_expected, const FunctionOptions* options) {
  CheckScalarUnary(std::move(func_name), ArrayFromJSON(in_ty, json_input),
                   ArrayFromJSON(out_ty, json_expected), options);
}

void CheckScalarUnary(std::string func_name, std::shared_ptr<Scalar> input,
                      std::shared_ptr<Scalar> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {input}, expected, options);
}

void CheckVectorUnary(std::string func_name, Datum input, std::shared_ptr<Array> expected,
                      const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {input}, options));
  std::shared_ptr<Array> actual = std::move(out).make_array();
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

void CheckScalarBinary(std::string func_name, std::shared_ptr<Scalar> left_input,
                       std::shared_ptr<Scalar> right_input,
                       std::shared_ptr<Scalar> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {left_input, right_input}, expected, options);
}

void CheckScalarBinary(std::string func_name, std::shared_ptr<Array> left_input,
                       std::shared_ptr<Array> right_input,
                       std::shared_ptr<Array> expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {left_input, right_input}, expected, options);
}

}  // namespace compute
}  // namespace arrow
