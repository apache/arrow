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

void CheckScalarUnaryNonRecursive(const std::string& func_name,
                                  const std::shared_ptr<Array>& input,
                                  const std::shared_ptr<Array>& expected,
                                  const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {input}, options));
  std::shared_ptr<Array> actual = std::move(out).make_array();
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

}  // namespace

void CheckScalarUnary(std::string func_name, std::shared_ptr<Array> input,
                      std::shared_ptr<Array> expected, const FunctionOptions* options) {
  CheckScalarUnaryNonRecursive(func_name, input, expected, options);

  // Check all the input scalars
  for (int64_t i = 0; i < input->length(); ++i) {
    ASSERT_OK_AND_ASSIGN(auto val, input->GetScalar(i));
    ASSERT_OK_AND_ASSIGN(auto ex_val, expected->GetScalar(i));
    CheckScalarUnary(func_name, val, ex_val, options);
  }

  const auto slice_length = input->length() / 3;
  // Since it's a scalar function, calling it on a sliced input should
  // result in the sliced expected output.
  if (slice_length > 0) {
    CheckScalarUnaryNonRecursive(func_name, input->Slice(0, slice_length),
                                 expected->Slice(0, slice_length), options);

    CheckScalarUnaryNonRecursive(func_name, input->Slice(slice_length, slice_length),
                                 expected->Slice(slice_length, slice_length), options);

    CheckScalarUnaryNonRecursive(func_name, input->Slice(2 * slice_length),
                                 expected->Slice(2 * slice_length), options);
  }

  // Ditto with a ChunkedArray input
  if (slice_length > 0) {
    ArrayVector input_chunks{input->Slice(0, slice_length), input->Slice(slice_length)},
        expected_chunks{expected->Slice(0, 2 * slice_length),
                        expected->Slice(2 * slice_length)};

    ASSERT_OK_AND_ASSIGN(
        Datum out,
        CallFunction(func_name, {std::make_shared<ChunkedArray>(input_chunks)}, options));
    ASSERT_OK(out.chunked_array()->ValidateFull());
    AssertDatumsEqual(std::make_shared<ChunkedArray>(expected_chunks), out);
  }
}

void CheckScalarUnary(std::string func_name, std::shared_ptr<DataType> in_ty,
                      std::string json_input, std::shared_ptr<DataType> out_ty,
                      std::string json_expected, const FunctionOptions* options) {
  CheckScalarUnary(func_name, ArrayFromJSON(in_ty, json_input),
                   ArrayFromJSON(out_ty, json_expected), options);
}

void CheckScalarUnary(std::string func_name, std::shared_ptr<Scalar> input,
                      std::shared_ptr<Scalar> expected, const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {input}, options));
  AssertScalarsEqual(*expected, *out.scalar(), /*verbose=*/true);
}

void CheckVectorUnary(std::string func_name, Datum input, std::shared_ptr<Array> expected,
                      const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {input}, options));
  std::shared_ptr<Array> actual = std::move(out).make_array();
  ASSERT_OK(actual->ValidateFull());
  AssertArraysEqual(*expected, *actual, /*verbose=*/true);
}

}  // namespace compute
}  // namespace arrow
