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
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

void CheckScalarUnary(std::string func_name, std::shared_ptr<DataType> in_ty,
                      std::string json_input, std::shared_ptr<DataType> out_ty,
                      std::string json_expected, const FunctionOptions* options) {
  auto input = ArrayFromJSON(in_ty, json_input);
  auto expected = ArrayFromJSON(out_ty, json_expected);
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {input}, options));
  AssertArraysEqual(*expected, *out.make_array(), /*verbose=*/true);

  // Check all the scalars
  for (int64_t i = 0; i < input->length(); ++i) {
    ASSERT_OK_AND_ASSIGN(auto val, input->GetScalar(i));
    ASSERT_OK_AND_ASSIGN(auto ex_val, expected->GetScalar(i));
    ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {val}, options));
    AssertScalarsEqual(*ex_val, *out.scalar(), /*verbose=*/true);
  }
}

}  // namespace compute
}  // namespace arrow
