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

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/chunked_array.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace compute {

void CheckBooleanScalarArrayBinary(std::string func_name, Datum array) {
  for (std::shared_ptr<Scalar> scalar :
       {std::make_shared<BooleanScalar>(), std::make_shared<BooleanScalar>(true),
        std::make_shared<BooleanScalar>(false)}) {
    ASSERT_OK_AND_ASSIGN(auto constant_array,
                         MakeArrayFromScalar(*scalar, array.length()));

    ASSERT_OK_AND_ASSIGN(Datum expected,
                         CallFunction(func_name, {Datum(constant_array), array}));
    CheckScalar(func_name, {scalar, array}, expected);

    ASSERT_OK_AND_ASSIGN(expected,
                         CallFunction(func_name, {array, Datum(constant_array)}));
    CheckScalar(func_name, {array, scalar}, expected);
  }
}

TEST(TestBooleanKernel, Invert) {
  auto arr =
      ArrayFromJSON(boolean(), "[true, false, true, null, false, true, false, null]");
  auto expected =
      ArrayFromJSON(boolean(), "[false, true, false, null, true, false, true, null]");
  CheckScalarUnary("invert", arr, expected);
}

TEST(TestBooleanKernel, And) {
  auto left = ArrayFromJSON(boolean(), "    [true, true,  true, false, false, null]");
  auto right = ArrayFromJSON(boolean(), "   [true, false, null, false, null,  null]");
  auto expected = ArrayFromJSON(boolean(), "[true, false, null, false, null,  null]");
  CheckScalarBinary("and", left, right, expected);
  CheckBooleanScalarArrayBinary("and", left);
}

TEST(TestBooleanKernel, Or) {
  auto left = ArrayFromJSON(boolean(), "    [true, true,  true, false, false, null]");
  auto right = ArrayFromJSON(boolean(), "   [true, false, null, false, null,  null]");
  auto expected = ArrayFromJSON(boolean(), "[true, true,  null, false, null,  null]");
  CheckScalarBinary("or", left, right, expected);
  CheckBooleanScalarArrayBinary("or", left);
}

TEST(TestBooleanKernel, Xor) {
  auto left = ArrayFromJSON(boolean(), "    [true,  true,  true, false, false, null]");
  auto right = ArrayFromJSON(boolean(), "   [true,  false, null, false, null,  null]");
  auto expected = ArrayFromJSON(boolean(), "[false, true,  null, false, null,  null]");
  CheckScalarBinary("xor", left, right, expected);
  CheckBooleanScalarArrayBinary("xor", left);
}

TEST(TestBooleanKernel, AndNot) {
  auto left = ArrayFromJSON(
      boolean(), "[true,  true,  true, false, false, false, null, null,  null]");
  auto right = ArrayFromJSON(
      boolean(), "[true,  false, null, true,  false, null,  true, false, null]");
  auto expected = ArrayFromJSON(
      boolean(), "[false, true,  null, false, false, null,  null, null,  null]");
  CheckScalarBinary("and_not", left, right, expected);
  CheckBooleanScalarArrayBinary("and_not", left);
}

TEST(TestBooleanKernel, KleeneAnd) {
  auto left = ArrayFromJSON(boolean(), "    [true, true,  true, false, false, null]");
  auto right = ArrayFromJSON(boolean(), "   [true, false, null, false, null,  null]");
  auto expected = ArrayFromJSON(boolean(), "[true, false, null, false, false, null]");
  CheckScalarBinary("and_kleene", left, right, expected);
  CheckBooleanScalarArrayBinary("and_kleene", left);

  left = ArrayFromJSON(boolean(), "    [true, true,  false, null, null]");
  right = ArrayFromJSON(boolean(), "   [true, false, false, true, false]");
  expected = ArrayFromJSON(boolean(), "[true, false, false, null, false]");
  CheckScalarBinary("and_kleene", left, right, expected);
  CheckBooleanScalarArrayBinary("and_kleene", left);

  left = ArrayFromJSON(boolean(), "    [true, true,  false, true]");
  right = ArrayFromJSON(boolean(), "   [true, false, false, false]");
  expected = ArrayFromJSON(boolean(), "[true, false, false, false]");
  CheckScalarBinary("and_kleene", left, right, expected);
  CheckBooleanScalarArrayBinary("and_kleene", left);
}

TEST(TestBooleanKernel, KleeneAndNot) {
  auto left = ArrayFromJSON(
      boolean(), "[true,  true,  true, false, false, false, null, null,  null]");
  auto right = ArrayFromJSON(
      boolean(), "[true,  false, null, true,  false, null,  true, false, null]");
  auto expected = ArrayFromJSON(
      boolean(), "[false, true,  null, false, false, false, false, null, null]");
  CheckScalarBinary("and_not_kleene", left, right, expected);
  CheckBooleanScalarArrayBinary("and_not_kleene", left);

  left = ArrayFromJSON(boolean(), "    [true,  true,  false, false]");
  right = ArrayFromJSON(boolean(), "   [true,  false, true,  false]");
  expected = ArrayFromJSON(boolean(), "[false, true,  false, false]");
  CheckScalarBinary("and_not_kleene", left, right, expected);
  CheckBooleanScalarArrayBinary("and_not_kleene", left);
}

TEST(TestBooleanKernel, KleeneOr) {
  auto left = ArrayFromJSON(boolean(), "    [true, true,  true, false, false, null]");
  auto right = ArrayFromJSON(boolean(), "   [true, false, null, false, null,  null]");
  auto expected = ArrayFromJSON(boolean(), "[true, true,  true, false, null,  null]");
  CheckScalarBinary("or_kleene", left, right, expected);
  CheckBooleanScalarArrayBinary("or_kleene", left);

  left = ArrayFromJSON(boolean(), "    [true, true,  false, null, null]");
  right = ArrayFromJSON(boolean(), "   [true, false, false, true, false]");
  expected = ArrayFromJSON(boolean(), "[true, true,  false, true, null]");
  CheckScalarBinary("or_kleene", left, right, expected);
  CheckBooleanScalarArrayBinary("or_kleene", left);

  left = ArrayFromJSON(boolean(), "    [true, true,  false, false]");
  right = ArrayFromJSON(boolean(), "   [true, false, false, true]");
  expected = ArrayFromJSON(boolean(), "[true, true,  false, true]");
  CheckScalarBinary("or_kleene", left, right, expected);
  CheckBooleanScalarArrayBinary("or_kleene", left);
}

}  // namespace compute
}  // namespace arrow
