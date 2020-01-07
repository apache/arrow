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
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/boolean.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/compute/test_util.h"
#include "arrow/table.h"

namespace arrow {
namespace compute {

using BinaryKernelFunc =
    std::function<Status(FunctionContext*, const Datum&, const Datum&, Datum* out)>;

class TestBooleanKernel : public ComputeFixture, public TestBase {
 public:
  void TestArrayBinary(const BinaryKernelFunc& kernel, const std::shared_ptr<Array>& left,
                       const std::shared_ptr<Array>& right,
                       const std::shared_ptr<Array>& expected) {
    Datum result;

    ASSERT_OK(kernel(&this->ctx_, left, right, &result));
    ASSERT_EQ(Datum::ARRAY, result.kind());
    std::shared_ptr<Array> result_array = result.make_array();
    ASSERT_OK(result_array->ValidateFull());
    ASSERT_ARRAYS_EQUAL(*expected, *result_array);

    ASSERT_OK(kernel(&this->ctx_, right, left, &result));
    ASSERT_EQ(Datum::ARRAY, result.kind());
    result_array = result.make_array();
    ASSERT_OK(result_array->ValidateFull());
    ASSERT_ARRAYS_EQUAL(*expected, *result_array);
  }

  void TestChunkedArrayBinary(const BinaryKernelFunc& kernel,
                              const std::shared_ptr<ChunkedArray>& left,
                              const std::shared_ptr<ChunkedArray>& right,
                              const std::shared_ptr<ChunkedArray>& expected) {
    Datum result;

    ASSERT_OK(kernel(&this->ctx_, left, right, &result));
    ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
    std::shared_ptr<ChunkedArray> result_ca = result.chunked_array();
    ASSERT_TRUE(result_ca->Equals(expected));

    ASSERT_OK(kernel(&this->ctx_, right, left, &result));
    ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
    result_ca = result.chunked_array();
    ASSERT_TRUE(result_ca->Equals(expected));
  }

  void TestBinaryKernel(const BinaryKernelFunc& kernel,
                        const std::shared_ptr<Array>& left,
                        const std::shared_ptr<Array>& right,
                        const std::shared_ptr<Array>& expected) {
    TestArrayBinary(kernel, left, right, expected);
    TestArrayBinary(kernel, left->Slice(1), right->Slice(1), expected->Slice(1));

    // ChunkedArray
    auto cleft = std::make_shared<ChunkedArray>(ArrayVector{left, left->Slice(1)});
    auto cright = std::make_shared<ChunkedArray>(ArrayVector{right, right->Slice(1)});
    auto cexpected =
        std::make_shared<ChunkedArray>(ArrayVector{expected, expected->Slice(1)});
    TestChunkedArrayBinary(kernel, cleft, cright, cexpected);

    // ChunkedArray with different chunks
    cleft = std::make_shared<ChunkedArray>(ArrayVector{
        left->Slice(0, 1), left->Slice(1), left->Slice(1, 1), left->Slice(2)});
    TestChunkedArrayBinary(kernel, cleft, cright, cexpected);
  }

  void TestBinaryKernelPropagate(const BinaryKernelFunc& kernel,
                                 const std::vector<bool>& left,
                                 const std::vector<bool>& right,
                                 const std::vector<bool>& expected,
                                 const std::vector<bool>& expected_nulls) {
    auto type = boolean();
    TestBinaryKernel(kernel, _MakeArray<BooleanType, bool>(type, left, {}),
                     _MakeArray<BooleanType, bool>(type, right, {}),
                     _MakeArray<BooleanType, bool>(type, expected, {}));

    TestBinaryKernel(kernel, _MakeArray<BooleanType, bool>(type, left, left),
                     _MakeArray<BooleanType, bool>(type, right, right),
                     _MakeArray<BooleanType, bool>(type, expected, expected_nulls));
  }
};

TEST_F(TestBooleanKernel, Invert) {
  std::vector<bool> values1 = {true, false, true, false};
  std::vector<bool> values2 = {false, true, false, true};

  auto type = boolean();
  auto a1 = _MakeArray<BooleanType, bool>(type, values1, {true, true, true, false});
  auto a2 = _MakeArray<BooleanType, bool>(type, values2, {true, true, true, false});

  // Plain array
  Datum result;
  ASSERT_OK(Invert(&this->ctx_, a1, &result));
  ASSERT_EQ(Datum::ARRAY, result.kind());
  ASSERT_ARRAYS_EQUAL(*a2, *result.make_array());

  // Array with offset
  ASSERT_OK(Invert(&this->ctx_, a1->Slice(1), &result));
  ASSERT_EQ(Datum::ARRAY, result.kind());
  ASSERT_ARRAYS_EQUAL(*a2->Slice(1), *result.make_array());

  // ChunkedArray
  std::vector<std::shared_ptr<Array>> ca1_arrs = {a1, a1->Slice(1)};
  auto ca1 = std::make_shared<ChunkedArray>(ca1_arrs);
  std::vector<std::shared_ptr<Array>> ca2_arrs = {a2, a2->Slice(1)};
  auto ca2 = std::make_shared<ChunkedArray>(ca2_arrs);
  ASSERT_OK(Invert(&this->ctx_, ca1, &result));
  ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
  std::shared_ptr<ChunkedArray> result_ca = result.chunked_array();
  AssertChunkedEqual(*ca2, *result_ca);
}

TEST_F(TestBooleanKernel, InvertEmptyArray) {
  std::vector<std::shared_ptr<Buffer>> data_buffers(2);
  Datum input;
  input.value = ArrayData::Make(boolean(), 0 /* length */, std::move(data_buffers),
                                0 /* null_count */);

  Datum result;
  ASSERT_OK(Invert(&this->ctx_, input, &result));
  ASSERT_ARRAYS_EQUAL(*input.make_array(), *result.make_array());
}

TEST_F(TestBooleanKernel, BinaryOpOnEmptyArray) {
  auto type = boolean();
  std::vector<std::shared_ptr<Buffer>> data_buffers(2);
  Datum input;
  input.value = ArrayData::Make(boolean(), 0 /* length */, std::move(data_buffers),
                                0 /* null_count */);

  Datum result;
  ASSERT_OK(And(&this->ctx_, input, input, &result));
  // Result should be empty as well.
  ASSERT_ARRAYS_EQUAL(*input.make_array(), *result.make_array());
}

TEST_F(TestBooleanKernel, And) {
  std::vector<bool> values1 = {true, false, true, false, true, true};
  std::vector<bool> values2 = {true, true, false, false, true, false};
  std::vector<bool> values3 = {true, false, false, false, true, false};
  TestBinaryKernelPropagate(And, values1, values2, values3, values3);
}

TEST_F(TestBooleanKernel, Or) {
  std::vector<bool> values1 = {true, false, true, false, true, true};
  std::vector<bool> values2 = {true, true, false, false, true, false};
  std::vector<bool> values3 = {true, true, true, false, true, true};
  std::vector<bool> values3_nulls = {true, false, false, false, true, false};
  TestBinaryKernelPropagate(Or, values1, values2, values3, values3_nulls);
}

TEST_F(TestBooleanKernel, Xor) {
  std::vector<bool> values1 = {true, false, true, false, true, true};
  std::vector<bool> values2 = {true, true, false, false, true, false};
  std::vector<bool> values3 = {false, true, true, false, false, true};
  std::vector<bool> values3_nulls = {true, false, false, false, true, false};
  TestBinaryKernelPropagate(Xor, values1, values2, values3, values3_nulls);
}

TEST_F(TestBooleanKernel, KleeneAnd) {
  auto left = ArrayFromJSON(boolean(), "    [true, true,  true, false, false, null]");
  auto right = ArrayFromJSON(boolean(), "   [true, false, null, false, null,  null]");
  auto expected = ArrayFromJSON(boolean(), "[true, false, null, false, false, null]");
  TestBinaryKernel(KleeneAnd, left, right, expected);

  left = ArrayFromJSON(boolean(), "    [true, true,  false, null, null]");
  right = ArrayFromJSON(boolean(), "   [true, false, false, true, false]");
  expected = ArrayFromJSON(boolean(), "[true, false, false, null, false]");
  TestBinaryKernel(KleeneAnd, left, right, expected);
}

TEST_F(TestBooleanKernel, KleeneOr) {
  auto left = ArrayFromJSON(boolean(), "    [true, true,  true, false, false, null]");
  auto right = ArrayFromJSON(boolean(), "   [true, false, null, false, null,  null]");
  auto expected = ArrayFromJSON(boolean(), "[true, true,  true, false, null,  null]");
  TestBinaryKernel(KleeneOr, left, right, expected);

  left = ArrayFromJSON(boolean(), "    [true, true,  false, null, null]");
  right = ArrayFromJSON(boolean(), "   [true, false, false, true, false]");
  expected = ArrayFromJSON(boolean(), "[true, true,  false, true, null]");
  TestBinaryKernel(KleeneOr, left, right, expected);
}

}  // namespace compute
}  // namespace arrow
