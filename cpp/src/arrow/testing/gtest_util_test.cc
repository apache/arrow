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

#include "arrow/array.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

// Test basic cases for contains NaN.
class TestAssertContainsNaN : public ::testing::Test {};

TEST_F(TestAssertContainsNaN, BatchesEqual) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });

  auto expected = RecordBatchFromJSON(schema,
                                      R"([{"a": 3,    "b": 5},
                                       {"a": 1,    "b": 3},
                                       {"a": 3,    "b": 4},
                                       {"a": NaN,  "b": 6},
                                       {"a": 2,    "b": 5},
                                       {"a": 1,    "b": NaN},
                                       {"a": 1,    "b": 3}
                                       ])");
  auto actual = RecordBatchFromJSON(schema,
                                    R"([{"a": 3,    "b": 5},
                                       {"a": 1,    "b": 3},
                                       {"a": 3,    "b": 4},
                                       {"a": NaN,  "b": 6},
                                       {"a": 2,    "b": 5},
                                       {"a": 1,    "b": NaN},
                                       {"a": 1,    "b": 3}
                                       ])");
  ASSERT_BATCHES_EQUAL(*expected, *actual);
  AssertBatchesApproxEqual(*expected, *actual);
}

TEST_F(TestAssertContainsNaN, TableEqual) {
  auto schema = ::arrow::schema({
      {field("a", float32())},
      {field("b", float64())},
  });

  auto expected = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": NaN,    "b": 3},
                                     {"a": 3,    "b": null}
                                    ])",
                                         R"([{"a": null, "b": null},
                                     {"a": 2,    "b": NaN},
                                     {"a": 1,    "b": 5},
                                     {"a": 3,    "b": 5}
                                    ])"});
  auto actual = TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": NaN,    "b": 3},
                                     {"a": 3,    "b": null}
                                    ])",
                                       R"([{"a": null, "b": null},
                                     {"a": 2,    "b": NaN},
                                     {"a": 1,    "b": 5},
                                     {"a": 3,    "b": 5}
                                    ])"});
  ASSERT_TABLES_EQUAL(*expected, *actual);
}

TEST_F(TestAssertContainsNaN, ArrayEqual) {
  auto expected = ArrayFromJSON(float64(), "[0, 1, 2, NaN]");
  auto actual = ArrayFromJSON(float64(), "[0, 1, 2, NaN]");
  AssertArraysEqual(*expected, *actual);
}

TEST_F(TestAssertContainsNaN, ChunkedEqual) {
  auto expected = ChunkedArrayFromJSON(float64(), {
                                                      "[null, 1]",
                                                      "[3, NaN, 2]",
                                                      "[NaN]",
                                                  });

  auto actual = ChunkedArrayFromJSON(float64(), {
                                                    "[null, 1]",
                                                    "[3, NaN, 2]",
                                                    "[NaN]",
                                                });
  AssertChunkedEqual(*expected, *actual);
}

TEST_F(TestAssertContainsNaN, DatumEqual) {
  // scalar
  auto expected_scalar = ScalarFromJSON(float64(), "NaN");
  auto actual_scalar = ScalarFromJSON(float64(), "NaN");
  AssertDatumsEqual(expected_scalar, actual_scalar);

  // array
  auto expected_array = ArrayFromJSON(float64(), "[3, NaN, 2, 1, 5]");
  auto actual_array = ArrayFromJSON(float64(), "[3, NaN, 2, 1, 5]");
  AssertDatumsEqual(expected_array, actual_array);

  // chunked array
  auto expected_chunked = ChunkedArrayFromJSON(float64(), {
                                                              "[null, 1]",
                                                              "[3, NaN, 2]",
                                                              "[NaN]",
                                                          });

  auto actual_chunked = ChunkedArrayFromJSON(float64(), {
                                                            "[null, 1]",
                                                            "[3, NaN, 2]",
                                                            "[NaN]",
                                                        });
  AssertDatumsEqual(expected_chunked, actual_chunked);
}

}  // namespace arrow
