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

#include <arrow/testing/builder.h>
#include <gtest/gtest.h>

#include <gmock/gmock-matchers.h>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/test_nodes.h"
#include "arrow/compute/ordering.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace acero {

auto table_schema = schema(FieldVector({field("id", int64())}));

void CheckAssert(const std::shared_ptr<ChunkedArray>& array,
                 const std::shared_ptr<Schema>& schema, const SortOptions& sort_options,
                 const Status& expected_status = Status::OK()) {
  // count empty chunks at the end of the table
  int empty_pad_chunks = 0;
  for (auto it = array->chunks().rbegin(); it != array->chunks().rend(); ++it) {
    if (it->get()->length() == 0) {
      ++empty_pad_chunks;
    } else {
      break;
    }
  }
  // remove empty chunks from array to construct expected array
  auto expected_chunks =
      ArrayVector(array->chunks().begin(), array->chunks().end() - empty_pad_chunks);
  auto expected_array = ChunkedArray::Make(expected_chunks, array->type()).ValueOrDie();

  auto table = Table::Make(schema, {array});
  auto expected = Table::Make(schema, {expected_array});

  constexpr random::SeedType kSeed = 42;
  constexpr int kJitterMod = 4;
  RegisterTestNodes();

  std::shared_ptr<ExecNodeOptions> assert_options =
      std::make_shared<AssertOrderNodeOptions>(sort_options);
  Declaration plan =
      Declaration::Sequence({{"table_source", TableSourceNodeOptions(table)},
                             {"jitter", JitterNodeOptions(kSeed, kJitterMod)},
                             {"assert-order", assert_options}});

  for (bool use_threads : {false, true}) {
    QueryOptions query_options;
    query_options.use_threads = use_threads;

    if (expected_status.ok()) {
      // assert actual output table is identical to input table
      ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> actual,
                           DeclarationToTable(plan, query_options));
      AssertTablesEqual(*expected, *actual);
    } else {
      auto result = DeclarationToTable(plan, query_options);
      ASSERT_NOT_OK(result);
      const auto& actual_status = result.status();
      ASSERT_EQ(actual_status.code(), expected_status.code());
      ASSERT_EQ(actual_status.message(), expected_status.message());
    }
  }
}

void CheckAssert(const std::shared_ptr<ChunkedArray>& array,
                 const SortOptions& sort_options,
                 const Status& expected_status = Status::OK()) {
  CheckAssert(array, table_schema, sort_options, expected_status);
}

TEST(AssertOrderNode, SingleColumnAsc) {
  const auto monotonic_ids =
      ChunkedArrayFromJSON(int64(), {"[1, 2, 3]", "[4, 5, 6]", "[7, 8, 9]"});
  const auto repeating_ids =
      ChunkedArrayFromJSON(int64(), {"[1, 2, 2]", "[3, 3, 3]", "[7, 7, 9]"});
  const auto identical_ids =
      ChunkedArrayFromJSON(int64(), {"[3, 3, 3]", "[3, 3, 3]", "[3, 3, 3]"});
  const auto some_negative_ids =
      ChunkedArrayFromJSON(int64(), {"[-5, -4, -3]", "[-2, 0, 2]", "[3, 5, 6]"});
  const auto all_negative_ids =
      ChunkedArrayFromJSON(int64(), {"[-9, -8, -7]", "[-7, -6, -6]", "[-6, -5, -1]"});

  const auto all_empty_batches = ChunkedArrayFromJSON(int64(), {"[]", "[]", "[]"});
  const auto many_empty_batches = ChunkedArrayFromJSON(
      int64(),
      {"[]", "[]", "[1, 2, 3]", "[]", "[]", "[4, 5, 6]", "[]", "[7, 8, 9]", "[]", "[]"});

  const auto unordered_batch =
      ChunkedArrayFromJSON(int64(), {"[1, 2, 3]", "[4, 6, 5]", "[7, 8, 9]"});
  const auto unordered_batches =
      ChunkedArrayFromJSON(int64(), {"[1, 2, 3]", "[7, 8, 9]", "[4, 5, 6]"});

  // TODO: add tests with NULL and NaN values

  auto asc_sort_options =
      SortOptions({compute::SortKey{"id", compute::SortOrder::Ascending}});

  CheckAssert(monotonic_ids, asc_sort_options);
  CheckAssert(repeating_ids, asc_sort_options);
  CheckAssert(identical_ids, asc_sort_options);
  CheckAssert(some_negative_ids, asc_sort_options);
  CheckAssert(all_negative_ids, asc_sort_options);

  CheckAssert(all_empty_batches, asc_sort_options);
  CheckAssert(many_empty_batches, asc_sort_options);

  CheckAssert(unordered_batch, asc_sort_options,
              Status::ExecutionError("Data is not ordered"));
  CheckAssert(unordered_batches, asc_sort_options,
              Status::ExecutionError("Data is not ordered"));
}

TEST(AssertOrderNode, SingleColumnDesc) {
  const auto monotonic_ids =
      ChunkedArrayFromJSON(int64(), {"[9, 8, 7]", "[6, 5, 4]", "[3, 2, 1]"});
  const auto repeating_ids =
      ChunkedArrayFromJSON(int64(), {"[9, 7, 7]", "[5, 5, 5]", "[3, 3, 1]"});
  const auto identical_ids =
      ChunkedArrayFromJSON(int64(), {"[3, 3, 3]", "[3, 3, 3]", "[3, 3, 3]"});
  const auto some_negative_ids =
      ChunkedArrayFromJSON(int64(), {"[5, 4, 3]", "[2, 0, -2]", "[-3, -5, -6]"});
  const auto all_negative_ids =
      ChunkedArrayFromJSON(int64(), {"[-1, -2, -3]", "[-4, -5, -6]", "[-7, -8, -9]"});

  const auto all_empty_batches = ChunkedArrayFromJSON(int64(), {"[]", "[]", "[]"});
  const auto many_empty_batches = ChunkedArrayFromJSON(
      int64(),
      {"[]", "[]", "[9, 8, 7]", "[]", "[]", "[6, 5, 4]", "[]", "[3, 2, 1]", "[]", "[]"});

  const auto unordered_batch =
      ChunkedArrayFromJSON(int64(), {"[9, 8, 7]", "[6, 4, 5]", "[3, 2, 1]"});
  const auto unordered_batches =
      ChunkedArrayFromJSON(int64(), {"[9, 8, 7]", "[3, 2, 1]", "[6, 5, 4]"});

  // TODO: add tests with NULL and NaN values

  auto desc_sort_options =
      SortOptions({compute::SortKey{"id", compute::SortOrder::Descending}});

  CheckAssert(monotonic_ids, desc_sort_options);
  CheckAssert(repeating_ids, desc_sort_options);
  CheckAssert(identical_ids, desc_sort_options);
  CheckAssert(some_negative_ids, desc_sort_options);
  CheckAssert(all_negative_ids, desc_sort_options);

  CheckAssert(all_empty_batches, desc_sort_options);
  CheckAssert(many_empty_batches, desc_sort_options);

  CheckAssert(unordered_batch, desc_sort_options,
              Status::ExecutionError("Data is not ordered"));
  CheckAssert(unordered_batches, desc_sort_options,
              Status::ExecutionError("Data is not ordered"));
}

// TODO: add tests with multiple columns

}  // namespace acero
}  // namespace arrow
