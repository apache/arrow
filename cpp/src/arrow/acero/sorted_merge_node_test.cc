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
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/test_nodes.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/ordering.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/scanner.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace arrow::acero {

std::shared_ptr<Table> TestTable(int start, int step, int rows_per_batch,
                                 int num_batches) {
  return gen::Gen({{"timestamp", gen::Step(start, step)}, {"str", gen::Random(utf8())}})
      ->FailOnError()
      ->Table(rows_per_batch, num_batches);
}

TEST(SortedMergeNode, Basic) {
  auto table1 = TestTable(
      /*start=*/0,
      /*step=*/2,
      /*rows_per_batch=*/2,
      /*num_batches=*/3);
  auto table2 = TestTable(
      /*start=*/1,
      /*step=*/2,
      /*rows_per_batch=*/3,
      /*num_batches=*/2);
  auto table3 = TestTable(
      /*start=*/3,
      /*step=*/3,
      /*rows_per_batch=*/6,
      /*num_batches=*/1);
  std::vector<Declaration::Input> src_decls;
  src_decls.emplace_back(Declaration("table_source", TableSourceNodeOptions(table1)));
  src_decls.emplace_back(Declaration("table_source", TableSourceNodeOptions(table2)));
  src_decls.emplace_back(Declaration("table_source", TableSourceNodeOptions(table3)));

  auto ops = OrderByNodeOptions(compute::Ordering({compute::SortKey("timestamp")}));

  Declaration sorted_merge{"sorted_merge", src_decls, ops};
  // We can't use threads for sorted merging since it relies on
  // ascending deterministic order of timestamps
  ASSERT_OK_AND_ASSIGN(auto output,
                       DeclarationToTable(sorted_merge, /*use_threads=*/false));
  ASSERT_EQ(output->num_rows(), 18);

  ASSERT_OK_AND_ASSIGN(auto expected_ts_builder,
                       MakeBuilder(int32(), default_memory_pool()));
  for (auto i : {0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 12, 15, 18}) {
    ASSERT_OK(expected_ts_builder->AppendScalar(*MakeScalar(i)));
  }
  ASSERT_OK_AND_ASSIGN(auto expected_ts, expected_ts_builder->Finish());
  auto output_col = output->column(0);
  ASSERT_OK_AND_ASSIGN(auto output_ts, Concatenate(output_col->chunks()));

  AssertArraysEqual(*expected_ts, *output_ts);
}

TEST(SortedMergeNode, TestSortedMergeTwoInputsWithBool) {
  const int64_t row_count = (16 << 10);  // 16k rows per input

  // Create schema with int column A and bool column B
  auto test_schema = arrow::schema(
      {arrow::field("col_a", arrow::int32()), arrow::field("col_b", arrow::boolean())});

  // Helper lambda to create table with specific pattern
  auto create_test_scanner = [&](int64_t cnt, int offset) -> arrow::Result<Declaration> {
    // Create column A (int) - values from offset to offset+cnt-1
    arrow::Int32Builder col_a_builder;
    std::vector<int32_t> col_a_values;
    col_a_values.reserve(cnt);
    for (int64_t i = 0; i < cnt; ++i) {
      col_a_values.push_back(static_cast<int32_t>(offset + i));
    }
    ARROW_RETURN_NOT_OK(col_a_builder.AppendValues(col_a_values));
    std::shared_ptr<arrow::Array> col_a_arr;
    ARROW_RETURN_NOT_OK(col_a_builder.Finish(&col_a_arr));

    // Create column B (bool) - pattern: true if col_a % 5 == 0, false otherwise
    arrow::BooleanBuilder col_b_builder;
    for (int64_t i = 0; i < cnt; ++i) {
      int32_t a_value = offset + i;
      bool b_value = (a_value % 5 == 0);
      ARROW_RETURN_NOT_OK(col_b_builder.Append(b_value));
    }
    std::shared_ptr<arrow::Array> col_b_arr;
    ARROW_RETURN_NOT_OK(col_b_builder.Finish(&col_b_arr));

    auto table = arrow::Table::Make(test_schema, {col_a_arr, col_b_arr});
    auto table_source =
        Declaration("table_source", TableSourceNodeOptions(table, row_count / 16));
    return table_source;
  };

  ASSERT_OK_AND_ASSIGN(auto source1, create_test_scanner(row_count, 0));
  ASSERT_OK_AND_ASSIGN(auto source2, create_test_scanner(row_count, 8192));

  // Create sorted merge by column A
  auto ops = OrderByNodeOptions(compute::Ordering({compute::SortKey("col_a")}));
  Declaration sorted_merge{"sorted_merge", {source1, source2}, ops};

  // Execute plan and collect result
  ASSERT_OK_AND_ASSIGN(auto result_table,
                       arrow::acero::DeclarationToTable(sorted_merge, false));

  ASSERT_TRUE(result_table != nullptr);

  // Verify results
  auto col_a = result_table->GetColumnByName("col_a");
  auto col_b = result_table->GetColumnByName("col_b");
  ASSERT_TRUE(col_a != nullptr);
  ASSERT_TRUE(col_b != nullptr);

  // Verify sorting and bool values
  int32_t last_a_value = std::numeric_limits<int32_t>::min();
  int64_t total_rows_checked = 0;
  int64_t true_count = 0;
  int64_t false_count = 0;

  for (int i = 0; i < col_a->num_chunks(); i++) {
    auto a_chunk = std::static_pointer_cast<arrow::Int32Array>(col_a->chunk(i));
    auto b_chunk = std::static_pointer_cast<arrow::BooleanArray>(col_b->chunk(i));

    ASSERT_EQ(a_chunk->length(), b_chunk->length())
        << "Column A and B must have same length in chunk " << i;

    for (int64_t j = 0; j < a_chunk->length(); j++) {
      ASSERT_FALSE(a_chunk->IsNull(j)) << "Column A should not have null values";
      ASSERT_FALSE(b_chunk->IsNull(j)) << "Column B should not have null values";

      int32_t a_value = a_chunk->Value(j);
      bool b_value = b_chunk->Value(j);

      // Verify sorting by column A
      ASSERT_GE(a_value, last_a_value)
          << "Values not sorted at chunk " << i << ", row " << j
          << ": current=" << a_value << ", last=" << last_a_value;
      last_a_value = a_value;

      // Verify bool value correctness: should be true if a_value % 3 == 0
      bool expected_b_value = (a_value % 5 == 0);
      ASSERT_EQ(b_value, expected_b_value)
          << "Bool value incorrect at chunk " << i << ", row " << j
          << ": col_a=" << a_value << ", col_b=" << b_value
          << ", expected=" << expected_b_value;

      if (b_value) {
        true_count++;
      } else {
        false_count++;
      }
      total_rows_checked++;
    }
  }

  ASSERT_EQ(last_a_value, 24575);

  ASSERT_EQ(total_rows_checked, row_count * 2)
      << "Expected " << row_count << " unique rows after merge";
}

}  // namespace arrow::acero
