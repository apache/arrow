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

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/map_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/test_nodes.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/ordering.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace arrow::acero {

std::shared_ptr<Table> TestTable(int start, int step, int rows_per_batch,
                                 int num_batches) {
  return gen::Gen({{"timestamp", gen::Step(start, step, /*signed_int=*/true)},
                   {"str", gen::Random(utf8())}})
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

}  // namespace arrow::acero
