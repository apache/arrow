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

#include "arrow/engine/substrait/test_util.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function_internal.h"
#include "arrow/datum.h"
#include "arrow/io/interfaces.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/vector.h"

namespace arrow {

using compute::SortKey;
using compute::SortOptions;
using compute::Take;
using internal::Executor;

namespace engine {

Result<std::shared_ptr<Table>> SortTableOnAllFields(const std::shared_ptr<Table>& tab) {
  std::vector<SortKey> sort_keys;
  for (int i = 0; i < tab->num_columns(); i++) {
    sort_keys.emplace_back(i);
  }
  ARROW_ASSIGN_OR_RAISE(auto sort_ids, SortIndices(tab, SortOptions(sort_keys)));
  ARROW_ASSIGN_OR_RAISE(auto tab_sorted, Take(tab, sort_ids));
  return tab_sorted.table();
}

void AssertTablesEqualIgnoringOrder(const std::shared_ptr<Table>& exp,
                                    const std::shared_ptr<Table>& act) {
  ASSERT_EQ(exp->num_columns(), act->num_columns());
  if (exp->num_rows() == 0) {
    ASSERT_EQ(exp->num_rows(), act->num_rows());
  } else {
    ASSERT_OK_AND_ASSIGN(auto exp_sorted, SortTableOnAllFields(exp));
    ASSERT_OK_AND_ASSIGN(auto act_sorted, SortTableOnAllFields(act));

    AssertTablesEqual(*exp_sorted, *act_sorted,
                      /*same_chunk_layout=*/false, /*flatten=*/true);
  }
}

}  // namespace engine
}  // namespace arrow
