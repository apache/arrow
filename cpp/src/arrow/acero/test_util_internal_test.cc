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

#include "arrow/acero/test_util_internal.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::acero {

TEST(RunEndEncodeTableColumnsTest, SchemaTypeIsModified) {
  std::shared_ptr<Table> table =
      arrow::TableFromJSON(arrow::schema({arrow::field("col", arrow::utf8())}), {R"([
            {"col": "a"},
            {"col": "b"},
            {"col": "c"},
            {"col": "d"}
          ])"});
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> ree_table,
                       RunEndEncodeTableColumns(*table, {0}));
  ASSERT_OK(ree_table->ValidateFull());
  ASSERT_TRUE(ree_table->schema()->field(0)->type()->Equals(
      arrow::run_end_encoded(arrow::int32(), arrow::utf8())));
}
}  // namespace arrow::acero
