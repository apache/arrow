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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/partition.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

#include "arrow/table.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

namespace acero {

TEST(SinkNode, CustomFieldMetadata) {
  // Create an input table with a nullable and a non-nullable type
  ExecBatch batch = gen::Gen({gen::Step()})->FailOnError()->ExecBatch(/*num_rows=*/1);
  std::shared_ptr<Schema> test_schema =
      schema({field("nullable_i32", uint32(), /*nullable=*/true,
                    key_value_metadata({{"foo", "bar"}})),
              field("non_nullable_i32", uint32(), /*nullable=*/false)});
  std::shared_ptr<RecordBatch> record_batch =
      RecordBatch::Make(test_schema, /*num_rows=*/1,
                        {batch.values[0].make_array(), batch.values[0].make_array()});
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> table,
                       Table::FromRecordBatches({std::move(record_batch)}));

  ASSERT_TRUE(table->field(0)->nullable());
  ASSERT_EQ(1, table->field(0)->metadata()->keys().size());
  ASSERT_FALSE(table->field(1)->nullable());
  ASSERT_EQ(0, table->field(1)->metadata()->keys().size());

  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(table))},
       {"project", ProjectNodeOptions({compute::field_ref(0), compute::field_ref(1)})}});

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> out_table, DeclarationToTable(plan));

  ASSERT_TRUE(table->field(0)->nullable());
  ASSERT_EQ(1, table->field(0)->metadata()->keys().size());
  ASSERT_FALSE(table->field(1)->nullable());
  ASSERT_EQ(0, table->field(1)->metadata()->keys().size());

  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema batches_and_schema,
                       DeclarationToExecBatches(plan));
  ASSERT_TRUE(batches_and_schema.schema->field(0)->nullable());
  ASSERT_FALSE(batches_and_schema.schema->field(1)->nullable());
}

}  // namespace acero
}  // namespace arrow
