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
#include "skyhook/protocol/skyhook_protocol.h"

#include "arrow/compute/expression.h"
#include "arrow/dataset/test_util_internal.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

std::shared_ptr<arrow::Table> CreateTable() {
  auto schema = arrow::schema({
      {arrow::field("a", arrow::uint8())},
      {arrow::field("b", arrow::uint32())},
  });

  std::shared_ptr<arrow::Table> table;
  return TableFromJSON(schema, {R"([{"a": null, "b": 5},
                                     {"a": 1,    "b": 3},
                                     {"a": 3,    "b": null},
                                     {"a": null, "b": null},
                                     {"a": 2,    "b": 5},
                                     {"a": 1,    "b": 5}
                                    ])"});
}

TEST(TestSkyhookProtocol, SerDeserScanRequest) {
  ceph::bufferlist bl;
  skyhook::ScanRequest req;
  req.filter_expression = arrow::compute::literal(true);
  req.partition_expression = arrow::compute::literal(false);
  req.projection_schema = arrow::schema({arrow::field("a", arrow::int64())});
  req.dataset_schema = arrow::schema({arrow::field("a", arrow::int64())});
  req.file_size = 1000000;
  req.file_format = skyhook::SkyhookFileType::type::IPC;
  ASSERT_OK(skyhook::SerializeScanRequest(req, &bl));

  skyhook::ScanRequest req_;
  ASSERT_OK(skyhook::DeserializeScanRequest(bl, &req_));
  ASSERT_TRUE(req.filter_expression.Equals(req_.filter_expression));
  ASSERT_TRUE(req.partition_expression.Equals(req_.partition_expression));
  ASSERT_TRUE(req.projection_schema->Equals(req_.projection_schema));
  ASSERT_TRUE(req.dataset_schema->Equals(req_.dataset_schema));
  ASSERT_EQ(req.file_size, req_.file_size);
  ASSERT_EQ(req.file_format, req_.file_format);
}

TEST(TestSkyhookProtocol, SerDeserTable) {
  std::shared_ptr<arrow::Table> table = CreateTable();
  ceph::bufferlist bl;
  ASSERT_OK(skyhook::SerializeTable(table, &bl));

  arrow::RecordBatchVector batches;
  ASSERT_OK(skyhook::DeserializeTable(bl, false, &batches));
  ASSERT_OK_AND_ASSIGN(auto materialized_table, arrow::Table::FromRecordBatches(batches));

  ASSERT_TRUE(table->Equals(*materialized_table));
}
