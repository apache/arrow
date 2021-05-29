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

#include "arrow/dataset/file_rados_parquet.h"
#include "arrow/api.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/test_util.h"

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

namespace arrow {
namespace dataset {

std::shared_ptr<arrow::Table> CreateTable() {
  auto schema =
      arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
                     arrow::field("c", arrow::int64())});
  std::shared_ptr<arrow::Array> array_a;
  std::shared_ptr<arrow::Array> array_b;
  std::shared_ptr<arrow::Array> array_c;
  arrow::NumericBuilder<arrow::Int64Type> builder;
  ABORT_ON_FAILURE(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
  ABORT_ON_FAILURE(builder.Finish(&array_a));
  builder.Reset();
  ABORT_ON_FAILURE(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
  ABORT_ON_FAILURE(builder.Finish(&array_b));
  builder.Reset();
  ABORT_ON_FAILURE(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
  ABORT_ON_FAILURE(builder.Finish(&array_c));
  return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

TEST(TestRadosParquetFileFormat, ScanRequestSerializeDeserialize) {
  std::shared_ptr<ScanOptions> options = std::make_shared<ScanOptions>();
  options->projected_schema = arrow::schema({arrow::field("a", arrow::int64())});
  options->dataset_schema = arrow::schema({arrow::field("a", arrow::int64())});

  ceph::bufferlist bl;
  int64_t file_size = 1000000;
  SerializeScanRequest(options, file_size, bl);

  compute::Expression filter_;
  compute::Expression partition_expression_;
  std::shared_ptr<Schema> projected_schema_;
  std::shared_ptr<Schema> dataset_schema_;
  int64_t file_size_;
  DeserializeScanRequest(&filter_, &partition_expression_, &projected_schema_,
                         &dataset_schema_, file_size_, bl);

  ASSERT_EQ(options->filter.Equals(filter_), 1);
  ASSERT_EQ(options->partition_expression.Equals(partition_expression_), 1);
  ASSERT_EQ(options->projected_schema->Equals(projected_schema_), 1);
  ASSERT_EQ(options->dataset_schema->Equals(dataset_schema_), 1);
}

TEST(TestRadosParquetFileFormat, SerializeDeserializeTable) {
  std::shared_ptr<Table> table = CreateTable();
  ceph::bufferlist bl;
  SerializeTable(table, bl);

  RecordBatchVector batches;
  DeserializeTable(batches, bl);
  ASSERT_OK_AND_ASSIGN(auto materialized_table, arrow::Table::FromRecordBatches(batches));

  ASSERT_EQ(table->Equals(*materialized_table), 1);
}

}  // namespace dataset
}  // namespace arrow
