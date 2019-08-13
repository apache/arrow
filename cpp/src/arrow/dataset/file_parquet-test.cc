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

#include "arrow/dataset/file_parquet.h"

#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"

namespace arrow {
namespace dataset {

class TestParquetFileFormat : public FileSourceFixtureMixin {
 public:
  TestParquetFileFormat() : ctx_(std::make_shared<ScanContext>()) {}

 protected:
  std::shared_ptr<ScanOptions> opts_;
  std::shared_ptr<ScanContext> ctx_;
};

TEST_F(TestParquetFileFormat, ScanFile) {
  auto location = GetParquetLocation("data/double_1Grows_1kgroups.parquet");
  auto fragment = std::make_shared<ParquetFragment>(*location, opts_);

  std::unique_ptr<ScanTaskIterator> it;
  ASSERT_OK(fragment->GetTasks(ctx_, &it));
  int64_t row_count = 0;

  ASSERT_OK(it->Visit([&row_count](std::unique_ptr<ScanTask> task) -> Status {
    auto batch_it = task->Scan();

    RETURN_NOT_OK(
        batch_it->Visit([&row_count](std::shared_ptr<RecordBatch> batch) -> Status {
          row_count += batch->num_rows();
          return Status::OK();
        }));

    return Status::OK();
  }));

  ASSERT_EQ(row_count, 1UL << 30);
}

}  // namespace dataset
}  // namespace arrow
