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

#include "arrow/testing/test_data.h"

#include <memory>
#include <string>
#include <utility>

#include "arrow/dataset/file_base.h"
#include "arrow/record_batch.h"
#include "arrow/util/stl.h"

namespace arrow {
namespace dataset {

// Convenience class allowing easy retrieval of FileSources pointing to test data.
class FileSourceFixtureMixin : public TestDataFixtureMixin {
 public:
  std::unique_ptr<FileSource> GetParquetDataSource(
      const std::string& path,
      Compression::type compression = Compression::UNCOMPRESSED) {
    return internal::make_unique<FileSource>(path, parquet_fs_.get(), compression);
  }

  std::unique_ptr<FileSource> GetArrowDataSource(
      const std::string& path,
      Compression::type compression = Compression::UNCOMPRESSED) {
    return internal::make_unique<FileSource>(path, arrow_fs_.get(), compression);
  }

  std::unique_ptr<FileSource> GetSource(std::shared_ptr<Buffer> buffer) {
    return internal::make_unique<FileSource>(std::move(buffer));
  }
};

class RepeatedRecordBatch : public RecordBatchReader {
 public:
  RepeatedRecordBatch(int64_t repetitions, std::shared_ptr<RecordBatch> batch)
      : repetitions_(repetitions), batch_(std::move(batch)) {}

  std::shared_ptr<Schema> schema() const override { return batch_->schema(); }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    if (repetitions_ > 0) {
      *batch = batch_;
      --repetitions_;
    } else {
      *batch = nullptr;
    }
    return Status::OK();
  }

 private:
  int64_t repetitions_;
  std::shared_ptr<RecordBatch> batch_;
};

}  // namespace dataset
}  // namespace arrow
