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

#include <utility>
#include <vector>

#include "arrow/table.h"
#include "arrow/util/iterator.h"
#include "arrow/util/range.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

namespace arrow {
namespace dataset {

using ScanTaskPtr = std::unique_ptr<ScanTask>;
using ParquetFileReaderPtr = std::unique_ptr<parquet::ParquetFileReader>;
using RecordBatchReaderPtr = std::unique_ptr<RecordBatchReader>;

// A set of RowGroup identifiers
using RowGroupSet = std::vector<int>;

class ParquetScanTask : public ScanTask {
 public:
  static Status Make(RowGroupSet row_groups, const std::vector<int>& columns_projection,
                     std::shared_ptr<parquet::arrow::FileReader> reader,
                     ScanTaskPtr* out) {
    RecordBatchReaderPtr record_batch_reader;
    // TODO(fsaintjacques): Ensure GetRecordBatchReader is truly streaming and
    // not using a TableBatchReader (materializing the full partition instead
    // of streaming).
    RETURN_NOT_OK(reader->GetRecordBatchReader(row_groups, columns_projection,
                                               &record_batch_reader));

    out->reset(new ParquetScanTask(row_groups, std::move(reader),
                                   std::move(record_batch_reader)));
    return Status::OK();
  }

  std::unique_ptr<RecordBatchIterator> Scan() override {
    return std::move(record_batch_reader_);
  }

 private:
  ParquetScanTask(RowGroupSet row_groups,
                  std::shared_ptr<parquet::arrow::FileReader> reader,
                  RecordBatchReaderPtr record_batch_reader)
      : row_groups_(std::move(row_groups)),
        reader_(reader),
        record_batch_reader_(std::move(record_batch_reader)) {}

  // List of RowGroup identifiers this ScanTask is associated with.
  RowGroupSet row_groups_;

  // The ScanTask _must_ hold a reference to reader_ because there's no
  // guarantee the producing ParquetScanTaskIterator is still alive. This is a
  // contract required by record_batch_reader_
  std::shared_ptr<parquet::arrow::FileReader> reader_;
  RecordBatchReaderPtr record_batch_reader_;
};

constexpr int64_t kDefaultRowCountPerPartition = 1U << 16;

// A class that clusters RowGroups of a Parquet file until the cluster has a specified
// total row count. This doesn't guarantee exact row counts; it may exceed the target.
class ParquetRowGroupPartitioner {
 public:
  ParquetRowGroupPartitioner(std::shared_ptr<parquet::FileMetaData> metadata,
                             int64_t row_count = kDefaultRowCountPerPartition)
      : metadata_(std::move(metadata)), row_count_(row_count), row_group_idx_(0) {
    num_row_groups_ = metadata_->num_row_groups();
  }

  RowGroupSet Next() {
    int64_t partition_size = 0;
    RowGroupSet partition;

    while (row_group_idx_ < num_row_groups_ && partition_size < row_count_) {
      partition_size += metadata_->RowGroup(row_group_idx_)->num_rows();
      partition.push_back(row_group_idx_++);
    }

    return partition;
  }

 private:
  std::shared_ptr<parquet::FileMetaData> metadata_;
  int64_t row_count_;
  int row_group_idx_;
  int num_row_groups_;
};

class ParquetScanTaskIterator : public ScanTaskIterator {
 public:
  static Status Make(std::shared_ptr<ScanOptions> options,
                     std::shared_ptr<ScanContext> context, ParquetFileReaderPtr reader,
                     std::unique_ptr<ScanTaskIterator>* out) {
    auto metadata = reader->metadata();

    std::vector<int> columns_projection;
    RETURN_NOT_OK(InferColumnProjection(*metadata, options, &columns_projection));

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    RETURN_NOT_OK(parquet::arrow::FileReader::Make(context->pool, std::move(reader),
                                                   &arrow_reader));

    out->reset(new ParquetScanTaskIterator(columns_projection, metadata,
                                           std::move(arrow_reader)));

    return Status::OK();
  }

  Status Next(ScanTaskPtr* task) override {
    auto partition = partitionner_.Next();

    // Iteration is done.
    if (partition.size() == 0) {
      task->reset(nullptr);
      return Status::OK();
    }

    return ParquetScanTask::Make(std::move(partition), columns_projection_, reader_,
                                 task);
  }

 private:
  // Compute the column projection out of an optional arrow::Schema
  static Status InferColumnProjection(const parquet::FileMetaData& metadata,
                                      const std::shared_ptr<ScanOptions>& options,
                                      std::vector<int>* out) {
    // TODO(fsaintjacques): Compute intersection _and_ validity
    *out = internal::Iota(metadata.num_columns());

    return Status::OK();
  }

  ParquetScanTaskIterator(std::vector<int> columns_projection,
                          std::shared_ptr<parquet::FileMetaData> metadata,
                          std::unique_ptr<parquet::arrow::FileReader> reader)
      : columns_projection_(columns_projection),
        partitionner_(std::move(metadata)),
        reader_(std::move(reader)) {}

  std::vector<int> columns_projection_;
  ParquetRowGroupPartitioner partitionner_;
  std::shared_ptr<parquet::arrow::FileReader> reader_;
};

Status ParquetFileFormat::ScanFile(const FileSource& source,
                                   std::shared_ptr<ScanOptions> scan_options,
                                   std::shared_ptr<ScanContext> scan_context,
                                   std::unique_ptr<ScanTaskIterator>* out) const {
  std::shared_ptr<io::RandomAccessFile> input;
  RETURN_NOT_OK(source.Open(&input));

  auto reader = parquet::ParquetFileReader::Open(input);
  return ParquetScanTaskIterator::Make(scan_options, scan_context, std::move(reader),
                                       out);
}

}  // namespace dataset
}  // namespace arrow
