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
//
#include "arrow/table.h"
#include "arrow/util/iterator.h"

#include "parquet/arrow/dataset/file_parquet.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"

namespace parquet {
namespace arrow {
namespace dataset {

using ScanTaskPtr = std::unique_ptr<::arrow::dataset::ScanTask>;
using ParquetFileReaderPtr = std::unique_ptr<parquet::ParquetFileReader>;
using RecordBatchReaderPtr = std::unique_ptr<::arrow::RecordBatchReader>;

class ParquetScanTask : public ::arrow::dataset::ScanTask {
 public:
  static ::arrow::Status Make(std::vector<int> row_groups,
                              const std::vector<int>& columns_projection,
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
    return ::arrow::Status::OK();
  }

  std::unique_ptr<::arrow::RecordBatchIterator> Scan() override {
    return std::move(record_batch_reader_);
  }

 private:
  ParquetScanTask(std::vector<int> row_groups,
                  std::shared_ptr<parquet::arrow::FileReader> reader,
                  RecordBatchReaderPtr record_batch_reader)
      : row_groups_(std::move(row_groups)),
        reader_(reader),
        record_batch_reader_(std::move(record_batch_reader)) {}

  // List of RowGroup identifiers this ScanTask is associated with.
  std::vector<int> row_groups_;

  // The ScanTask _must_ hold a reference to reader_ because there's no
  // guarantee the producing ParquetScanTaskIterator is still alive. This is a
  // contract required by record_batch_reader_
  std::shared_ptr<parquet::arrow::FileReader> reader_;
  RecordBatchReaderPtr record_batch_reader_;
};

class ParquetScanTaskIterator : public ::arrow::dataset::ScanTaskIterator {
 public:
  static ::arrow::Status Make(std::shared_ptr<::arrow::dataset::ScanOptions> options,
                              std::shared_ptr<::arrow::dataset::ScanContext> context,
                              ParquetFileReaderPtr reader,
                              std::unique_ptr<::arrow::dataset::ScanTaskIterator>* out) {
    // Take a reference on metadata because FileReader takes ownership of
    // reader.
    auto metadata = reader->metadata();

    std::vector<int> columns_projection;
    RETURN_NOT_OK(InferColumnProjection(*metadata, *options, &columns_projection));

    std::unique_ptr<FileReader> arrow_reader;
    RETURN_NOT_OK(FileReader::Make(context->pool, std::move(reader), &arrow_reader));

    out->reset(new ParquetScanTaskIterator(columns_projection, metadata,
                                           std::move(arrow_reader)));

    return ::arrow::Status::OK();
  }

  ::arrow::Status Next(ScanTaskPtr* task) override {
    auto next_partition = NextRowGroupPartition();

    // Iteration is done.
    if (next_partition.size() == 0) {
      task->reset(nullptr);
      return ::arrow::Status::OK();
    }

    return ParquetScanTask::Make(std::move(next_partition), columns_projection_, reader_,
                                 task);
  }

 private:
  // Compute the column projection out of an optional arrow::Schema
  static ::arrow::Status InferColumnProjection(
      const FileMetaData& metadata, const ::arrow::dataset::ScanOptions& options,
      std::vector<int>* out) {
    // TODO(fsaintjacques): Compute intersection _and_ validity
    *out = metadata.AllColumnIndices();

    return ::arrow::Status::OK();
  }

  ParquetScanTaskIterator(std::vector<int> columns_projection,
                          std::shared_ptr<FileMetaData> metadata,
                          std::unique_ptr<parquet::arrow::FileReader> reader)
      : row_group_idx_(0),
        columns_projection_(columns_projection),
        metadata_(std::move(metadata)),
        reader_(std::move(reader)) {}

  std::vector<int> NextRowGroupPartition() {
    // TODO(fsaintjacques): Apply filters to RowGroups with metadata
    // TODO(fsaintjacques): Partitions the RowGroups properly
    if (row_group_idx_ == metadata_->num_row_groups()) return {};
    return {row_group_idx_++};
  }

  // Index that keeps track of the last consumed RowGroup
  int row_group_idx_;

  // Subset of columns to ingest
  std::vector<int> columns_projection_;

  // The metadata reference is used to discover the number and sizes of
  // RowGroups allowing an (hopefully) balanced partitioning in ScanTasks
  std::shared_ptr<FileMetaData> metadata_;

  std::shared_ptr<parquet::arrow::FileReader> reader_;

  std::shared_ptr<::arrow::dataset::ScanOptions> opts_;
  std::shared_ptr<::arrow::dataset::ScanContext> ctx_;
};

::arrow::Status ParquetFileFormat::ScanFile(
    const ::arrow::dataset::FileSource& location,
    std::shared_ptr<::arrow::dataset::ScanOptions> scan_options,
    std::shared_ptr<::arrow::dataset::ScanContext> scan_context,
    std::unique_ptr<::arrow::dataset::ScanTaskIterator>* out) const {
  std::shared_ptr<::arrow::io::RandomAccessFile> input;
  RETURN_NOT_OK(location.Open(&input));

  auto reader = ParquetFileReader::Open(input);
  return ParquetScanTaskIterator::Make(scan_options, scan_context, std::move(reader),
                                       out);
}

}  // namespace dataset
}  // namespace arrow
}  // namespace parquet
