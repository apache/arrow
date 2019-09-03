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

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/memory_pool.h"

namespace arrow {
namespace dataset {

/// Container for scan state
struct ARROW_DS_EXPORT ScanOptions final {
 public:
  ScanOptions() = default;

  /// Filters
  const std::shared_ptr<DataSelector>& selector() const { return selector_; }

  ScanOptions& selector(std::shared_ptr<DataSelector> s) {
    selector_ = std::move(s);
    return *this;
  }

  /// Schema to which record batches will be projected
  const std::shared_ptr<Schema>& schema() const { return schema_; }

  ScanOptions& schema(std::shared_ptr<Schema> s) {
    schema_ = std::move(s);
    return *this;
  }

  /// MemoryPool used for allocating temporary memory and yielded record batches
  MemoryPool* pool() const { return pool_; }

  ScanOptions& pool(MemoryPool* p) {
    pool_ = p;
    return *this;
  }

  /// \brief Base class for format specific file scanning options
  class ARROW_DS_EXPORT FileOptions {
   public:
    /// \brief The file format this options corresponds to
    virtual std::shared_ptr<FileFormat> file_format() const = 0;

    virtual ~FileOptions() = default;
  };

  /// Format-specific file scanning options
  const std::vector<std::shared_ptr<FileOptions>>& file_options() const {
    return file_options_;
  }

  ScanOptions& AddFileOptions(std::shared_ptr<FileOptions> file_options) {
    file_options_.push_back(std::move(file_options));
    return *this;
  }

  /// Include columns generated from partition keys
  bool include_partition_keys() const { return include_partition_keys_; }

  ScanOptions& include_partition_keys(bool include_partition_keys) {
    include_partition_keys_ = include_partition_keys;
    return *this;
  }

 protected:
  bool include_partition_keys_;

  std::shared_ptr<DataSelector> selector_;

  // Schema to which record batches will be reconciled
  std::shared_ptr<Schema> schema_;

  MemoryPool* pool_ = default_memory_pool();

  std::vector<std::shared_ptr<FileOptions>> file_options_;
};

/// \brief Read record batches from a range of a single data fragment. A
/// ScanTask is meant to be a unit of work to be dispatched. The implementation
/// must be thread and concurrent safe.
class ARROW_DS_EXPORT ScanTask {
 public:
  /// \brief Iterate through sequence of materialized record batches
  /// resulting from the Scan. Execution semantics encapsulated in the
  /// particular ScanTask implementation
  virtual std::unique_ptr<RecordBatchIterator> Scan() = 0;

  virtual ~ScanTask() = default;
};

/// \brief A trivial ScanTask that yields the RecordBatch of an array.
class ARROW_DS_EXPORT SimpleScanTask : public ScanTask {
 public:
  explicit SimpleScanTask(std::vector<std::shared_ptr<RecordBatch>> record_batches)
      : record_batches_(std::move(record_batches)) {}

  std::unique_ptr<RecordBatchIterator> Scan() override;

 protected:
  std::vector<std::shared_ptr<RecordBatch>> record_batches_;
};

/// \brief Scanner is a materialized scan operation with context and options
/// bound. A scanner is the class that glues ScanTask, DataFragment,
/// and DataSource. In python pseudo code, it performs the following:
///
///  def Scan():
///    for source in this.sources_:
///      for fragment in source.GetFragments(this.options_):
///        for scan_task in fragment.Scan(this.context_):
///          yield scan_task
class ARROW_DS_EXPORT Scanner {
 public:
  /// \brief The Scan operator returns a stream of ScanTask. The caller is
  /// responsible to dispatch/schedule said tasks. Tasks should be safe to run
  /// in a concurrent fashion and outlive the iterator.
  virtual std::unique_ptr<ScanTaskIterator> Scan() = 0;

  virtual ~Scanner() = default;
};

/// \brief SimpleScanner is a trivial Scanner implementation that flattens
/// chained iterators.
///
/// The returned iterator of SimpleScanner::Scan is a serial blocking
/// iterator. It will block if any of the following methods blocks:
///  - Iterator::Next
///  - DataSource::GetFragments
///  - DataFragment::Scan
///
/// Thus, this iterator is not suited for consumption of sources/fragments
/// where the previous methods can block for a long time, e.g. if fetching a
/// DataFragment from cloud storage, or a DataFragment must be parsed before
/// returning a ScanTaskIterator.
class ARROW_DS_EXPORT SimpleScanner : public Scanner {
 public:
  SimpleScanner(std::vector<std::shared_ptr<DataSource>> sources,
                std::shared_ptr<ScanContext> context)
      : sources_(std::move(sources)), context_(std::move(context)) {}

  std::unique_ptr<ScanTaskIterator> Scan() override;

 private:
  std::vector<std::shared_ptr<DataSource>> sources_;
  std::shared_ptr<ScanContext> context_;
};

class ARROW_DS_EXPORT ScannerBuilder {
 public:
  explicit ScannerBuilder(std::shared_ptr<Dataset> dataset);

  /// \brief Set
  ScannerBuilder* Project(const std::vector<std::string>& columns);

  ScannerBuilder* AddFilter(const std::shared_ptr<Filter>& filter);

  ScannerBuilder* SetGlobalFileOptions(std::shared_ptr<ScanOptions::FileOptions> options);

  /// \brief If true (default), add partition keys to the
  /// RecordBatches that the scan produces if they are not in the data
  /// otherwise
  ScannerBuilder* IncludePartitionKeys(bool include = true);

  /// \brief Return the constructed now-immutable Scanner object
  Status Finish(std::unique_ptr<Scanner>* out) const;

 private:
  std::shared_ptr<Dataset> dataset_;
  std::vector<std::string> project_columns_;
  FilterVector filters_;
  bool include_partition_keys_;
};

}  // namespace dataset
}  // namespace arrow
