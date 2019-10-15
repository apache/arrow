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

#include "arrow/compute/context.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/memory_pool.h"

namespace arrow {

class Table;

namespace dataset {

/// \brief Shared state for a Scan operation
struct ARROW_DS_EXPORT ScanContext {
  MemoryPool* pool = arrow::default_memory_pool();
};

class ARROW_DS_EXPORT ScanOptions {
 public:
  virtual ~ScanOptions() = default;

  static std::shared_ptr<ScanOptions> Defaults();

  // Filter
  std::shared_ptr<Expression> filter;

  // Evaluator for Filter
  std::shared_ptr<ExpressionEvaluator> evaluator;

  // Schema to which record batches will be projected
  std::shared_ptr<Schema> schema;

  std::vector<std::shared_ptr<FileScanOptions>> options;

  bool include_partition_keys = true;

 private:
  ScanOptions();
};

/// \brief Read record batches from a range of a single data fragment. A
/// ScanTask is meant to be a unit of work to be dispatched. The implementation
/// must be thread and concurrent safe.
class ARROW_DS_EXPORT ScanTask {
 public:
  /// \brief Iterate through sequence of materialized record batches
  /// resulting from the Scan. Execution semantics encapsulated in the
  /// particular ScanTask implementation
  virtual RecordBatchIterator Scan() = 0;

  virtual ~ScanTask() = default;
};

/// \brief A trivial ScanTask that yields the RecordBatch of an array.
class ARROW_DS_EXPORT SimpleScanTask : public ScanTask {
 public:
  explicit SimpleScanTask(std::vector<std::shared_ptr<RecordBatch>> record_batches)
      : record_batches_(std::move(record_batches)) {}

  SimpleScanTask(std::vector<std::shared_ptr<RecordBatch>> record_batches,
                 std::shared_ptr<ScanOptions> options,
                 std::shared_ptr<ScanContext> context)
      : record_batches_(std::move(record_batches)),
        options_(std::move(options)),
        context_(std::move(context)) {}

  RecordBatchIterator Scan() override;

 protected:
  std::vector<std::shared_ptr<RecordBatch>> record_batches_;
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
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
  virtual ScanTaskIterator Scan() = 0;

  virtual ~Scanner() = default;

  /// \brief Convert a Scanner into a Table.
  ///
  /// Use this convenience utility with care. This will serially materialize the
  /// Scan result in memory before creating the Table.
  static Status ToTable(std::shared_ptr<Scanner> scanner, std::shared_ptr<Table>* out);
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
                std::shared_ptr<ScanOptions> options,
                std::shared_ptr<ScanContext> context)
      : sources_(std::move(sources)),
        options_(std::move(options)),
        context_(std::move(context)) {}

  ScanTaskIterator Scan() override;

 private:
  std::vector<std::shared_ptr<DataSource>> sources_;
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
};

class ARROW_DS_EXPORT ScannerBuilder {
 public:
  ScannerBuilder(std::shared_ptr<Dataset> dataset,
                 std::shared_ptr<ScanContext> scan_context);

  /// \brief Set
  ScannerBuilder* Project(const std::vector<std::string>& columns);

  ScannerBuilder* Filter(std::shared_ptr<Expression> filter);
  ScannerBuilder* Filter(const Expression& filter);

  ScannerBuilder* FilterEvaluator(std::shared_ptr<ExpressionEvaluator> evaluator);

  ScannerBuilder* SetGlobalFileOptions(std::shared_ptr<FileScanOptions> options);

  /// \brief If true (default), add partition keys to the
  /// RecordBatches that the scan produces if they are not in the data
  /// otherwise
  ScannerBuilder* IncludePartitionKeys(bool include = true);

  /// \brief Return the constructed now-immutable Scanner object
  Status Finish(std::unique_ptr<Scanner>* out) const;

 private:
  std::shared_ptr<Dataset> dataset_;
  std::shared_ptr<ScanOptions> scan_options_;
  std::shared_ptr<ScanContext> scan_context_;
  std::vector<std::string> project_columns_;
};

}  // namespace dataset
}  // namespace arrow
