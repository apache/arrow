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
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/memory_pool.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

class Table;

namespace internal {
class TaskGroup;
};

namespace dataset {

/// \brief Shared state for a Scan operation
struct ARROW_DS_EXPORT ScanContext {
  MemoryPool* pool = arrow::default_memory_pool();
  internal::ThreadPool* thread_pool = arrow::internal::GetCpuThreadPool();
};

class RecordBatchProjector;

class ARROW_DS_EXPORT ScanOptions {
 public:
  virtual ~ScanOptions() = default;

  static std::shared_ptr<ScanOptions> Defaults();

  // Indicate if the Scanner should make use of the ThreadPool found in the
  // ScanContext.
  bool use_threads = false;

  // Filter
  std::shared_ptr<Expression> filter;
  // Evaluator for Filter
  std::shared_ptr<ExpressionEvaluator> evaluator;

  // Schema to which record batches will be reconciled
  std::shared_ptr<Schema> schema;
  // Projector for reconciling the final RecordBatch to the requested schema.
  std::shared_ptr<RecordBatchProjector> projector;

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

Status ScanTaskIteratorFromRecordBatch(std::vector<std::shared_ptr<RecordBatch>> batches,
                                       ScanTaskIterator* out);

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
  Scanner(DataSourceVector sources, std::shared_ptr<ScanOptions> options,
          std::shared_ptr<ScanContext> context)
      : sources_(std::move(sources)),
        options_(std::move(options)),
        context_(std::move(context)) {}

  virtual ~Scanner() = default;

  /// \brief The Scan operator returns a stream of ScanTask. The caller is
  /// responsible to dispatch/schedule said tasks. Tasks should be safe to run
  /// in a concurrent fashion and outlive the iterator.
  virtual ScanTaskIterator Scan() = 0;

  /// \brief Convert a Scanner into a Table.
  ///
  /// \param[out] out output parameter
  ///
  /// Use this convenience utility with care. This will serially materialize the
  /// Scan result in memory before creating the Table.
  Status ToTable(std::shared_ptr<Table>* out);

 protected:
  /// \brief Return a TaskGroup according to ScanContext thread rules.
  std::shared_ptr<internal::TaskGroup> TaskGroup() const;

  DataSourceVector sources_;
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
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
      : Scanner(std::move(sources), std::move(options), std::move(context)) {}

  ScanTaskIterator Scan() override;
};

/// \brief ScannerBuilder is a factory class to construct a Scanner. It is used
/// to pass information, notably a potential filter expression and a subset of
/// columns to materialize.
class ARROW_DS_EXPORT ScannerBuilder {
 public:
  ScannerBuilder(std::shared_ptr<Dataset> dataset,
                 std::shared_ptr<ScanContext> scan_context);

  /// \brief Set the subset of columns to materialize.
  ///
  /// This subset wil be passed down to DataSources and corresponding DataFragments.
  /// The goal is to avoid loading/copying/deserializing columns that will
  /// not be required further down the compute chain.
  ///
  /// \param[in] columns list of columns to project. Order and duplicates will
  ///            be preserved.
  ///
  /// \return Failure if any column name does not exists in the dataset's
  ///         Schema.
  Status Project(const std::vector<std::string>& columns);

  /// \brief Set the filter expression to return only rows matching the filter.
  ///
  /// The predicate will be passed down to DataSources and corresponding
  /// DataFragments to exploit predicate pushdown if possible using
  /// partition information or DataFragment internal metadata, e.g. Parquet statistics.
  /// statistics.
  ///
  /// \param[in] filter expression to filter rows with.
  ///
  /// \return Failure if any referenced columns does not exist in the dataset's
  ///         Schema.
  Status Filter(std::shared_ptr<Expression> filter);
  Status Filter(const Expression& filter);

  /// \brief Indicate if the Scanner should make use of the available
  ///        ThreadPool found in ScanContext;
  Status UseThreads(bool use_threads = true);

  /// \brief Return the constructed now-immutable Scanner object
  Status Finish(std::unique_ptr<Scanner>* out) const;

  std::shared_ptr<Schema> schema() const { return dataset_->schema(); }

 private:
  std::shared_ptr<Dataset> dataset_;
  std::shared_ptr<ScanOptions> scan_options_;
  std::shared_ptr<ScanContext> scan_context_;
  bool has_projection_ = false;
  std::vector<std::string> project_columns_;
};

}  // namespace dataset
}  // namespace arrow
