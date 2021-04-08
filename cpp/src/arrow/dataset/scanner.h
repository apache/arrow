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

// This API is EXPERIMENTAL.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/expression.h"
#include "arrow/dataset/projector.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/memory_pool.h"
#include "arrow/type_fwd.h"
#include "arrow/util/type_fwd.h"

namespace arrow {

using RecordBatchGenerator = std::function<Future<std::shared_ptr<RecordBatch>>()>;

namespace dataset {

constexpr int64_t kDefaultBatchSize = 1 << 20;

struct ARROW_DS_EXPORT ScanOptions {
  // Filter and projection
  Expression filter = literal(true);
  Expression projection;

  // Schema with which batches will be read from fragments. This is also known as the
  // "reader schema" it will be used (for example) in constructing CSV file readers to
  // identify column types for parsing. Usually only a subset of its fields (see
  // MaterializedFields) will be materialized during a scan.
  std::shared_ptr<Schema> dataset_schema;

  // Schema of projected record batches. This is independent of dataset_schema as its
  // fields are derived from the projection. For example, let
  //
  //   dataset_schema = {"a": int32, "b": int32, "id": utf8}
  //   projection = project({equal(field_ref("a"), field_ref("b"))}, {"a_plus_b"})
  //
  // (no filter specified). In this case, the projected_schema would be
  //
  //   {"a_plus_b": int32}
  std::shared_ptr<Schema> projected_schema;

  // Maximum row count for scanned batches.
  int64_t batch_size = kDefaultBatchSize;

  /// A pool from which materialized and scanned arrays will be allocated.
  MemoryPool* pool = arrow::default_memory_pool();

  /// Indicate if the Scanner should make use of a ThreadPool.
  bool use_threads = false;

  /// Fragment-specific scan options.
  std::shared_ptr<FragmentScanOptions> fragment_scan_options;

  // Return a vector of fields that requires materialization.
  //
  // This is usually the union of the fields referenced in the projection and the
  // filter expression. Examples:
  //
  // - `SELECT a, b WHERE a < 2 && c > 1` => ["a", "b", "a", "c"]
  // - `SELECT a + b < 3 WHERE a > 1` => ["a", "b"]
  //
  // This is needed for expression where a field may not be directly
  // used in the final projection but is still required to evaluate the
  // expression.
  //
  // This is used by Fragment implementations to apply the column
  // sub-selection optimization.
  std::vector<std::string> MaterializedFields() const;

  /// Return a threaded or serial TaskGroup according to use_threads.
  std::shared_ptr<internal::TaskGroup> TaskGroup() const;
};

/// \brief Read record batches from a range of a single data fragment. A
/// ScanTask is meant to be a unit of work to be dispatched. The implementation
/// must be thread and concurrent safe.
class ARROW_DS_EXPORT ScanTask {
 public:
  /// \brief Iterate through sequence of materialized record batches
  /// resulting from the Scan. Execution semantics are encapsulated in the
  /// particular ScanTask implementation
  virtual Result<RecordBatchIterator> Execute() = 0;
  virtual Result<RecordBatchGenerator> ExecuteAsync(internal::Executor* cpu_executor);
  virtual bool supports_async() const;

  virtual ~ScanTask() = default;

  const std::shared_ptr<ScanOptions>& options() const { return options_; }
  const std::shared_ptr<Fragment>& fragment() const { return fragment_; }

 protected:
  ScanTask(std::shared_ptr<ScanOptions> options, std::shared_ptr<Fragment> fragment)
      : options_(std::move(options)), fragment_(std::move(fragment)) {}

  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<Fragment> fragment_;
};

/// \brief A trivial ScanTask that yields the RecordBatch of an array.
class ARROW_DS_EXPORT InMemoryScanTask : public ScanTask {
 public:
  InMemoryScanTask(std::vector<std::shared_ptr<RecordBatch>> record_batches,
                   std::shared_ptr<ScanOptions> options,
                   std::shared_ptr<Fragment> fragment)
      : ScanTask(std::move(options), std::move(fragment)),
        record_batches_(std::move(record_batches)) {}

  Result<RecordBatchIterator> Execute() override;

 protected:
  std::vector<std::shared_ptr<RecordBatch>> record_batches_;
};

ARROW_DS_EXPORT Result<ScanTaskIterator> ScanTaskIteratorFromRecordBatch(
    std::vector<std::shared_ptr<RecordBatch>> batches,
    std::shared_ptr<ScanOptions> options);

/// \brief Scanner is a materialized scan operation with context and options
/// bound. A scanner is the class that glues ScanTask, Fragment,
/// and Dataset. In python pseudo code, it performs the following:
///
///  def Scan():
///    for fragment in self.dataset.GetFragments(this.options.filter):
///      for scan_task in fragment.Scan(this.options):
///        yield scan_task
class ARROW_DS_EXPORT Scanner {
 public:
  Scanner(std::shared_ptr<Dataset> dataset, std::shared_ptr<ScanOptions> scan_options)
      : dataset_(std::move(dataset)), scan_options_(std::move(scan_options)) {}

  Scanner(std::shared_ptr<Fragment> fragment, std::shared_ptr<ScanOptions> scan_options)
      : fragment_(std::move(fragment)), scan_options_(std::move(scan_options)) {}

  /// \brief The Scan operator returns a stream of ScanTask. The caller is
  /// responsible to dispatch/schedule said tasks. Tasks should be safe to run
  /// in a concurrent fashion and outlive the iterator.
  Result<ScanTaskIterator> Scan();

  /// \brief Convert a Scanner into a Table.
  ///
  /// Use this convenience utility with care. This will serially materialize the
  /// Scan result in memory before creating the Table.
  Result<std::shared_ptr<Table>> ToTable();

  /// \brief GetFragments returns an iterator over all Fragments in this scan.
  Result<FragmentIterator> GetFragments();

  const std::shared_ptr<Schema>& schema() const {
    return scan_options_->projected_schema;
  }

  const std::shared_ptr<ScanOptions>& options() const { return scan_options_; }

 protected:
  Future<std::shared_ptr<Table>> ToTableInternal(internal::Executor* cpu_executor);

  std::shared_ptr<Dataset> dataset_;
  // TODO(ARROW-8065) remove fragment_ after a Dataset is constuctible from fragments
  std::shared_ptr<Fragment> fragment_;
  std::shared_ptr<ScanOptions> scan_options_;
};

/// \brief ScannerBuilder is a factory class to construct a Scanner. It is used
/// to pass information, notably a potential filter expression and a subset of
/// columns to materialize.
class ARROW_DS_EXPORT ScannerBuilder {
 public:
  explicit ScannerBuilder(std::shared_ptr<Dataset> dataset);

  ScannerBuilder(std::shared_ptr<Dataset> dataset,
                 std::shared_ptr<ScanOptions> scan_options);

  ScannerBuilder(std::shared_ptr<Schema> schema, std::shared_ptr<Fragment> fragment,
                 std::shared_ptr<ScanOptions> scan_options);

  /// \brief Set the subset of columns to materialize.
  ///
  /// Columns which are not referenced may not be read from fragments.
  ///
  /// \param[in] columns list of columns to project. Order and duplicates will
  ///            be preserved.
  ///
  /// \return Failure if any column name does not exists in the dataset's
  ///         Schema.
  Status Project(std::vector<std::string> columns);

  /// \brief Set expressions which will be evaluated to produce the materialized columns.
  ///
  /// Columns which are not referenced may not be read from fragments.
  ///
  /// \param[in] exprs expressions to evaluate to produce columns.
  /// \param[in] names list of names for the resulting columns.
  ///
  /// \return Failure if any referenced column does not exists in the dataset's
  ///         Schema.
  Status Project(std::vector<Expression> exprs, std::vector<std::string> names);

  /// \brief Set the filter expression to return only rows matching the filter.
  ///
  /// The predicate will be passed down to Sources and corresponding
  /// Fragments to exploit predicate pushdown if possible using
  /// partition information or Fragment internal metadata, e.g. Parquet statistics.
  /// Columns which are not referenced may not be read from fragments.
  ///
  /// \param[in] filter expression to filter rows with.
  ///
  /// \return Failure if any referenced columns does not exist in the dataset's
  ///         Schema.
  Status Filter(const Expression& filter);

  /// \brief Indicate if the Scanner should make use of the available
  ///        ThreadPool found in ScanOptions;
  Status UseThreads(bool use_threads = true);

  /// \brief Set the maximum number of rows per RecordBatch.
  ///
  /// \param[in] batch_size the maximum number of rows.
  /// \returns An error if the number for batch is not greater than 0.
  ///
  /// This option provides a control limiting the memory owned by any RecordBatch.
  Status BatchSize(int64_t batch_size);

  /// \brief Set the pool from which materialized and scanned arrays will be allocated.
  Status Pool(MemoryPool* pool);

  /// \brief Set fragment-specific scan options.
  Status FragmentScanOptions(std::shared_ptr<FragmentScanOptions> fragment_scan_options);

  /// \brief Return the constructed now-immutable Scanner object
  Result<std::shared_ptr<Scanner>> Finish();

  const std::shared_ptr<Schema>& schema() const;

 private:
  std::shared_ptr<Dataset> dataset_;
  std::shared_ptr<Fragment> fragment_;
  std::shared_ptr<ScanOptions> scan_options_;
};

}  // namespace dataset
}  // namespace arrow
