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

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/projector.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/memory_pool.h"
#include "arrow/type_fwd.h"
#include "arrow/util/type_fwd.h"

namespace arrow {
namespace dataset {

/// \brief Shared state for a Scan operation
struct ARROW_DS_EXPORT ScanContext {
  /// A pool from which materialized and scanned arrays will be allocated.
  MemoryPool* pool = arrow::default_memory_pool();

  /// Indicate if the Scanner should make use of a ThreadPool.
  bool use_threads = false;

  /// Return a threaded or serial TaskGroup according to use_threads.
  std::shared_ptr<internal::TaskGroup> TaskGroup() const;
};

class ARROW_DS_EXPORT ScanOptions {
 public:
  virtual ~ScanOptions() = default;

  static std::shared_ptr<ScanOptions> Make(std::shared_ptr<Schema> schema) {
    return std::shared_ptr<ScanOptions>(new ScanOptions(std::move(schema)));
  }

  // Construct a copy of these options with a different schema.
  // The projector will be reconstructed.
  std::shared_ptr<ScanOptions> ReplaceSchema(std::shared_ptr<Schema> schema) const;

  // Filter
  std::shared_ptr<Expression> filter = scalar(true);

  // Evaluator for Filter
  std::shared_ptr<ExpressionEvaluator> evaluator;

  // Schema to which record batches will be reconciled
  const std::shared_ptr<Schema>& schema() const { return projector.schema(); }

  // Projector for reconciling the final RecordBatch to the requested schema.
  RecordBatchProjector projector;

  // Maximum row count for scanned batches.
  int64_t batch_size = 1 << 15;

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
  // This is used by Fragments implementation to apply the column
  // sub-selection optimization.
  std::vector<std::string> MaterializedFields() const;

 private:
  explicit ScanOptions(std::shared_ptr<Schema> schema);
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

  virtual ~ScanTask() = default;

  const std::shared_ptr<ScanOptions>& options() const { return options_; }
  const std::shared_ptr<ScanContext>& context() const { return context_; }

 protected:
  ScanTask(std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext> context)
      : options_(std::move(options)), context_(std::move(context)) {}

  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
};

/// \brief A trivial ScanTask that yields the RecordBatch of an array.
class ARROW_DS_EXPORT InMemoryScanTask : public ScanTask {
 public:
  InMemoryScanTask(std::vector<std::shared_ptr<RecordBatch>> record_batches,
                   std::shared_ptr<ScanOptions> options,
                   std::shared_ptr<ScanContext> context)
      : ScanTask(std::move(options), std::move(context)),
        record_batches_(std::move(record_batches)) {}

  Result<RecordBatchIterator> Execute() override;

 protected:
  std::vector<std::shared_ptr<RecordBatch>> record_batches_;
};

ARROW_DS_EXPORT Result<ScanTaskIterator> ScanTaskIteratorFromRecordBatch(
    std::vector<std::shared_ptr<RecordBatch>> batches,
    std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext>);

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
  Scanner(std::shared_ptr<Dataset> dataset, std::shared_ptr<ScanOptions> scan_options,
          std::shared_ptr<ScanContext> scan_context)
      : dataset_(std::move(dataset)),
        scan_options_(std::move(scan_options)),
        scan_context_(std::move(scan_context)) {}

  Scanner(std::shared_ptr<Fragment> fragment, std::shared_ptr<ScanOptions> scan_options,
          std::shared_ptr<ScanContext> scan_context)
      : fragment_(std::move(fragment)),
        scan_options_(std::move(scan_options)),
        scan_context_(std::move(scan_context)) {}

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
  FragmentIterator GetFragments();

  const std::shared_ptr<Schema>& schema() const { return scan_options_->schema(); }

  const std::shared_ptr<ScanOptions>& options() const { return scan_options_; }

  const std::shared_ptr<ScanContext>& context() const { return scan_context_; }

 protected:
  std::shared_ptr<Dataset> dataset_;
  // TODO(ARROW-8065) remove fragment_ after a Dataset is constuctible from fragments
  std::shared_ptr<Fragment> fragment_;
  std::shared_ptr<ScanOptions> scan_options_;
  std::shared_ptr<ScanContext> scan_context_;
};

/// \brief ScannerBuilder is a factory class to construct a Scanner. It is used
/// to pass information, notably a potential filter expression and a subset of
/// columns to materialize.
class ARROW_DS_EXPORT ScannerBuilder {
 public:
  ScannerBuilder(std::shared_ptr<Dataset> dataset,
                 std::shared_ptr<ScanContext> scan_context);

  ScannerBuilder(std::shared_ptr<Schema> schema, std::shared_ptr<Fragment> fragment,
                 std::shared_ptr<ScanContext> scan_context);

  /// \brief Set the subset of columns to materialize.
  ///
  /// This subset will be passed down to Sources and corresponding Fragments.
  /// The goal is to avoid loading/copying/deserializing columns that will
  /// not be required further down the compute chain.
  ///
  /// \param[in] columns list of columns to project. Order and duplicates will
  ///            be preserved.
  ///
  /// \return Failure if any column name does not exists in the dataset's
  ///         Schema.
  Status Project(std::vector<std::string> columns);

  /// \brief Set the filter expression to return only rows matching the filter.
  ///
  /// The predicate will be passed down to Sources and corresponding
  /// Fragments to exploit predicate pushdown if possible using
  /// partition information or Fragment internal metadata, e.g. Parquet statistics.
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

  /// \brief Set the maximum number of rows per RecordBatch.
  ///
  /// \param[in] batch_size the maximum number of rows.
  /// \returns An error if the number for batch is not greater than 0.
  ///
  /// This option provides a control limiting the memory owned by any RecordBatch.
  Status BatchSize(int64_t batch_size);

  /// \brief Return the constructed now-immutable Scanner object
  Result<std::shared_ptr<Scanner>> Finish() const;

  std::shared_ptr<Schema> schema() const { return scan_options_->schema(); }

 private:
  std::shared_ptr<Dataset> dataset_;
  std::shared_ptr<Fragment> fragment_;
  std::shared_ptr<ScanOptions> scan_options_;
  std::shared_ptr<ScanContext> scan_context_;
  bool has_projection_ = false;
  std::vector<std::string> project_columns_;
};

}  // namespace dataset
}  // namespace arrow
