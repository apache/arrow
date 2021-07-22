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

#include "arrow/compute/exec/expression.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/projector.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/io/interfaces.h"
#include "arrow/memory_pool.h"
#include "arrow/type_fwd.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/type_fwd.h"

namespace arrow {

using RecordBatchGenerator = std::function<Future<std::shared_ptr<RecordBatch>>()>;

namespace dataset {

/// \defgroup dataset-scanning Scanning API
///
/// @{

constexpr int64_t kDefaultBatchSize = 1 << 20;
constexpr int32_t kDefaultBatchReadahead = 32;
constexpr int32_t kDefaultFragmentReadahead = 8;

/// Scan-specific options, which can be changed between scans of the same dataset.
struct ARROW_DS_EXPORT ScanOptions {
  /// A row filter (which will be pushed down to partitioning/reading if supported).
  compute::Expression filter = compute::literal(true);
  /// A projection expression (which can add/remove/rename columns).
  compute::Expression projection;

  /// Schema with which batches will be read from fragments. This is also known as the
  /// "reader schema" it will be used (for example) in constructing CSV file readers to
  /// identify column types for parsing. Usually only a subset of its fields (see
  /// MaterializedFields) will be materialized during a scan.
  std::shared_ptr<Schema> dataset_schema;

  /// Schema of projected record batches. This is independent of dataset_schema as its
  /// fields are derived from the projection. For example, let
  ///
  ///   dataset_schema = {"a": int32, "b": int32, "id": utf8}
  ///   projection = project({equal(field_ref("a"), field_ref("b"))}, {"a_plus_b"})
  ///
  /// (no filter specified). In this case, the projected_schema would be
  ///
  ///   {"a_plus_b": int32}
  std::shared_ptr<Schema> projected_schema;

  /// Maximum row count for scanned batches.
  int64_t batch_size = kDefaultBatchSize;

  /// How many batches to read ahead within a file
  ///
  /// Set to 0 to disable batch readahead
  ///
  /// Note: May not be supported by all formats
  /// Note: May not be supported by all scanners
  /// Note: Will be ignored if use_threads is set to false
  int32_t batch_readahead = kDefaultBatchReadahead;

  /// How many files to read ahead
  ///
  /// Set to 0 to disable fragment readahead
  ///
  /// Note: May not be enforced by all scanners
  /// Note: Will be ignored if use_threads is set to false
  int32_t fragment_readahead = kDefaultFragmentReadahead;

  /// A pool from which materialized and scanned arrays will be allocated.
  MemoryPool* pool = arrow::default_memory_pool();

  /// IOContext for any IO tasks
  ///
  /// Note: The IOContext executor will be ignored if use_threads is set to false
  io::IOContext io_context;

  /// If true the scanner will scan in parallel
  ///
  /// Note: If true, this will use threads from both the cpu_executor and the
  /// io_context.executor
  /// Note: This  must be true in order for any readahead to happen
  bool use_threads = false;

  /// If true then an asycnhronous implementation of the scanner will be used.
  /// This implementation is newer and generally performs better.  However, it
  /// makes extensive use of threading and is still considered experimental
  bool use_async = false;

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

  // Return a threaded or serial TaskGroup according to use_threads.
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
  virtual Future<RecordBatchVector> SafeExecute(internal::Executor* executor);
  virtual Future<> SafeVisit(internal::Executor* executor,
                             std::function<Status(std::shared_ptr<RecordBatch>)> visitor);

  virtual ~ScanTask() = default;

  const std::shared_ptr<ScanOptions>& options() const { return options_; }
  const std::shared_ptr<Fragment>& fragment() const { return fragment_; }

 protected:
  ScanTask(std::shared_ptr<ScanOptions> options, std::shared_ptr<Fragment> fragment)
      : options_(std::move(options)), fragment_(std::move(fragment)) {}

  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<Fragment> fragment_;
};

/// \brief Combines a record batch with the fragment that the record batch originated
/// from
///
/// Knowing the source fragment can be useful for debugging & understanding loaded data
struct TaggedRecordBatch {
  std::shared_ptr<RecordBatch> record_batch;
  std::shared_ptr<Fragment> fragment;
};
using TaggedRecordBatchGenerator = std::function<Future<TaggedRecordBatch>()>;
using TaggedRecordBatchIterator = Iterator<TaggedRecordBatch>;

/// \brief Combines a tagged batch with positional information
///
/// This is returned when scanning batches in an unordered fashion.  This information is
/// needed if you ever want to reassemble the batches in order
struct EnumeratedRecordBatch {
  Enumerated<std::shared_ptr<RecordBatch>> record_batch;
  Enumerated<std::shared_ptr<Fragment>> fragment;
};
using EnumeratedRecordBatchGenerator = std::function<Future<EnumeratedRecordBatch>()>;
using EnumeratedRecordBatchIterator = Iterator<EnumeratedRecordBatch>;

/// @}

}  // namespace dataset

template <>
struct IterationTraits<dataset::TaggedRecordBatch> {
  static dataset::TaggedRecordBatch End() {
    return dataset::TaggedRecordBatch{NULLPTR, NULLPTR};
  }
  static bool IsEnd(const dataset::TaggedRecordBatch& val) {
    return val.record_batch == NULLPTR;
  }
};

template <>
struct IterationTraits<dataset::EnumeratedRecordBatch> {
  static dataset::EnumeratedRecordBatch End() {
    return dataset::EnumeratedRecordBatch{
        IterationEnd<Enumerated<std::shared_ptr<RecordBatch>>>(),
        IterationEnd<Enumerated<std::shared_ptr<dataset::Fragment>>>()};
  }
  static bool IsEnd(const dataset::EnumeratedRecordBatch& val) {
    return IsIterationEnd(val.fragment);
  }
};

namespace dataset {

/// \defgroup dataset-scanning Scanning API
///
/// @{

/// \brief A scanner glues together several dataset classes to load in data.
/// The dataset contains a collection of fragments and partitioning rules.
///
/// The fragments identify independently loadable units of data (i.e. each fragment has
/// a potentially unique schema and possibly even format.  It should be possible to read
/// fragments in parallel if desired).
///
/// The fragment's format contains the logic necessary to actually create a task to load
/// the fragment into memory.  That task may or may not support parallel execution of
/// its own.
///
/// The scanner is then responsible for creating scan tasks from every fragment in the
/// dataset and (potentially) sequencing the loaded record batches together.
///
/// The scanner should not buffer the entire dataset in memory (unless asked) instead
/// yielding record batches as soon as they are ready to scan.  Various readahead
/// properties control how much data is allowed to be scanned before pausing to let a
/// slow consumer catchup.
///
/// Today the scanner also handles projection & filtering although that may change in
/// the future.
class ARROW_DS_EXPORT Scanner {
 public:
  virtual ~Scanner() = default;

  /// \brief The Scan operator returns a stream of ScanTask. The caller is
  /// responsible to dispatch/schedule said tasks. Tasks should be safe to run
  /// in a concurrent fashion and outlive the iterator.
  ///
  /// Note: Not supported by the async scanner
  /// Planned for removal from the public API in ARROW-11782.
  ARROW_DEPRECATED("Deprecated in 4.0.0 for removal in 5.0.0. Use ScanBatches().")
  virtual Result<ScanTaskIterator> Scan();

  /// \brief Apply a visitor to each RecordBatch as it is scanned. If multiple threads
  /// are used (via use_threads), the visitor will be invoked from those threads and is
  /// responsible for any synchronization.
  virtual Status Scan(std::function<Status(TaggedRecordBatch)> visitor) = 0;
  /// \brief Convert a Scanner into a Table.
  ///
  /// Use this convenience utility with care. This will serially materialize the
  /// Scan result in memory before creating the Table.
  virtual Result<std::shared_ptr<Table>> ToTable() = 0;
  /// \brief Scan the dataset into a stream of record batches.  Each batch is tagged
  /// with the fragment it originated from.  The batches will arrive in order.  The
  /// order of fragments is determined by the dataset.
  ///
  /// Note: The scanner will perform some readahead but will avoid materializing too
  /// much in memory (this is goverended by the readahead options and use_threads option).
  /// If the readahead queue fills up then I/O will pause until the calling thread catches
  /// up.
  virtual Result<TaggedRecordBatchIterator> ScanBatches() = 0;
  virtual Result<TaggedRecordBatchGenerator> ScanBatchesAsync() = 0;
  /// \brief Scan the dataset into a stream of record batches.  Unlike ScanBatches this
  /// method may allow record batches to be returned out of order.  This allows for more
  /// efficient scanning: some fragments may be accessed more quickly than others (e.g.
  /// may be cached in RAM or just happen to get scheduled earlier by the I/O)
  ///
  /// To make up for the out-of-order iteration each batch is further tagged with
  /// positional information.
  virtual Result<EnumeratedRecordBatchIterator> ScanBatchesUnordered();
  virtual Result<EnumeratedRecordBatchGenerator> ScanBatchesUnorderedAsync() = 0;
  /// \brief A convenience to synchronously load the given rows by index.
  ///
  /// Will only consume as many batches as needed from ScanBatches().
  virtual Result<std::shared_ptr<Table>> TakeRows(const Array& indices);
  /// \brief Get the first N rows.
  virtual Result<std::shared_ptr<Table>> Head(int64_t num_rows);
  /// \brief Count rows matching a predicate.
  ///
  /// This method will push down the predicate and compute the result based on fragment
  /// metadata if possible.
  virtual Result<int64_t> CountRows();
  /// \brief Convert the Scanner to a RecordBatchReader so it can be
  /// easily used with APIs that expect a reader.
  Result<std::shared_ptr<RecordBatchReader>> ToRecordBatchReader();

  /// \brief Get the options for this scan.
  const std::shared_ptr<ScanOptions>& options() const { return scan_options_; }

 protected:
  explicit Scanner(std::shared_ptr<ScanOptions> scan_options)
      : scan_options_(std::move(scan_options)) {}

  Result<EnumeratedRecordBatchIterator> AddPositioningToInOrderScan(
      TaggedRecordBatchIterator scan);

  const std::shared_ptr<ScanOptions> scan_options_;
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

  /// \brief Make a scanner from a record batch reader.
  ///
  /// The resulting scanner can be scanned only once. This is intended
  /// to support writing data from streaming sources or other sources
  /// that can be iterated only once.
  static std::shared_ptr<ScannerBuilder> FromRecordBatchReader(
      std::shared_ptr<RecordBatchReader> reader);

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

  /// \brief Set expressions which will be evaluated to produce the materialized
  /// columns.
  ///
  /// Columns which are not referenced may not be read from fragments.
  ///
  /// \param[in] exprs expressions to evaluate to produce columns.
  /// \param[in] names list of names for the resulting columns.
  ///
  /// \return Failure if any referenced column does not exists in the dataset's
  ///         Schema.
  Status Project(std::vector<compute::Expression> exprs, std::vector<std::string> names);

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
  Status Filter(const compute::Expression& filter);

  /// \brief Indicate if the Scanner should make use of the available
  ///        ThreadPool found in ScanOptions;
  Status UseThreads(bool use_threads = true);

  /// \brief Limit how many fragments the scanner will read at once
  ///
  /// Note: This is only enforced in "async" mode
  Status FragmentReadahead(int fragment_readahead);

  /// \brief Indicate if the Scanner should run in experimental "async" mode
  ///
  /// This mode should have considerably better performance on high-latency or parallel
  /// filesystems but is still experimental
  Status UseAsync(bool use_async = true);

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
  const std::shared_ptr<Schema>& projected_schema() const;

 private:
  std::shared_ptr<Dataset> dataset_;
  std::shared_ptr<ScanOptions> scan_options_ = std::make_shared<ScanOptions>();
};

/// \brief Construct a source ExecNode which yields batches from a dataset scan.
///
/// Does not construct associated filter or project nodes.
/// Yielded batches will be augmented with fragment/batch indices to enable stable
/// ordering for simple ExecPlans.
ARROW_DS_EXPORT Result<compute::ExecNode*> MakeScanNode(compute::ExecPlan*,
                                                        std::shared_ptr<Dataset>,
                                                        std::shared_ptr<ScanOptions>);

/// \brief Construct a ProjectNode which preserves fragment/batch indices.
ARROW_DS_EXPORT Result<compute::ExecNode*> MakeAugmentedProjectNode(
    compute::ExecNode* input, std::string label, std::vector<compute::Expression> exprs,
    std::vector<std::string> names = {});

/// \brief Add a sink node which forwards to an AsyncGenerator<ExecBatch>
///
/// Emitted batches will be ordered by fragment and batch indices, or an error
/// will be raised if those fields are not available in the input.
ARROW_DS_EXPORT Result<AsyncGenerator<util::optional<compute::ExecBatch>>>
MakeOrderedSinkNode(compute::ExecNode*, std::string label);

/// @}

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

}  // namespace dataset
}  // namespace arrow
