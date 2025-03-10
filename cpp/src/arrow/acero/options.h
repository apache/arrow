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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/type_fwd.h"
#include "arrow/acero/visibility.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/result.h"
#include "arrow/util/future.h"

namespace arrow {

using compute::Aggregate;
using compute::ExecBatch;
using compute::Expression;
using compute::literal;
using compute::Ordering;
using compute::SelectKOptions;
using compute::SortOptions;

namespace internal {

class Executor;

}  // namespace internal

namespace acero {

/// \brief This must not be used in release-mode
struct DebugOptions;

using AsyncExecBatchGenerator = std::function<Future<std::optional<ExecBatch>>()>;

/// \addtogroup acero-nodes
/// @{

/// \brief A base class for all options objects
///
/// The only time this is used directly is when a node has no configuration
class ARROW_ACERO_EXPORT ExecNodeOptions {
 public:
  virtual ~ExecNodeOptions() = default;

  /// \brief This must not be used in release-mode
  std::shared_ptr<DebugOptions> debug_opts;
};

/// \brief A node representing a generic source of data for Acero
///
/// The source node will start calling `generator` during StartProducing.  An initial
/// task will be created that will call `generator`.  It will not call `generator`
/// reentrantly.  If the source can be read in parallel then those details should be
/// encapsulated within `generator`.
///
/// For each batch received a new task will be created to push that batch downstream.
/// This task will slice smaller units of size `ExecPlan::kMaxBatchSize` from the
/// parent batch and call InputReceived.  Thus, if the `generator` yields a large
/// batch it may result in several calls to InputReceived.
///
/// The SourceNode will, by default, assign an implicit ordering to outgoing batches.
/// This is valid as long as the generator generates batches in a deterministic fashion.
/// Currently, the only way to override this is to subclass the SourceNode.
///
/// This node is not generally used directly but can serve as the basis for various
/// specialized nodes.
class ARROW_ACERO_EXPORT SourceNodeOptions : public ExecNodeOptions {
 public:
  /// Create an instance from values
  SourceNodeOptions(std::shared_ptr<Schema> output_schema,
                    std::function<Future<std::optional<ExecBatch>>()> generator,
                    Ordering ordering = Ordering::Unordered())
      : output_schema(std::move(output_schema)),
        generator(std::move(generator)),
        ordering(std::move(ordering)) {}

  /// \brief the schema for batches that will be generated by this source
  std::shared_ptr<Schema> output_schema;
  /// \brief an asynchronous stream of batches ending with std::nullopt
  std::function<Future<std::optional<ExecBatch>>()> generator;
  /// \brief the order of the data, defaults to Ordering::Unordered
  Ordering ordering;
};

/// \brief a node that generates data from a table already loaded in memory
///
/// The table source node will slice off chunks, defined by `max_batch_size`
/// for parallel processing.  The table source node extends source node and so these
/// chunks will be iteratively processed in small batches.  \see SourceNodeOptions
/// for details.
class ARROW_ACERO_EXPORT TableSourceNodeOptions : public ExecNodeOptions {
 public:
  static constexpr int64_t kDefaultMaxBatchSize = 1 << 20;

  /// Create an instance from values
  TableSourceNodeOptions(std::shared_ptr<Table> table,
                         int64_t max_batch_size = kDefaultMaxBatchSize)
      : table(std::move(table)), max_batch_size(max_batch_size) {}

  /// \brief a table which acts as the data source
  std::shared_ptr<Table> table;
  /// \brief size of batches to emit from this node
  /// If the table is larger the node will emit multiple batches from the
  /// the table to be processed in parallel.
  int64_t max_batch_size;
};

/// \brief define a lazily resolved Arrow table.
///
/// The table uniquely identified by the names can typically be resolved at the time when
/// the plan is to be consumed.
///
/// This node is for serialization purposes only and can never be executed.
class ARROW_ACERO_EXPORT NamedTableNodeOptions : public ExecNodeOptions {
 public:
  /// Create an instance from values
  NamedTableNodeOptions(std::vector<std::string> names, std::shared_ptr<Schema> schema)
      : names(std::move(names)), schema(std::move(schema)) {}

  /// \brief the names to put in the serialized plan
  std::vector<std::string> names;
  /// \brief the output schema of the table
  std::shared_ptr<Schema> schema;
};

/// \brief a source node which feeds data from a synchronous iterator of batches
///
/// ItMaker is a maker of an iterator of tabular data.
///
/// The node can be configured to use an I/O executor.  If set then each time the
/// iterator is polled a new I/O thread task will be created to do the polling.  This
/// allows a blocking iterator to stay off the CPU thread pool.
template <typename ItMaker>
class ARROW_ACERO_EXPORT SchemaSourceNodeOptions : public ExecNodeOptions {
 public:
  /// Create an instance that will create a new task on io_executor for each iteration
  SchemaSourceNodeOptions(std::shared_ptr<Schema> schema, ItMaker it_maker,
                          arrow::internal::Executor* io_executor)
      : schema(std::move(schema)),
        it_maker(std::move(it_maker)),
        io_executor(io_executor),
        requires_io(true) {}

  /// Create an instance that will either iterate synchronously or use the default I/O
  /// executor
  SchemaSourceNodeOptions(std::shared_ptr<Schema> schema, ItMaker it_maker,
                          bool requires_io = false)
      : schema(std::move(schema)),
        it_maker(std::move(it_maker)),
        io_executor(NULLPTR),
        requires_io(requires_io) {}

  /// \brief The schema of the record batches from the iterator
  std::shared_ptr<Schema> schema;

  /// \brief A maker of an iterator which acts as the data source
  ItMaker it_maker;

  /// \brief The executor to use for scanning the iterator
  ///
  /// Defaults to the default I/O executor.  Only used if requires_io is true.
  /// If requires_io is false then this MUST be nullptr.
  arrow::internal::Executor* io_executor;

  /// \brief If true then items will be fetched from the iterator on a dedicated I/O
  ///        thread to keep I/O off the CPU thread
  bool requires_io;
};

/// a source node that reads from a RecordBatchReader
///
/// Each iteration of the RecordBatchReader will be run on a new thread task created
/// on the I/O thread pool.
class ARROW_ACERO_EXPORT RecordBatchReaderSourceNodeOptions : public ExecNodeOptions {
 public:
  /// Create an instance from values
  RecordBatchReaderSourceNodeOptions(std::shared_ptr<RecordBatchReader> reader,
                                     arrow::internal::Executor* io_executor = NULLPTR)
      : reader(std::move(reader)), io_executor(io_executor) {}

  /// \brief The RecordBatchReader which acts as the data source
  std::shared_ptr<RecordBatchReader> reader;

  /// \brief The executor to use for the reader
  ///
  /// Defaults to the default I/O executor.
  arrow::internal::Executor* io_executor;
};

/// a source node that reads from an iterator of array vectors
using ArrayVectorIteratorMaker = std::function<Iterator<std::shared_ptr<ArrayVector>>()>;
/// \brief An extended Source node which accepts a schema and array-vectors
class ARROW_ACERO_EXPORT ArrayVectorSourceNodeOptions
    : public SchemaSourceNodeOptions<ArrayVectorIteratorMaker> {
  using SchemaSourceNodeOptions::SchemaSourceNodeOptions;
};

/// a source node that reads from an iterator of ExecBatch
using ExecBatchIteratorMaker = std::function<Iterator<std::shared_ptr<ExecBatch>>()>;
/// \brief An extended Source node which accepts a schema and exec-batches
class ARROW_ACERO_EXPORT ExecBatchSourceNodeOptions
    : public SchemaSourceNodeOptions<ExecBatchIteratorMaker> {
 public:
  using SchemaSourceNodeOptions::SchemaSourceNodeOptions;
  ExecBatchSourceNodeOptions(std::shared_ptr<Schema> schema,
                             std::vector<ExecBatch> batches,
                             ::arrow::internal::Executor* io_executor);
  ExecBatchSourceNodeOptions(std::shared_ptr<Schema> schema,
                             std::vector<ExecBatch> batches, bool requires_io = false);
};

using RecordBatchIteratorMaker = std::function<Iterator<std::shared_ptr<RecordBatch>>()>;
/// a source node that reads from an iterator of RecordBatch
class ARROW_ACERO_EXPORT RecordBatchSourceNodeOptions
    : public SchemaSourceNodeOptions<RecordBatchIteratorMaker> {
  using SchemaSourceNodeOptions::SchemaSourceNodeOptions;
};

/// \brief a node which excludes some rows from batches passed through it
///
/// filter_expression will be evaluated against each batch which is pushed to
/// this node. Any rows for which filter_expression does not evaluate to `true` will be
/// excluded in the batch emitted by this node.
///
/// This node will emit empty batches if all rows are excluded.  This is done
/// to avoid gaps in the ordering.
class ARROW_ACERO_EXPORT FilterNodeOptions : public ExecNodeOptions {
 public:
  /// \brief create an instance from values
  explicit FilterNodeOptions(Expression filter_expression)
      : filter_expression(std::move(filter_expression)) {}

  /// \brief the expression to filter batches
  ///
  /// The return type of this expression must be boolean
  Expression filter_expression;
};

/// \brief a node which selects a specified subset from the input
class ARROW_ACERO_EXPORT FetchNodeOptions : public ExecNodeOptions {
 public:
  static constexpr std::string_view kName = "fetch";
  /// \brief create an instance from values
  FetchNodeOptions(int64_t offset, int64_t count) : offset(offset), count(count) {}
  /// \brief the number of rows to skip
  int64_t offset;
  /// \brief the number of rows to keep (not counting skipped rows)
  int64_t count;
};

/// \brief a node which executes expressions on input batches, producing batches
/// of the same length with new columns.
///
/// Each expression will be evaluated against each batch which is pushed to
/// this node to produce a corresponding output column.
///
/// If names are not provided, the string representations of exprs will be used.
class ARROW_ACERO_EXPORT ProjectNodeOptions : public ExecNodeOptions {
 public:
  /// \brief create an instance from values
  explicit ProjectNodeOptions(std::vector<Expression> expressions,
                              std::vector<std::string> names = {})
      : expressions(std::move(expressions)), names(std::move(names)) {}

  /// \brief the expressions to run on the batches
  ///
  /// The output will have one column for each expression.  If you wish to keep any of
  /// the columns from the input then you should create a simple field_ref expression
  /// for that column.
  std::vector<Expression> expressions;
  /// \brief the names of the output columns
  ///
  /// If this is not specified then the result of calling ToString on the expression will
  /// be used instead
  ///
  /// This list should either be empty or have the same length as `expressions`
  std::vector<std::string> names;
};

/// \brief a node which aggregates input batches and calculates summary statistics
///
/// The node can summarize the entire input or it can group the input with grouping keys
/// and segment keys.
///
/// By default, the aggregate node is a pipeline breaker.  It must accumulate all input
/// before any output is produced.  Segment keys are a performance optimization.  If
/// you know your input is already partitioned by one or more columns then you can
/// specify these as segment keys.  At each change in the segment keys the node will
/// emit values for all data seen so far.
///
/// Segment keys are currently limited to single-threaded mode.
///
/// Both keys and segment-keys determine the group.  However segment-keys are also used
/// for determining grouping segments, which should be large, and allow streaming a
/// partial aggregation result after processing each segment.  One common use-case for
/// segment-keys is ordered aggregation, in which the segment-key attribute specifies a
/// column with non-decreasing values or a lexicographically-ordered set of such columns.
///
/// If the keys attribute is a non-empty vector, then each aggregate in `aggregates` is
/// expected to be a HashAggregate function. If the keys attribute is an empty vector,
/// then each aggregate is assumed to be a ScalarAggregate function.
///
/// If the segment_keys attribute is a non-empty vector, then segmented aggregation, as
/// described above, applies.
///
/// The keys and segment_keys vectors must be disjoint.
///
/// If no measures are provided then you will simply get the list of unique keys.
///
/// This node outputs segment keys first, followed by regular keys, followed by one
/// column for each aggregate.
class ARROW_ACERO_EXPORT AggregateNodeOptions : public ExecNodeOptions {
 public:
  /// \brief create an instance from values
  explicit AggregateNodeOptions(std::vector<Aggregate> aggregates,
                                std::vector<FieldRef> keys = {},
                                std::vector<FieldRef> segment_keys = {})
      : aggregates(std::move(aggregates)),
        keys(std::move(keys)),
        segment_keys(std::move(segment_keys)) {}

  // aggregations which will be applied to the targeted fields
  std::vector<Aggregate> aggregates;
  // keys by which aggregations will be grouped (optional)
  std::vector<FieldRef> keys;
  // keys by which aggregations will be segmented (optional)
  std::vector<FieldRef> segment_keys;
};

/// \brief a default value at which backpressure will be applied
constexpr int32_t kDefaultBackpressureHighBytes = 1 << 30;  // 1GiB
/// \brief a default value at which backpressure will be removed
constexpr int32_t kDefaultBackpressureLowBytes = 1 << 28;  // 256MiB

/// \brief an interface that can be queried for backpressure statistics
class ARROW_ACERO_EXPORT BackpressureMonitor {
 public:
  virtual ~BackpressureMonitor() = default;
  /// \brief fetches the number of bytes currently queued up
  virtual uint64_t bytes_in_use() = 0;
  /// \brief checks to see if backpressure is currently applied
  virtual bool is_paused() = 0;
};

/// \brief Options to control backpressure behavior
struct ARROW_ACERO_EXPORT BackpressureOptions {
  /// \brief Create default options that perform no backpressure
  BackpressureOptions() : resume_if_below(0), pause_if_above(0) {}
  /// \brief Create options that will perform backpressure
  ///
  /// \param resume_if_below The producer should resume producing if the backpressure
  ///                        queue has fewer than resume_if_below items.
  /// \param pause_if_above The producer should pause producing if the backpressure
  ///                       queue has more than pause_if_above items
  BackpressureOptions(uint64_t resume_if_below, uint64_t pause_if_above)
      : resume_if_below(resume_if_below), pause_if_above(pause_if_above) {}

  /// \brief create an instance using default values for backpressure limits
  static BackpressureOptions DefaultBackpressure() {
    return BackpressureOptions(kDefaultBackpressureLowBytes,
                               kDefaultBackpressureHighBytes);
  }

  /// \brief helper method to determine if backpressure is disabled
  /// \return true if pause_if_above is greater than zero, false otherwise
  bool should_apply_backpressure() const { return pause_if_above > 0; }

  /// \brief the number of bytes at which the producer should resume producing
  uint64_t resume_if_below;
  /// \brief the number of bytes at which the producer should pause producing
  ///
  /// If this is <= 0 then backpressure will be disabled
  uint64_t pause_if_above;
};

/// \brief a sink node which collects results in a queue
///
/// Emitted batches will only be ordered if there is a meaningful ordering
/// and sequence_output is not set to false.
class ARROW_ACERO_EXPORT SinkNodeOptions : public ExecNodeOptions {
 public:
  explicit SinkNodeOptions(std::function<Future<std::optional<ExecBatch>>()>* generator,
                           std::shared_ptr<Schema>* schema,
                           BackpressureOptions backpressure = {},
                           BackpressureMonitor** backpressure_monitor = NULLPTR,
                           std::optional<bool> sequence_output = std::nullopt)
      : generator(generator),
        schema(schema),
        backpressure(backpressure),
        backpressure_monitor(backpressure_monitor),
        sequence_output(sequence_output) {}

  explicit SinkNodeOptions(std::function<Future<std::optional<ExecBatch>>()>* generator,
                           BackpressureOptions backpressure = {},
                           BackpressureMonitor** backpressure_monitor = NULLPTR,
                           std::optional<bool> sequence_output = std::nullopt)
      : generator(generator),
        schema(NULLPTR),
        backpressure(std::move(backpressure)),
        backpressure_monitor(backpressure_monitor),
        sequence_output(sequence_output) {}

  /// \brief A pointer to a generator of batches.
  ///
  /// This will be set when the node is added to the plan and should be used to consume
  /// data from the plan.  If this function is not called frequently enough then the sink
  /// node will start to accumulate data and may apply backpressure.
  std::function<Future<std::optional<ExecBatch>>()>* generator;
  /// \brief A pointer which will be set to the schema of the generated batches
  ///
  /// This is optional, if nullptr is passed in then it will be ignored.
  /// This will be set when the node is added to the plan, before StartProducing is called
  std::shared_ptr<Schema>* schema;
  /// \brief Options to control when to apply backpressure
  ///
  /// This is optional, the default is to never apply backpressure.  If the plan is not
  /// consumed quickly enough the system may eventually run out of memory.
  BackpressureOptions backpressure;
  /// \brief A pointer to a backpressure monitor
  ///
  /// This will be set when the node is added to the plan.  This can be used to inspect
  /// the amount of data currently queued in the sink node.  This is an optional utility
  /// and backpressure can be applied even if this is not used.
  BackpressureMonitor** backpressure_monitor;
  /// \brief Controls whether batches should be emitted immediately or sequenced in order
  ///
  /// \see QueryOptions for more details
  std::optional<bool> sequence_output;
};

/// \brief Control used by a SinkNodeConsumer to pause & resume
///
/// Callers should ensure that they do not call Pause and Resume simultaneously and they
/// should sequence things so that a call to Pause() is always followed by an eventual
/// call to Resume()
class ARROW_ACERO_EXPORT BackpressureControl {
 public:
  virtual ~BackpressureControl() = default;
  /// \brief Ask the input to pause
  ///
  /// This is best effort, batches may continue to arrive
  /// Must eventually be followed by a call to Resume() or deadlock will occur
  virtual void Pause() = 0;
  /// \brief Ask the input to resume
  virtual void Resume() = 0;
};

/// \brief a sink node that consumes the data as part of the plan using callbacks
class ARROW_ACERO_EXPORT SinkNodeConsumer {
 public:
  virtual ~SinkNodeConsumer() = default;
  /// \brief Prepare any consumer state
  ///
  /// This will be run once the schema is finalized as the plan is starting and
  /// before any calls to Consume.  A common use is to save off the schema so that
  /// batches can be interpreted.
  virtual Status Init(const std::shared_ptr<Schema>& schema,
                      BackpressureControl* backpressure_control, ExecPlan* plan) = 0;
  /// \brief Consume a batch of data
  virtual Status Consume(ExecBatch batch) = 0;
  /// \brief Signal to the consumer that the last batch has been delivered
  ///
  /// The returned future should only finish when all outstanding tasks have completed
  ///
  /// If the plan is ended early or aborts due to an error then this will not be
  /// called.
  virtual Future<> Finish() = 0;
};

/// \brief Add a sink node which consumes data within the exec plan run
class ARROW_ACERO_EXPORT ConsumingSinkNodeOptions : public ExecNodeOptions {
 public:
  explicit ConsumingSinkNodeOptions(std::shared_ptr<SinkNodeConsumer> consumer,
                                    std::vector<std::string> names = {},
                                    std::optional<bool> sequence_output = std::nullopt)
      : consumer(std::move(consumer)),
        names(std::move(names)),
        sequence_output(sequence_output) {}

  std::shared_ptr<SinkNodeConsumer> consumer;
  /// \brief Names to rename the sink's schema fields to
  ///
  /// If specified then names must be provided for all fields. Currently, only a flat
  /// schema is supported (see GH-31875).
  ///
  /// If not specified then names will be generated based on the source data.
  std::vector<std::string> names;
  /// \brief Controls whether batches should be emitted immediately or sequenced in order
  ///
  /// \see QueryOptions for more details
  std::optional<bool> sequence_output;
};

/// \brief Make a node which sorts rows passed through it
///
/// All batches pushed to this node will be accumulated, then sorted, by the given
/// fields. Then sorted batches will be forwarded to the generator in sorted order.
class ARROW_ACERO_EXPORT OrderBySinkNodeOptions : public SinkNodeOptions {
 public:
  /// \brief create an instance from values
  explicit OrderBySinkNodeOptions(
      SortOptions sort_options,
      std::function<Future<std::optional<ExecBatch>>()>* generator)
      : SinkNodeOptions(generator), sort_options(std::move(sort_options)) {}

  /// \brief options describing which columns and direction to sort
  SortOptions sort_options;
};

/// \brief Apply a new ordering to data
///
/// Currently this node works by accumulating all data, sorting, and then emitting
/// the new data with an updated batch index.
///
/// Larger-than-memory sort is not currently supported.
class ARROW_ACERO_EXPORT OrderByNodeOptions : public ExecNodeOptions {
 public:
  static constexpr std::string_view kName = "order_by";
  explicit OrderByNodeOptions(Ordering ordering) : ordering(std::move(ordering)) {}

  /// \brief The new ordering to apply to outgoing data
  Ordering ordering;
};

enum class JoinType {
  LEFT_SEMI,
  RIGHT_SEMI,
  LEFT_ANTI,
  RIGHT_ANTI,
  INNER,
  LEFT_OUTER,
  RIGHT_OUTER,
  FULL_OUTER
};

std::string ToString(JoinType t);

enum class JoinKeyCmp { EQ, IS };

/// \brief a node which implements a join operation using a hash table
class ARROW_ACERO_EXPORT HashJoinNodeOptions : public ExecNodeOptions {
 public:
  static constexpr const char* default_output_suffix_for_left = "";
  static constexpr const char* default_output_suffix_for_right = "";
  /// \brief create an instance from values that outputs all columns
  HashJoinNodeOptions(
      JoinType in_join_type, std::vector<FieldRef> in_left_keys,
      std::vector<FieldRef> in_right_keys, Expression filter = literal(true),
      std::string output_suffix_for_left = default_output_suffix_for_left,
      std::string output_suffix_for_right = default_output_suffix_for_right,
      bool disable_bloom_filter = false)
      : join_type(in_join_type),
        left_keys(std::move(in_left_keys)),
        right_keys(std::move(in_right_keys)),
        output_all(true),
        output_suffix_for_left(std::move(output_suffix_for_left)),
        output_suffix_for_right(std::move(output_suffix_for_right)),
        filter(std::move(filter)),
        disable_bloom_filter(disable_bloom_filter) {
    this->key_cmp.resize(this->left_keys.size());
    for (size_t i = 0; i < this->left_keys.size(); ++i) {
      this->key_cmp[i] = JoinKeyCmp::EQ;
    }
  }
  /// \brief create an instance from keys
  ///
  /// This will create an inner join that outputs all columns and has no post join filter
  ///
  /// `in_left_keys` should have the same length and types as `in_right_keys`
  /// @param in_left_keys the keys in the left input
  /// @param in_right_keys the keys in the right input
  HashJoinNodeOptions(std::vector<FieldRef> in_left_keys,
                      std::vector<FieldRef> in_right_keys)
      : left_keys(std::move(in_left_keys)), right_keys(std::move(in_right_keys)) {
    this->join_type = JoinType::INNER;
    this->output_all = true;
    this->output_suffix_for_left = default_output_suffix_for_left;
    this->output_suffix_for_right = default_output_suffix_for_right;
    this->key_cmp.resize(this->left_keys.size());
    for (size_t i = 0; i < this->left_keys.size(); ++i) {
      this->key_cmp[i] = JoinKeyCmp::EQ;
    }
    this->filter = literal(true);
  }
  /// \brief create an instance from values using JoinKeyCmp::EQ for all comparisons
  HashJoinNodeOptions(
      JoinType join_type, std::vector<FieldRef> left_keys,
      std::vector<FieldRef> right_keys, std::vector<FieldRef> left_output,
      std::vector<FieldRef> right_output, Expression filter = literal(true),
      std::string output_suffix_for_left = default_output_suffix_for_left,
      std::string output_suffix_for_right = default_output_suffix_for_right,
      bool disable_bloom_filter = false)
      : join_type(join_type),
        left_keys(std::move(left_keys)),
        right_keys(std::move(right_keys)),
        output_all(false),
        left_output(std::move(left_output)),
        right_output(std::move(right_output)),
        output_suffix_for_left(std::move(output_suffix_for_left)),
        output_suffix_for_right(std::move(output_suffix_for_right)),
        filter(std::move(filter)),
        disable_bloom_filter(disable_bloom_filter) {
    this->key_cmp.resize(this->left_keys.size());
    for (size_t i = 0; i < this->left_keys.size(); ++i) {
      this->key_cmp[i] = JoinKeyCmp::EQ;
    }
  }
  /// \brief create an instance from values
  HashJoinNodeOptions(
      JoinType join_type, std::vector<FieldRef> left_keys,
      std::vector<FieldRef> right_keys, std::vector<FieldRef> left_output,
      std::vector<FieldRef> right_output, std::vector<JoinKeyCmp> key_cmp,
      Expression filter = literal(true),
      std::string output_suffix_for_left = default_output_suffix_for_left,
      std::string output_suffix_for_right = default_output_suffix_for_right,
      bool disable_bloom_filter = false)
      : join_type(join_type),
        left_keys(std::move(left_keys)),
        right_keys(std::move(right_keys)),
        output_all(false),
        left_output(std::move(left_output)),
        right_output(std::move(right_output)),
        key_cmp(std::move(key_cmp)),
        output_suffix_for_left(std::move(output_suffix_for_left)),
        output_suffix_for_right(std::move(output_suffix_for_right)),
        filter(std::move(filter)),
        disable_bloom_filter(disable_bloom_filter) {}

  HashJoinNodeOptions() = default;

  // type of join (inner, left, semi...)
  JoinType join_type = JoinType::INNER;
  // key fields from left input
  std::vector<FieldRef> left_keys;
  // key fields from right input
  std::vector<FieldRef> right_keys;
  // if set all valid fields from both left and right input will be output
  // (and field ref vectors for output fields will be ignored)
  bool output_all = false;
  // output fields passed from left input
  std::vector<FieldRef> left_output;
  // output fields passed from right input
  std::vector<FieldRef> right_output;
  // key comparison function (determines whether a null key is equal another null
  // key or not)
  std::vector<JoinKeyCmp> key_cmp;
  // suffix added to names of output fields coming from left input (used to distinguish,
  // if necessary, between fields of the same name in left and right input and can be left
  // empty if there are no name collisions)
  std::string output_suffix_for_left;
  // suffix added to names of output fields coming from right input
  std::string output_suffix_for_right;
  // residual filter which is applied to matching rows.  Rows that do not match
  // the filter are not included.  The filter is applied against the
  // concatenated input schema (left fields then right fields) and can reference
  // fields that are not included in the output.
  Expression filter = literal(true);
  // whether or not to disable Bloom filters in this join
  bool disable_bloom_filter = false;
};

/// \brief a node which implements the asof join operation
///
/// Note, this API is experimental and will change in the future
///
/// This node takes one left table and any number of right tables, and asof joins them
/// together. Batches produced by each input must be ordered by the "on" key.
/// This node will output one row for each row in the left table.
class ARROW_ACERO_EXPORT AsofJoinNodeOptions : public ExecNodeOptions {
 public:
  /// \brief Keys for one input table of the AsofJoin operation
  ///
  /// The keys must be consistent across the input tables:
  /// Each "on" key must refer to a field of the same type and units across the tables.
  /// Each "by" key must refer to a list of fields of the same types across the tables.
  struct Keys {
    /// \brief "on" key for the join.
    ///
    /// The input table must be sorted by the "on" key. Must be a single field of a common
    /// type. Inexact match is used on the "on" key. i.e., a row is considered a match iff
    /// left_on - tolerance <= right_on <= left_on.
    /// Currently, the "on" key must be of an integer, date, or timestamp type.
    FieldRef on_key;
    /// \brief "by" key for the join.
    ///
    /// Each input table must have each field of the "by" key.  Exact equality is used for
    /// each field of the "by" key.
    /// Currently, each field of the "by" key must be of an integer, date, timestamp, or
    /// base-binary type.
    std::vector<FieldRef> by_key;
  };

  AsofJoinNodeOptions(std::vector<Keys> input_keys, int64_t tolerance)
      : input_keys(std::move(input_keys)), tolerance(tolerance) {}

  /// \brief AsofJoin keys per input table. At least two keys must be given. The first key
  /// corresponds to a left table and all other keys correspond to right tables for the
  /// as-of-join.
  ///
  /// \see `Keys` for details.
  std::vector<Keys> input_keys;
  /// \brief Tolerance for inexact "on" key matching. A right row is considered a match
  /// with the left row if `right.on - left.on <= tolerance`. The `tolerance` may be:
  /// - negative, in which case a past-as-of-join occurs;
  /// - or positive, in which case a future-as-of-join occurs;
  /// - or zero, in which case an exact-as-of-join occurs.
  ///
  /// The tolerance is interpreted in the same units as the "on" key.
  int64_t tolerance;
};

/// \brief a node which select top_k/bottom_k rows passed through it
///
/// All batches pushed to this node will be accumulated, then selected, by the given
/// fields. Then sorted batches will be forwarded to the generator in sorted order.
class ARROW_ACERO_EXPORT SelectKSinkNodeOptions : public SinkNodeOptions {
 public:
  explicit SelectKSinkNodeOptions(
      SelectKOptions select_k_options,
      std::function<Future<std::optional<ExecBatch>>()>* generator)
      : SinkNodeOptions(generator), select_k_options(std::move(select_k_options)) {}

  /// SelectK options
  SelectKOptions select_k_options;
};

/// \brief a sink node which accumulates all output into a table
class ARROW_ACERO_EXPORT TableSinkNodeOptions : public ExecNodeOptions {
 public:
  /// \brief create an instance from values
  explicit TableSinkNodeOptions(std::shared_ptr<Table>* output_table,
                                std::optional<bool> sequence_output = std::nullopt)
      : output_table(output_table), sequence_output(sequence_output) {}

  /// \brief an "out parameter" specifying the table that will be created
  ///
  /// Must not be null and remain valid for the entirety of the plan execution.  After the
  /// plan has completed this will be set to point to the result table
  std::shared_ptr<Table>* output_table;
  /// \brief Controls whether batches should be emitted immediately or sequenced in order
  ///
  /// \see QueryOptions for more details
  std::optional<bool> sequence_output;
  /// \brief Custom names to use for the columns.
  ///
  /// If specified then names must be provided for all fields. Currently, only a flat
  /// schema is supported (see GH-31875).
  ///
  /// If not specified then names will be generated based on the source data.
  std::vector<std::string> names;
};

/// \brief a row template that describes one row that will be generated for each input row
struct ARROW_ACERO_EXPORT PivotLongerRowTemplate {
  PivotLongerRowTemplate(std::vector<std::string> feature_values,
                         std::vector<std::optional<FieldRef>> measurement_values)
      : feature_values(std::move(feature_values)),
        measurement_values(std::move(measurement_values)) {}
  /// A (typically unique) set of feature values for the template, usually derived from a
  /// column name
  ///
  /// These will be used to populate the feature columns
  std::vector<std::string> feature_values;
  /// The fields containing the measurements to use for this row
  ///
  /// These will be used to populate the measurement columns.  If nullopt then nulls
  /// will be inserted for the given value.
  std::vector<std::optional<FieldRef>> measurement_values;
};

/// \brief Reshape a table by turning some columns into additional rows
///
/// This operation is sometimes also referred to as UNPIVOT
///
/// This is typically done when there are multiple observations in each row in order to
/// transform to a table containing a single observation per row.
///
/// For example:
///
/// | time | left_temp | right_temp |
/// | ---- | --------- | ---------- |
/// | 1    | 10        | 20         |
/// | 2    | 15        | 18         |
///
/// The above table contains two observations per row.  There is an implicit feature
/// "location" (left vs right) and a measurement "temp".  What we really want is:
///
/// | time | location | temp |
/// | ---  | ---      | ---  |
/// | 1    | left     | 10   |
/// | 1    | right    | 20   |
/// | 2    | left     | 15   |
/// | 2    | right    | 18   |
///
/// For a more complex example consider:
///
/// | time | ax1 | ay1 | bx1 | ay2 |
/// | ---- | --- | --- | --- | --- |
/// | 0    | 1   | 2   | 3   | 4   |
///
/// We can pretend a vs b and x vs y are features while 1 and 2 are two different
/// kinds of measurements.  We thus want to pivot to
///
/// | time | a/b | x/y |  f1  |  f2  |
/// | ---- | --- | --- | ---- | ---- |
/// | 0    | a   | x   | 1    | null |
/// | 0    | a   | y   | 2    | 4    |
/// | 0    | b   | x   | 3    | null |
///
/// To do this we create a row template for each combination of features.  One should
/// be able to do this purely by looking at the column names.  For example, given the
/// above columns "ax1", "ay1", "bx1", and "ay2" we know we have three feature
/// combinations (a, x), (a, y), and (b, x).  Similarly, we know we have two possible
/// measurements, "1" and "2".
///
/// For each combination of features we create a row template.  In each row template we
/// describe the combination and then list which columns to use for the measurements.
/// If a measurement doesn't exist for a given combination then we use nullopt.
///
/// So, for our above example, we have:
///
/// (a, x): names={"a", "x"}, values={"ax1", nullopt}
/// (a, y): names={"a", "y"}, values={"ay1", "ay2"}
/// (b, x): names={"b", "x"}, values={"bx1", nullopt}
///
/// Finishing it off we name our new columns:
/// feature_field_names={"a/b","x/y"}
/// measurement_field_names={"f1", "f2"}
class ARROW_ACERO_EXPORT PivotLongerNodeOptions : public ExecNodeOptions {
 public:
  static constexpr std::string_view kName = "pivot_longer";
  /// One or more row templates to create new output rows
  ///
  /// Normally there are at least two row templates.  The output # of rows
  /// will be the input # of rows * the number of row templates
  std::vector<PivotLongerRowTemplate> row_templates;
  /// The names of the columns which describe the new features
  std::vector<std::string> feature_field_names;
  /// The names of the columns which represent the measurements
  std::vector<std::string> measurement_field_names;
};

/// \brief a node which implements experimental node that enables multiple sink exec nodes
///
/// Note, this API is experimental and will change in the future
///
/// This node forwards each exec batch to its output and also provides number of
/// additional source nodes for additional acero pipelines.
class ARROW_ACERO_EXPORT PipeSourceNodeOptions : public ExecNodeOptions {
 public:
  PipeSourceNodeOptions(std::string pipe_name, std::shared_ptr<Schema> output_schema,
                        Ordering ordering = Ordering::Unordered())
      : pipe_name(std::move(pipe_name)),
        output_schema(std::move(output_schema)),
        ordering(std::move(ordering)) {}

  /// \brief Pipe name used to match with pipe sink
  std::string pipe_name;

  /// \brief Expected schema of data. Validated during initialization.
  std::shared_ptr<Schema> output_schema;

  /// \brief Expected ordering of data. Validated during initialization.
  Ordering ordering;
};

class ARROW_ACERO_EXPORT PipeSinkNodeOptions : public ExecNodeOptions {
 public:
  PipeSinkNodeOptions(std::string pipe_name, bool pause_on_any = true,
                      bool stop_on_any = false)
      : pipe_name(std::move(pipe_name)),
        pause_on_any(pause_on_any),
        stop_on_any(stop_on_any) {}

  /// \brief Pipe name used to match with pipe sources
  std::string pipe_name;

  /// \brief pause_on_any controls pausing strategy. If true sink input will be paused
  /// when any source is paused. If false sink input will be paused hen all sources are
  /// paused
  bool pause_on_any;

  /// \brief stop_on_any controls stopping strategy. If true sink input will be stopped
  /// when any source is stopped. If false sink input will be stopped hen all sources are
  /// stopped
  bool stop_on_any;
};

/// @}

}  // namespace acero
}  // namespace arrow
