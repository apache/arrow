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
#include <string>
#include <vector>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/result.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/optional.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

using AsyncExecBatchGenerator = AsyncGenerator<util::optional<ExecBatch>>;

/// \addtogroup execnode-options
/// @{
class ARROW_EXPORT ExecNodeOptions {
 public:
  virtual ~ExecNodeOptions() = default;
};

/// \brief Adapt an AsyncGenerator<ExecBatch> as a source node
///
/// plan->exec_context()->executor() will be used to parallelize pushing to
/// outputs, if provided.
class ARROW_EXPORT SourceNodeOptions : public ExecNodeOptions {
 public:
  SourceNodeOptions(std::shared_ptr<Schema> output_schema,
                    std::function<Future<util::optional<ExecBatch>>()> generator)
      : output_schema(std::move(output_schema)), generator(std::move(generator)) {}

  static Result<std::shared_ptr<SourceNodeOptions>> FromTable(const Table& table,
                                                              arrow::internal::Executor*);

  std::shared_ptr<Schema> output_schema;
  std::function<Future<util::optional<ExecBatch>>()> generator;
};

/// \brief An extended Source node which accepts a table
class ARROW_EXPORT TableSourceNodeOptions : public ExecNodeOptions {
 public:
  TableSourceNodeOptions(std::shared_ptr<Table> table, int64_t max_batch_size)
      : table(table), max_batch_size(max_batch_size) {}

  // arrow table which acts as the data source
  std::shared_ptr<Table> table;
  // Size of batches to emit from this node
  // If the table is larger the node will emit multiple batches from the
  // the table to be processed in parallel.
  int64_t max_batch_size;
};

/// \brief Make a node which excludes some rows from batches passed through it
///
/// filter_expression will be evaluated against each batch which is pushed to
/// this node. Any rows for which filter_expression does not evaluate to `true` will be
/// excluded in the batch emitted by this node.
class ARROW_EXPORT FilterNodeOptions : public ExecNodeOptions {
 public:
  explicit FilterNodeOptions(Expression filter_expression, bool async_mode = true)
      : filter_expression(std::move(filter_expression)), async_mode(async_mode) {}

  Expression filter_expression;
  bool async_mode;
};

/// \brief Make a node which executes expressions on input batches, producing new batches.
///
/// Each expression will be evaluated against each batch which is pushed to
/// this node to produce a corresponding output column.
///
/// If names are not provided, the string representations of exprs will be used.
class ARROW_EXPORT ProjectNodeOptions : public ExecNodeOptions {
 public:
  explicit ProjectNodeOptions(std::vector<Expression> expressions,
                              std::vector<std::string> names = {}, bool async_mode = true)
      : expressions(std::move(expressions)),
        names(std::move(names)),
        async_mode(async_mode) {}

  std::vector<Expression> expressions;
  std::vector<std::string> names;
  bool async_mode;
};

/// \brief Make a node which aggregates input batches, optionally grouped by keys.
class ARROW_EXPORT AggregateNodeOptions : public ExecNodeOptions {
 public:
  AggregateNodeOptions(std::vector<internal::Aggregate> aggregates,
                       std::vector<FieldRef> targets, std::vector<std::string> names,
                       std::vector<FieldRef> keys = {})
      : aggregates(std::move(aggregates)),
        targets(std::move(targets)),
        names(std::move(names)),
        keys(std::move(keys)) {}

  // aggregations which will be applied to the targetted fields
  std::vector<internal::Aggregate> aggregates;
  // fields to which aggregations will be applied
  std::vector<FieldRef> targets;
  // output field names for aggregations
  std::vector<std::string> names;
  // keys by which aggregations will be grouped
  std::vector<FieldRef> keys;
};

constexpr int32_t kDefaultBackpressureHighBytes = 1 << 30;  // 1GiB
constexpr int32_t kDefaultBackpressureLowBytes = 1 << 28;   // 256MiB

class ARROW_EXPORT BackpressureMonitor {
 public:
  virtual ~BackpressureMonitor() = default;
  virtual uint64_t bytes_in_use() const = 0;
  virtual bool is_paused() const = 0;
};

/// \brief Options to control backpressure behavior
struct ARROW_EXPORT BackpressureOptions {
  /// \brief Create default options that perform no backpressure
  BackpressureOptions() : resume_if_below(0), pause_if_above(0) {}
  /// \brief Create options that will perform backpressure
  ///
  /// \param resume_if_below The producer should resume producing if the backpressure
  ///                        queue has fewer than resume_if_below items.
  /// \param pause_if_above The producer should pause producing if the backpressure
  ///                       queue has more than pause_if_above items
  BackpressureOptions(uint32_t resume_if_below, uint32_t pause_if_above)
      : resume_if_below(resume_if_below), pause_if_above(pause_if_above) {}

  static BackpressureOptions DefaultBackpressure() {
    return BackpressureOptions(kDefaultBackpressureLowBytes,
                               kDefaultBackpressureHighBytes);
  }

  bool should_apply_backpressure() const { return pause_if_above > 0; }

  uint64_t resume_if_below;
  uint64_t pause_if_above;
};

/// \brief Add a sink node which forwards to an AsyncGenerator<ExecBatch>
///
/// Emitted batches will not be ordered.
class ARROW_EXPORT SinkNodeOptions : public ExecNodeOptions {
 public:
  explicit SinkNodeOptions(std::function<Future<util::optional<ExecBatch>>()>* generator,
                           BackpressureOptions backpressure = {},
                           BackpressureMonitor** backpressure_monitor = NULLPTR)
      : generator(generator),
        backpressure(std::move(backpressure)),
        backpressure_monitor(backpressure_monitor) {}

  /// \brief A pointer to a generator of batches.
  ///
  /// This will be set when the node is added to the plan and should be used to consume
  /// data from the plan.  If this function is not called frequently enough then the sink
  /// node will start to accumulate data and may apply backpressure.
  std::function<Future<util::optional<ExecBatch>>()>* generator;
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
};

/// \brief Control used by a SinkNodeConsumer to pause & resume
///
/// Callers should ensure that they do not call Pause and Resume simultaneously and they
/// should sequence things so that a call to Pause() is always followed by an eventual
/// call to Resume()
class ARROW_EXPORT BackpressureControl {
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

class ARROW_EXPORT SinkNodeConsumer {
 public:
  virtual ~SinkNodeConsumer() = default;
  /// \brief Prepare any consumer state
  ///
  /// This will be run once the schema is finalized as the plan is starting and
  /// before any calls to Consume.  A common use is to save off the schema so that
  /// batches can be interpreted.
  virtual Status Init(const std::shared_ptr<Schema>& schema,
                      BackpressureControl* backpressure_control) = 0;
  /// \brief Consume a batch of data
  virtual Status Consume(ExecBatch batch) = 0;
  /// \brief Signal to the consumer that the last batch has been delivered
  ///
  /// The returned future should only finish when all outstanding tasks have completed
  virtual Future<> Finish() = 0;
};

/// \brief Add a sink node which consumes data within the exec plan run
class ARROW_EXPORT ConsumingSinkNodeOptions : public ExecNodeOptions {
 public:
  explicit ConsumingSinkNodeOptions(std::shared_ptr<SinkNodeConsumer> consumer)
      : consumer(std::move(consumer)) {}

  std::shared_ptr<SinkNodeConsumer> consumer;
};

/// \brief Make a node which sorts rows passed through it
///
/// All batches pushed to this node will be accumulated, then sorted, by the given
/// fields. Then sorted batches will be forwarded to the generator in sorted order.
class ARROW_EXPORT OrderBySinkNodeOptions : public SinkNodeOptions {
 public:
  explicit OrderBySinkNodeOptions(
      SortOptions sort_options,
      std::function<Future<util::optional<ExecBatch>>()>* generator)
      : SinkNodeOptions(generator), sort_options(std::move(sort_options)) {}

  SortOptions sort_options;
};

/// @}

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

/// \addtogroup execnode-options
/// @{

/// \brief Make a node which implements join operation using hash join strategy.
class ARROW_EXPORT HashJoinNodeOptions : public ExecNodeOptions {
 public:
  static constexpr const char* default_output_suffix_for_left = "";
  static constexpr const char* default_output_suffix_for_right = "";
  HashJoinNodeOptions(
      JoinType in_join_type, std::vector<FieldRef> in_left_keys,
      std::vector<FieldRef> in_right_keys, Expression filter = literal(true),
      std::string output_suffix_for_left = default_output_suffix_for_left,
      std::string output_suffix_for_right = default_output_suffix_for_right)
      : join_type(in_join_type),
        left_keys(std::move(in_left_keys)),
        right_keys(std::move(in_right_keys)),
        output_all(true),
        output_suffix_for_left(std::move(output_suffix_for_left)),
        output_suffix_for_right(std::move(output_suffix_for_right)),
        filter(std::move(filter)) {
    this->key_cmp.resize(this->left_keys.size());
    for (size_t i = 0; i < this->left_keys.size(); ++i) {
      this->key_cmp[i] = JoinKeyCmp::EQ;
    }
  }
  HashJoinNodeOptions(
      JoinType join_type, std::vector<FieldRef> left_keys,
      std::vector<FieldRef> right_keys, std::vector<FieldRef> left_output,
      std::vector<FieldRef> right_output, Expression filter = literal(true),
      std::string output_suffix_for_left = default_output_suffix_for_left,
      std::string output_suffix_for_right = default_output_suffix_for_right)
      : join_type(join_type),
        left_keys(std::move(left_keys)),
        right_keys(std::move(right_keys)),
        output_all(false),
        left_output(std::move(left_output)),
        right_output(std::move(right_output)),
        output_suffix_for_left(std::move(output_suffix_for_left)),
        output_suffix_for_right(std::move(output_suffix_for_right)),
        filter(std::move(filter)) {
    this->key_cmp.resize(this->left_keys.size());
    for (size_t i = 0; i < this->left_keys.size(); ++i) {
      this->key_cmp[i] = JoinKeyCmp::EQ;
    }
  }
  HashJoinNodeOptions(
      JoinType join_type, std::vector<FieldRef> left_keys,
      std::vector<FieldRef> right_keys, std::vector<FieldRef> left_output,
      std::vector<FieldRef> right_output, std::vector<JoinKeyCmp> key_cmp,
      Expression filter = literal(true),
      std::string output_suffix_for_left = default_output_suffix_for_left,
      std::string output_suffix_for_right = default_output_suffix_for_right)
      : join_type(join_type),
        left_keys(std::move(left_keys)),
        right_keys(std::move(right_keys)),
        output_all(false),
        left_output(std::move(left_output)),
        right_output(std::move(right_output)),
        key_cmp(std::move(key_cmp)),
        output_suffix_for_left(std::move(output_suffix_for_left)),
        output_suffix_for_right(std::move(output_suffix_for_right)),
        filter(std::move(filter)) {}

  // type of join (inner, left, semi...)
  JoinType join_type;
  // key fields from left input
  std::vector<FieldRef> left_keys;
  // key fields from right input
  std::vector<FieldRef> right_keys;
  // if set all valid fields from both left and right input will be output
  // (and field ref vectors for output fields will be ignored)
  bool output_all;
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
  Expression filter;
};

/// \brief Make a node which select top_k/bottom_k rows passed through it
///
/// All batches pushed to this node will be accumulated, then selected, by the given
/// fields. Then sorted batches will be forwarded to the generator in sorted order.
class ARROW_EXPORT SelectKSinkNodeOptions : public SinkNodeOptions {
 public:
  explicit SelectKSinkNodeOptions(
      SelectKOptions select_k_options,
      std::function<Future<util::optional<ExecBatch>>()>* generator)
      : SinkNodeOptions(generator), select_k_options(std::move(select_k_options)) {}

  /// SelectK options
  SelectKOptions select_k_options;
};
/// @}

/// \brief Adapt a Table as a sink node
///
/// obtains the output of an execution plan to
/// a table pointer.
class ARROW_EXPORT TableSinkNodeOptions : public ExecNodeOptions {
 public:
  explicit TableSinkNodeOptions(std::shared_ptr<Table>* output_table)
      : output_table(output_table) {}

  std::shared_ptr<Table>* output_table;
};

}  // namespace compute
}  // namespace arrow
