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

#include <iostream>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/optional.h>
#include "arrow/compute/exec/asof_join.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/make_unique.h"

#include <condition_variable>
#include <mutex>
#include <thread>

namespace arrow {
namespace compute {

/**
 * Simple implementation for an unbound concurrent queue
 */
template <class T>
class ConcurrentQueue {
 public:
  T Pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [&] { return !queue_.empty(); });
    auto item = queue_.front();
    queue_.pop();
    return item;
  }

  void push(const T& item) {
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push(item);
    cond_.notify_one();
  }

  util::optional<T> try_pop() {
    // Try to pop the oldest value from the queue (or return nullopt if none)
    std::unique_lock<std::mutex> lock(mutex_);
    if (queue_.empty()) {
      return util::nullopt;
    } else {
      auto item = queue_.front();
      queue_.pop();
      return item;
    }
  }

  bool empty() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  // Un-synchronized access to front
  // For this to be "safe":
  // 1) the caller logically guarantees that queue is not empty
  // 2) pop/try_pop cannot be called concurrently with this
  const T& unsync_front() const { return queue_.front(); }

 private:
  std::queue<T> queue_;
  mutable std::mutex mutex_;
  std::condition_variable cond_;
};

struct MemoStore {
  // Stores last known values for all the keys

  struct Entry {
    // Timestamp associated with the entry
    int64_t _time;

    // Batch associated with the entry (perf is probably OK for this; batches change
    // rarely)
    std::shared_ptr<arrow::RecordBatch> _batch;

    // Row associated with the entry
    row_index_t _row;
  };

  std::unordered_map<KeyType, Entry> _entries;

  void store(const std::shared_ptr<RecordBatch>& batch, row_index_t row, int64_t time,
             KeyType key) {
    auto& e = _entries[key];
    // that we can do this assignment optionally, is why we
    // can get array with using shared_ptr above (the batch
    // shouldn't change that often)
    if (e._batch != batch) e._batch = batch;
    e._row = row;
    e._time = time;
  }

  util::optional<const Entry*> get_entry_for_key(KeyType key) const {
    auto e = _entries.find(key);
    if (_entries.end() == e) return util::nullopt;
    return util::optional<const Entry*>(&e->second);
  }

  void remove_entries_with_lesser_time(int64_t ts) {
    size_t dbg_size0 = _entries.size();
    for (auto e = _entries.begin(); e != _entries.end();)
      if (e->second._time < ts)
        e = _entries.erase(e);
      else
        ++e;
    size_t dbg_size1 = _entries.size();
    if (dbg_size1 < dbg_size0) {
      // cerr << "Removed " << dbg_size0-dbg_size1 << " memo entries.\n";
    }
  }
};

class InputState {
  // InputState correponds to an input
  // Input record batches are queued up in InputState until processed and
  // turned into output record batches.

 public:
  InputState(const std::shared_ptr<arrow::Schema>& schema,
             const std::string& time_col_name, const std::string& key_col_name,
             util::optional<KeyType> wildcard_key)
      : queue_(),
        wildcard_key_(wildcard_key),
        schema_(schema),
        time_col_index_(
            schema->GetFieldIndex(time_col_name)),  // TODO: handle missing field name
        key_col_index_(schema->GetFieldIndex(key_col_name)) {}

  col_index_t init_src_to_dst_mapping(col_index_t dst_offset,
                                      bool skip_time_and_key_fields) {
    src_to_dst_.resize(schema_->num_fields());
    for (int i = 0; i < schema_->num_fields(); ++i)
      if (!(skip_time_and_key_fields && is_time_or_key_column(i)))
        src_to_dst_[i] = dst_offset++;
    return dst_offset;
  }

  const util::optional<col_index_t>& map_src_to_dst(col_index_t src) const {
    return src_to_dst_[src];
  }

  bool is_time_or_key_column(col_index_t i) const {
    assert(i < schema_->num_fields());
    return (i == time_col_index_) || (i == key_col_index_);
  }

  // Gets the latest row index,  assuming the queue isn't empty
  row_index_t get_latest_row() const { return latest_ref_row_; }

  bool empty() const {
    if (latest_ref_row_ > 0)
      return false;  // cannot be empty if ref row is >0 -- can avoid slow queue lock
                     // below
    return queue_.empty();
  }

  int countbatches_processed_() const { return batches_processed_; }
  int count_total_batches() const { return total_batches_; }

  // Gets latest batch (precondition: must not be empty)
  const std::shared_ptr<arrow::RecordBatch>& get_latest_batch() const {
    return queue_.unsync_front();
  }
  KeyType get_latest_key() const {
    return queue_.unsync_front()
        ->column_data(key_col_index_)
        ->GetValues<KeyType>(1)[latest_ref_row_];
  }
  int64_t get_latest_time() const {
    return queue_.unsync_front()
        ->column_data(time_col_index_)
        ->GetValues<int64_t>(1)[latest_ref_row_];
  }

  bool finished() const { return batches_processed_ == total_batches_; }

  bool advance() {
    // Returns true if able to advance, false if not.

    bool have_active_batch =
        (latest_ref_row_ > 0 /*short circuit the lock on the queue*/) || !queue_.empty();
    if (have_active_batch) {
      // If we have an active batch
      if (++latest_ref_row_ >= (row_index_t)queue_.unsync_front()->num_rows()) {
        // hit the end of the batch, need to get the next batch if possible.
        ++batches_processed_;
        latest_ref_row_ = 0;
        have_active_batch &= !queue_.try_pop();
        if (have_active_batch)
          assert(queue_.unsync_front()->num_rows() > 0);  // empty batches disallowed
      }
    }
    return have_active_batch;
  }

  // Advance the data to be immediately past the specified TS, updating latest and
  // latest_ref_row to the latest data prior to that immediate just past Returns true if
  // updates were made, false if not.
  bool advance_and_memoize(int64_t ts) {
    // Advance the right side row index until we reach the latest right row (for each key)
    // for the given left timestamp.

    // Check if already updated for TS (or if there is no latest)
    if (empty()) return false;  // can't advance if empty
    auto latest_time = get_latest_time();
    if (latest_time > ts) return false;  // already advanced

    // Not updated.  Try to update and possibly advance.
    bool updated = false;
    do {
      latest_time = get_latest_time();
      // if advance() returns true, then the latest_ts must also be valid
      // Keep advancing right table until we hit the latest row that has
      // timestamp <= ts. This is because we only need the latest row for the
      // match given a left ts.
      if (latest_time <= ts) {
        memo_.store(get_latest_batch(), latest_ref_row_, latest_time, get_latest_key());
      } else {
        break;  // hit a future timestamp -- done updating for now
      }
      updated = true;
    } while (advance());
    return updated;
  }

  void push(const std::shared_ptr<arrow::RecordBatch>& rb) {
    if (rb->num_rows() > 0) {
      queue_.push(rb);
    } else {
      ++batches_processed_;  // don't enqueue empty batches, just record as processed
    }
  }

  util::optional<const MemoStore::Entry*> get_memo_entry_for_key(KeyType key) {
    auto r = memo_.get_entry_for_key(key);
    if (r.has_value()) return r;
    if (wildcard_key_.has_value()) r = memo_.get_entry_for_key(*wildcard_key_);
    return r;
  }

  util::optional<int64_t> get_memo_time_for_key(KeyType key) {
    auto r = get_memo_entry_for_key(key);
    return r.has_value() ? util::make_optional((*r)->_time) : util::nullopt;
  }

  void remove_memo_entries_with_lesser_time(int64_t ts) {
    memo_.remove_entries_with_lesser_time(ts);
  }

  const std::shared_ptr<Schema>& get_schema() const { return schema_; }

  void set_total_batches(int n) {
    assert(n >= 0);
    assert(total_batches_ == -1);  // shouldn't be set more than once
    total_batches_ = n;
  }

 private:
  // Pending record batches.  The latest is the front.  Batches cannot be empty.
  ConcurrentQueue<std::shared_ptr<RecordBatch>> queue_;

  // Wildcard key for this input, if applicable.
  util::optional<KeyType> wildcard_key_;

  // Schema associated with the input
  std::shared_ptr<Schema> schema_;

  // Total number of batches (only int because InputFinished uses int)
  int total_batches_ = -1;

  // Number of batches processed so far (only int because InputFinished uses int)
  int batches_processed_ = 0;

  // Index of the time col
  col_index_t time_col_index_;

  // Index of the key col
  col_index_t key_col_index_;

  // Index of the latest row reference within; if >0 then queue_ cannot be empty
  row_index_t latest_ref_row_ =
      0;  // must be < queue_.front()->num_rows() if queue_ is non-empty

  // Stores latest known values for the various keys
  MemoStore memo_;

  // Mapping of source columns to destination columns
  std::vector<util::optional<col_index_t>> src_to_dst_;
};

template <size_t MAX_TABLES>
struct CompositeReferenceRow {
  struct Entry {
    arrow::RecordBatch* batch;  // can be NULL if there's no value
    row_index_t row;
  };
  Entry refs[MAX_TABLES];
};

// A table of composite reference rows.  Rows maintain pointers to the
// constituent record batches, but the overall table retains shared_ptr
// references to ensure memory remains resident while the table is live.
//
// The main reason for this is that, especially for wide tables, joins
// are effectively row-oriented, rather than column-oriented.  Separating
// the join part from the columnar materialization part simplifies the
// logic around data types and increases efficiency.
//
// We don't put the shared_ptr's into the rows for efficiency reasons.
template <size_t MAX_TABLES>
class CompositeReferenceTable {
 public:
  explicit CompositeReferenceTable(size_t n_tables) : n_tables_(n_tables) {
    assert(n_tables_ >= 1);
    assert(n_tables_ <= MAX_TABLES);
  }

  size_t n_rows() const { return rows_.size(); }

  // Adds the latest row from the input state as a new composite reference row
  // - LHS must have a valid key,timestep,and latest rows
  // - RHS must have valid data memo'ed for the key
  void emplace(std::vector<std::unique_ptr<InputState>>& in, int64_t tolerance) {
    assert(in.size() == n_tables_);

    // Get the LHS key
    KeyType key = in[0]->get_latest_key();

    // Add row and setup LHS
    // (the LHS state comes just from the latest row of the LHS table)
    assert(!in[0]->empty());
    const std::shared_ptr<arrow::RecordBatch>& lhs_latest_batch =
        in[0]->get_latest_batch();
    row_index_t lhs_latest_row = in[0]->get_latest_row();
    int64_t lhs_latest_time = in[0]->get_latest_time();
    if (0 == lhs_latest_row) {
      // On the first row of the batch, we resize the destination.
      // The destination size is dictated by the size of the LHS batch.
      row_index_t new_batch_size = lhs_latest_batch->num_rows();
      row_index_t new_capacity = rows_.size() + new_batch_size;
      if (rows_.capacity() < new_capacity) rows_.reserve(new_capacity);
    }
    rows_.resize(rows_.size() + 1);
    auto& row = rows_.back();
    row.refs[0].batch = lhs_latest_batch.get();
    row.refs[0].row = lhs_latest_row;
    add_record_batch_ref(lhs_latest_batch);

    // Get the state for that key from all on the RHS -- assumes it's up to date
    // (the RHS state comes from the memoized row references)
    for (size_t i = 1; i < in.size(); ++i) {
      util::optional<const MemoStore::Entry*> opt_entry =
          in[i]->get_memo_entry_for_key(key);
      if (opt_entry.has_value()) {
        assert(*opt_entry);
        if ((*opt_entry)->_time + tolerance >= lhs_latest_time) {
          // Have a valid entry
          const MemoStore::Entry* entry = *opt_entry;
          row.refs[i].batch = entry->_batch.get();
          row.refs[i].row = entry->_row;
          add_record_batch_ref(entry->_batch);
          continue;
        }
      }
      row.refs[i].batch = NULL;
      row.refs[i].row = 0;
    }
  }

  // Materializes the current reference table into a target record batch
  Result<std::shared_ptr<RecordBatch>> materialize(
      const std::shared_ptr<arrow::Schema>& output_schema,
      const std::vector<std::unique_ptr<InputState>>& state) {
    // cerr << "materialize BEGIN\n";
    DCHECK_EQ(state.size(), n_tables_);
    DCHECK_GE(state.size(), 1);

    // Don't build empty batches
    size_t n_rows = rows_.size();
    if (!n_rows) return NULLPTR;

    // Build the arrays column-by-column from the rows
    std::vector<std::shared_ptr<arrow::Array>> arrays(output_schema->num_fields());
    for (size_t i_table = 0; i_table < n_tables_; ++i_table) {
      int n_src_cols = state.at(i_table)->get_schema()->num_fields();
      {
        for (col_index_t i_src_col = 0; i_src_col < n_src_cols; ++i_src_col) {
          util::optional<col_index_t> i_dst_col_opt =
              state[i_table]->map_src_to_dst(i_src_col);
          if (!i_dst_col_opt) continue;
          col_index_t i_dst_col = *i_dst_col_opt;
          const auto& src_field = state[i_table]->get_schema()->field(i_src_col);
          const auto& dst_field = output_schema->field(i_dst_col);
          assert(src_field->type()->Equals(dst_field->type()));
          assert(src_field->name() == dst_field->name());
          const auto& field_type = src_field->type();

          if (field_type->Equals(arrow::int32())) {
            ARROW_ASSIGN_OR_RAISE(
                arrays.at(i_dst_col),
                (materialize_primitive_column<arrow::Int32Builder, int32_t>(i_table,
                                                                            i_src_col)));
          } else if (field_type->Equals(arrow::int64())) {
            ARROW_ASSIGN_OR_RAISE(
                arrays.at(i_dst_col),
                (materialize_primitive_column<arrow::Int64Builder, int64_t>(i_table,
                                                                            i_src_col)));
          } else if (field_type->Equals(arrow::float64())) {
            ARROW_ASSIGN_OR_RAISE(
                arrays.at(i_dst_col),
                (materialize_primitive_column<arrow::DoubleBuilder, double>(i_table,
                                                                            i_src_col)));
          } else {
            ARROW_RETURN_NOT_OK(
                Status::Invalid("Unsupported data type: ", src_field->name()));
          }
        }
      }
    }

    // Build the result
    assert(sizeof(size_t) >= sizeof(int64_t));  // Make takes signed int64_t for num_rows

    // TODO: check n_rows for cast
    std::shared_ptr<arrow::RecordBatch> r =
        arrow::RecordBatch::Make(output_schema, (int64_t)n_rows, arrays);
    return r;
  }

  // Returns true if there are no rows
  bool empty() const { return rows_.empty(); }

 private:
  // Contains shared_ptr refs for all RecordBatches referred to by the contents of rows_
  std::unordered_map<uintptr_t, std::shared_ptr<RecordBatch>> _ptr2ref;

  // Row table references
  std::vector<CompositeReferenceRow<MAX_TABLES>> rows_;

  // Total number of tables in the composite table
  size_t n_tables_;

  // Adds a RecordBatch ref to the mapping, if needed
  void add_record_batch_ref(const std::shared_ptr<RecordBatch>& ref) {
    if (!_ptr2ref.count((uintptr_t)ref.get())) _ptr2ref[(uintptr_t)ref.get()] = ref;
  }

  template <class Builder, class PrimitiveType>
  Result<std::shared_ptr<Array>> materialize_primitive_column(size_t i_table,
                                                              col_index_t i_col) {
    Builder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(rows_.size()));
    for (row_index_t i_row = 0; i_row < rows_.size(); ++i_row) {
      const auto& ref = rows_[i_row].refs[i_table];
      if (ref.batch) {
        builder.UnsafeAppend(
            ref.batch->column_data(i_col)->template GetValues<PrimitiveType>(1)[ref.row]);
      } else {
        builder.UnsafeAppendNull();
      }
    }
    std::shared_ptr<Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
  }
};

class AsofJoinNode : public ExecNode {
  // Constructs labels for inputs
  static std::vector<std::string> build_input_labels(
      const std::vector<ExecNode*>& inputs) {
    std::vector<std::string> r(inputs.size());
    for (size_t i = 0; i < r.size(); ++i) r[i] = "input_" + std::to_string(i) + "_label";
    return r;
  }

  // Advances the RHS as far as possible to be up to date for the current LHS timestamp
  bool update_rhs() {
    auto& lhs = *_state.at(0);
    auto lhs_latest_time = lhs.get_latest_time();
    bool any_updated = false;
    for (size_t i = 1; i < _state.size(); ++i)
      any_updated |= _state[i]->advance_and_memoize(lhs_latest_time);
    return any_updated;
  }

  // Returns false if RHS not up to date for LHS
  bool is_up_to_date_for_lhs_row() const {
    auto& lhs = *_state[0];
    if (lhs.empty()) return false;  // can't proceed if nothing on the LHS
    int64_t lhs_ts = lhs.get_latest_time();
    for (size_t i = 1; i < _state.size(); ++i) {
      auto& rhs = *_state[i];
      if (!rhs.finished()) {
        // If RHS is finished, then we know it's up to date (but if it isn't, it might be
        // up to date)
        if (rhs.empty())
          return false;  // RHS isn't finished, but is empty --> not up to date
        if (lhs_ts >= rhs.get_latest_time())
          return false;  // TS not up to date (and not finished)
      }
    }
    return true;
  }

  Result<std::shared_ptr<RecordBatch>> process_inner() {
    assert(!_state.empty());
    auto& lhs = *_state.at(0);

    // Construct new target table if needed
    CompositeReferenceTable<MAX_JOIN_TABLES> dst(_state.size());

    // Generate rows into the dst table until we either run out of data or hit the row
    // limit, or run out of input
    for (;;) {
      // If LHS is finished or empty then there's nothing we can do here
      if (lhs.finished() || lhs.empty()) break;

      // Advance each of the RHS as far as possible to be up to date for the LHS timestamp
      bool any_advanced = update_rhs();

      // Only update if we have up-to-date information for the LHS row
      if (is_up_to_date_for_lhs_row()) {
        dst.emplace(_state, _options.tolerance);
        if (!lhs.advance()) break;  // if we can't advance LHS, we're done for this batch
      } else {
        if ((!any_advanced) && (_state.size() > 1)) break;  // need to wait for new data
      }
    }

    // Prune memo entries that have expired (to bound memory consumption)
    if (!lhs.empty()) {
      for (size_t i = 1; i < _state.size(); ++i) {
        _state[i]->remove_memo_entries_with_lesser_time(lhs.get_latest_time() -
                                                        _options.tolerance);
      }
    }

    // Emit the batch
    if (dst.empty()) {
      return NULLPTR;
    } else {
      return dst.materialize(output_schema(), _state);
    }
  }

  void process() {
    std::cerr << "process() begin\n";

    std::lock_guard<std::mutex> guard(_gate);
    if (finished_.is_finished()) {
      std::cerr << "InputReceived EARLYEND\n";
      return;
    }

    // Process batches while we have data
    for (;;) {
      Result<std::shared_ptr<RecordBatch>> result = process_inner();

      if (result.ok()) {
        auto out_rb = *result;
        if (!out_rb) break;
        ++_progress_batches_produced;
        ExecBatch out_b(*out_rb);
        outputs_[0]->InputReceived(this, std::move(out_b));
      } else {
        StopProducing();
        ErrorIfNotOk(result.status());
        return;
      }
    }

    std::cerr << "process() end\n";

    // Report to the output the total batch count, if we've already finished everything
    // (there are two places where this can happen: here and InputFinished)
    //
    // It may happen here in cases where InputFinished was called before we were finished
    // producing results (so we didn't know the output size at that time)
    if (_state.at(0)->finished()) {
      total_batches_produced_ = util::make_optional<int>(_progress_batches_produced);
      StopProducing();
      assert(total_batches_produced_.has_value());
      outputs_[0]->InputFinished(this, *total_batches_produced_);
    }
  }

  void process_thread() {
    std::cerr << "AsofJoinNode::process_thread started.\n";
    for (;;) {
      if (!_process.pop()) {
        std::cerr << "AsofJoinNode::process_thread done.\n";
        return;
      }
      process();
    }
  }

  static void process_thread_wrapper(AsofJoinNode* node) { node->process_thread(); }

 public:
  AsofJoinNode(ExecPlan* plan, NodeVector inputs, std::vector<std::string> input_labels,
               const AsofJoinNodeOptions& join_options,
               std::shared_ptr<Schema> output_schema,
               std::unique_ptr<AsofJoinSchema> schema_mgr);

  virtual ~AsofJoinNode() {
    _process.push(false);  // poison pill
    _process_thread.join();
  }

  static arrow::Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                       const ExecNodeOptions& options) {
    std::unique_ptr<AsofJoinSchema> schema_mgr =
        ::arrow::internal::make_unique<AsofJoinSchema>();

    const auto& join_options = checked_cast<const AsofJoinNodeOptions&>(options);
    std::shared_ptr<Schema> output_schema =
        schema_mgr->MakeOutputSchema(inputs, join_options);

    std::vector<std::string> input_labels(inputs.size());
    input_labels[0] = "left";
    for (size_t i = 1; i < inputs.size(); ++i) {
      input_labels[i] = "right_" + std::to_string(i);
    }

    return plan->EmplaceNode<AsofJoinNode>(plan, inputs, std::move(input_labels),
                                           join_options, std::move(output_schema),
                                           std::move(schema_mgr));
  }

  const char* kind_name() const override { return "AsofJoinNode"; }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    // Get the input
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());
    size_t k = std::find(inputs_.begin(), inputs_.end(), input) - inputs_.begin();
    std::cerr << "InputReceived BEGIN (k=" << k << ")\n";

    // Put into the queue
    auto rb = *batch.ToRecordBatch(input->output_schema());

    _state.at(k)->push(rb);
    _process.push(true);

    std::cerr << "InputReceived END\n";
  }
  void ErrorReceived(ExecNode* input, Status error) override {
    outputs_[0]->ErrorReceived(this, std::move(error));
    StopProducing();
  }
  void InputFinished(ExecNode* input, int total_batches) override {
    std::cerr << "InputFinished BEGIN\n";
    // bool is_finished=false;
    {
      std::lock_guard<std::mutex> guard(_gate);
      std::cerr << "InputFinished find\n";
      ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());
      size_t k = std::find(inputs_.begin(), inputs_.end(), input) - inputs_.begin();
      // cerr << "set_total_batches for input " << k << ": " << total_batches << "\n";
      _state.at(k)->set_total_batches(total_batches);
    }
    // Trigger a process call
    // The reason for this is that there are cases at the end of a table where we don't
    // know whether the RHS of the join is up-to-date until we know that the table is
    // finished.
    _process.push(true);

    std::cerr << "InputFinished END\n";
  }
  Status StartProducing() override {
    std::cout << "StartProducing"
              << "\n";
    finished_ = arrow::Future<>::Make();
    return Status::OK();
  }
  void PauseProducing(ExecNode* output, int32_t counter) override {
    std::cout << "PauseProducing"
              << "\n";
  }
  void ResumeProducing(ExecNode* output, int32_t counter) override {
    std::cout << "ResumeProducing"
              << "\n";
  }
  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
    std::cout << "StopProducing"
              << "\n";
  }
  void StopProducing() override {
    std::cerr << "StopProducing" << std::endl;
    // if(batch_count_.Cancel()) finished_.MarkFinished();
    finished_.MarkFinished();
    for (auto&& input : inputs_) input->StopProducing(this);
  }
  arrow::Future<> finished() override { return finished_; }

 private:
  std::unique_ptr<AsofJoinSchema> schema_mgr_;
  arrow::Future<> finished_;
  // InputStates
  // Each input state correponds to an input table
  //
  std::vector<std::unique_ptr<InputState>> _state;
  std::mutex _gate;
  AsofJoinNodeOptions _options;

  // Queue for triggering processing of a given input
  // (a false value is a poison pill)
  ConcurrentQueue<bool> _process;
  // Worker thread
  std::thread _process_thread;

  // Total batches produced, once we've finished -- only known at completion time.
  util::optional<int> total_batches_produced_;

  // In-progress batches produced
  int _progress_batches_produced = 0;
};

std::shared_ptr<Schema> AsofJoinSchema::MakeOutputSchema(
    const std::vector<ExecNode*>& inputs, const AsofJoinNodeOptions& options) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  DCHECK_GT(inputs.size(), 1);

  // Directly map LHS fields
  for (int i = 0; i < inputs[0]->output_schema()->num_fields(); ++i)
    fields.push_back(inputs[0]->output_schema()->field(i));

  // Take all non-key, non-time RHS fields
  for (size_t j = 1; j < inputs.size(); ++j) {
    const auto& input_schema = inputs[j]->output_schema();
    for (int i = 0; i < input_schema->num_fields(); ++i) {
      const auto& name = input_schema->field(i)->name();
      if ((name != *options.keys.name()) && (name != *options.time.name())) {
        fields.push_back(input_schema->field(i));
      }
    }
  }

  return std::make_shared<arrow::Schema>(fields);
}

AsofJoinNode::AsofJoinNode(ExecPlan* plan, NodeVector inputs,
                           std::vector<std::string> input_labels,
                           const AsofJoinNodeOptions& join_options,
                           std::shared_ptr<Schema> output_schema,
                           std::unique_ptr<AsofJoinSchema> schema_mgr)
    : ExecNode(plan, inputs, input_labels,
               /*output_schema=*/std::move(output_schema),
               /*num_outputs=*/1),
      _options(join_options),
      _process(),
      _process_thread(&AsofJoinNode::process_thread_wrapper, this) {
  for (size_t i = 0; i < inputs.size(); ++i)
    _state.push_back(::arrow::internal::make_unique<InputState>(
        inputs[i]->output_schema(), *_options.time.name(), *_options.keys.name(),
        util::make_optional<KeyType>(0) /*TODO: make wildcard configuirable*/));
  col_index_t dst_offset = 0;
  for (auto& state : _state)
    dst_offset = state->init_src_to_dst_mapping(dst_offset, !!dst_offset);

  finished_ = arrow::Future<>::MakeFinished();
}

namespace internal {
void RegisterAsofJoinNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("asofjoin", AsofJoinNode::Make));
}
}  // namespace internal

}  // namespace compute
}  // namespace arrow
