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

#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/optional.h"

namespace arrow {
namespace compute {

// Remove this when multiple keys and/or types is supported
typedef int32_t KeyType;

// Maximum number of tables that can be joined
#define MAX_JOIN_TABLES 64
typedef uint64_t row_index_t;
typedef int col_index_t;

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

  void Push(const T& item) {
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push(item);
    cond_.notify_one();
  }

  util::optional<T> TryPop() {
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

  bool Empty() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  // Un-synchronized access to front
  // For this to be "safe":
  // 1) the caller logically guarantees that queue is not empty
  // 2) pop/try_pop cannot be called concurrently with this
  const T& UnsyncFront() const { return queue_.front(); }

 private:
  std::queue<T> queue_;
  mutable std::mutex mutex_;
  std::condition_variable cond_;
};

struct MemoStore {
  // Stores last known values for all the keys

  struct Entry {
    // Timestamp associated with the entry
    int64_t time;

    // Batch associated with the entry (perf is probably OK for this; batches change
    // rarely)
    std::shared_ptr<arrow::RecordBatch> batch;

    // Row associated with the entry
    row_index_t row;
  };

  std::unordered_map<KeyType, Entry> entries_;

  void Store(const std::shared_ptr<RecordBatch>& batch, row_index_t row, int64_t time,
             KeyType key) {
    auto& e = entries_[key];
    // that we can do this assignment optionally, is why we
    // can get array with using shared_ptr above (the batch
    // shouldn't change that often)
    if (e.batch != batch) e.batch = batch;
    e.row = row;
    e.time = time;
  }

  util::optional<const Entry*> GetEntryForKey(KeyType key) const {
    auto e = entries_.find(key);
    if (entries_.end() == e) return util::nullopt;
    return util::optional<const Entry*>(&e->second);
  }

  void RemoveEntriesWithLesserTime(int64_t ts) {
    for (auto e = entries_.begin(); e != entries_.end();)
      if (e->second.time < ts)
        e = entries_.erase(e);
      else
        ++e;
  }
};

class InputState {
  // InputState correponds to an input
  // Input record batches are queued up in InputState until processed and
  // turned into output record batches.

 public:
  InputState(const std::shared_ptr<arrow::Schema>& schema,
             const std::string& time_col_name, const std::string& key_col_name)
      : queue_(),
        schema_(schema),
        time_col_index_(schema->GetFieldIndex(time_col_name)),
        key_col_index_(schema->GetFieldIndex(key_col_name)) {}

  col_index_t InitSrcToDstMapping(col_index_t dst_offset, bool skip_time_and_key_fields) {
    src_to_dst_.resize(schema_->num_fields());
    for (int i = 0; i < schema_->num_fields(); ++i)
      if (!(skip_time_and_key_fields && IsTimeOrKeyColumn(i)))
        src_to_dst_[i] = dst_offset++;
    return dst_offset;
  }

  const util::optional<col_index_t>& MapSrcToDst(col_index_t src) const {
    return src_to_dst_[src];
  }

  bool IsTimeOrKeyColumn(col_index_t i) const {
    DCHECK_LT(i, schema_->num_fields());
    return (i == time_col_index_) || (i == key_col_index_);
  }

  // Gets the latest row index,  assuming the queue isn't empty
  row_index_t GetLatestRow() const { return latest_ref_row_; }

  bool Empty() const {
    // cannot be empty if ref row is >0 -- can avoid slow queue lock
    // below
    if (latest_ref_row_ > 0) return false;
    return queue_.Empty();
  }

  int total_batches() const { return total_batches_; }

  // Gets latest batch (precondition: must not be empty)
  const std::shared_ptr<arrow::RecordBatch>& GetLatestBatch() const {
    return queue_.UnsyncFront();
  }

  KeyType GetLatestKey() const {
    return queue_.UnsyncFront()
        ->column_data(key_col_index_)
        ->GetValues<KeyType>(1)[latest_ref_row_];
  }

  int64_t GetLatestTime() const {
    return queue_.UnsyncFront()
        ->column_data(time_col_index_)
        ->GetValues<int64_t>(1)[latest_ref_row_];
  }

  bool Finished() const { return batches_processed_ == total_batches_; }

  bool Advance() {
    // Try advancing to the next row and update latest_ref_row_
    // Returns true if able to advance, false if not.
    bool have_active_batch =
        (latest_ref_row_ > 0 /*short circuit the lock on the queue*/) || !queue_.Empty();

    if (have_active_batch) {
      // If we have an active batch
      if (++latest_ref_row_ >= (row_index_t)queue_.UnsyncFront()->num_rows()) {
        // hit the end of the batch, need to get the next batch if possible.
        ++batches_processed_;
        latest_ref_row_ = 0;
        have_active_batch &= !queue_.TryPop();
        if (have_active_batch)
          DCHECK_GT(queue_.UnsyncFront()->num_rows(), 0);  // empty batches disallowed
      }
    }
    return have_active_batch;
  }

  // Advance the data to be immediately past the specified timestamp, update
  // latest_time and latest_ref_row to the value that immediately pass the
  // specified timestamp.
  // Returns true if updates were made, false if not.
  bool AdvanceAndMemoize(int64_t ts) {
    // Advance the right side row index until we reach the latest right row (for each key)
    // for the given left timestamp.

    // Check if already updated for TS (or if there is no latest)
    if (Empty()) return false;  // can't advance if empty
    auto latest_time = GetLatestTime();
    if (latest_time > ts) return false;  // already advanced

    // Not updated.  Try to update and possibly advance.
    bool updated = false;
    do {
      latest_time = GetLatestTime();
      // if Advance() returns true, then the latest_ts must also be valid
      // Keep advancing right table until we hit the latest row that has
      // timestamp <= ts. This is because we only need the latest row for the
      // match given a left ts.
      if (latest_time <= ts) {
        memo_.Store(GetLatestBatch(), latest_ref_row_, latest_time, GetLatestKey());
      } else {
        break;  // hit a future timestamp -- done updating for now
      }
      updated = true;
    } while (Advance());
    return updated;
  }

  void Push(const std::shared_ptr<arrow::RecordBatch>& rb) {
    if (rb->num_rows() > 0) {
      queue_.Push(rb);
    } else {
      ++batches_processed_;  // don't enqueue empty batches, just record as processed
    }
  }

  util::optional<const MemoStore::Entry*> GetMemoEntryForKey(KeyType key) {
    return memo_.GetEntryForKey(key);
  }

  util::optional<int64_t> GetMemoTimeForKey(KeyType key) {
    auto r = GetMemoEntryForKey(key);
    if (r.has_value()) {
      return (*r)->time;
    } else {
      return util::nullopt;
    }
  }

  void RemoveMemoEntriesWithLesserTime(int64_t ts) {
    memo_.RemoveEntriesWithLesserTime(ts);
  }

  const std::shared_ptr<Schema>& get_schema() const { return schema_; }

  void set_total_batches(int n) {
    DCHECK_GE(n, 0);
    DCHECK_EQ(total_batches_, -1) << "Set total batch more than once";
    total_batches_ = n;
  }

 private:
  // Pending record batches. The latest is the front. Batches cannot be empty.
  ConcurrentQueue<std::shared_ptr<RecordBatch>> queue_;
  // Schema associated with the input
  std::shared_ptr<Schema> schema_;
  // Total number of batches (only int because InputFinished uses int)
  std::atomic<int> total_batches_{-1};
  // Number of batches processed so far (only int because InputFinished uses int)
  std::atomic<int> batches_processed_{0};
  // Index of the time col
  col_index_t time_col_index_;
  // Index of the key col
  col_index_t key_col_index_;
  // Index of the latest row reference within; if >0 then queue_ cannot be empty
  // Must be < queue_.front()->num_rows() if queue_ is non-empty
  row_index_t latest_ref_row_ = 0;
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
    DCHECK_GE(n_tables_, 1);
    DCHECK_LE(n_tables_, MAX_TABLES);
  }

  size_t n_rows() const { return rows_.size(); }

  // Adds the latest row from the input state as a new composite reference row
  // - LHS must have a valid key,timestep,and latest rows
  // - RHS must have valid data memo'ed for the key
  void Emplace(std::vector<std::unique_ptr<InputState>>& in, int64_t tolerance) {
    DCHECK_EQ(in.size(), n_tables_);

    // Get the LHS key
    KeyType key = in[0]->GetLatestKey();

    // Add row and setup LHS
    // (the LHS state comes just from the latest row of the LHS table)
    DCHECK(!in[0]->Empty());
    const std::shared_ptr<arrow::RecordBatch>& lhs_latest_batch = in[0]->GetLatestBatch();
    row_index_t lhs_latest_row = in[0]->GetLatestRow();
    int64_t lhs_latest_time = in[0]->GetLatestTime();
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
    AddRecordBatchRef(lhs_latest_batch);

    // Get the state for that key from all on the RHS -- assumes it's up to date
    // (the RHS state comes from the memoized row references)
    for (size_t i = 1; i < in.size(); ++i) {
      util::optional<const MemoStore::Entry*> opt_entry = in[i]->GetMemoEntryForKey(key);
      if (opt_entry.has_value()) {
        DCHECK(*opt_entry);
        if ((*opt_entry)->time + tolerance >= lhs_latest_time) {
          // Have a valid entry
          const MemoStore::Entry* entry = *opt_entry;
          row.refs[i].batch = entry->batch.get();
          row.refs[i].row = entry->row;
          AddRecordBatchRef(entry->batch);
          continue;
        }
      }
      row.refs[i].batch = NULL;
      row.refs[i].row = 0;
    }
  }

  // Materializes the current reference table into a target record batch
  Result<std::shared_ptr<RecordBatch>> Materialize(
      MemoryPool* memory_pool, const std::shared_ptr<arrow::Schema>& output_schema,
      const std::vector<std::unique_ptr<InputState>>& state) {
    DCHECK_EQ(state.size(), n_tables_);

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
              state[i_table]->MapSrcToDst(i_src_col);
          if (!i_dst_col_opt) continue;
          col_index_t i_dst_col = *i_dst_col_opt;
          const auto& src_field = state[i_table]->get_schema()->field(i_src_col);
          const auto& dst_field = output_schema->field(i_dst_col);
          DCHECK(src_field->type()->Equals(dst_field->type()));
          DCHECK_EQ(src_field->name(), dst_field->name());
          const auto& field_type = src_field->type();

          if (field_type->Equals(arrow::int32())) {
            ARROW_ASSIGN_OR_RAISE(
                arrays.at(i_dst_col),
                (MaterializePrimitiveColumn<arrow::Int32Builder, int32_t>(
                    memory_pool, i_table, i_src_col)));
          } else if (field_type->Equals(arrow::int64())) {
            ARROW_ASSIGN_OR_RAISE(
                arrays.at(i_dst_col),
                (MaterializePrimitiveColumn<arrow::Int64Builder, int64_t>(
                    memory_pool, i_table, i_src_col)));
          } else if (field_type->Equals(arrow::float32())) {
            ARROW_ASSIGN_OR_RAISE(arrays.at(i_dst_col),
                                  (MaterializePrimitiveColumn<arrow::FloatBuilder, float>(
                                      memory_pool, i_table, i_src_col)));
          } else if (field_type->Equals(arrow::float64())) {
            ARROW_ASSIGN_OR_RAISE(
                arrays.at(i_dst_col),
                (MaterializePrimitiveColumn<arrow::DoubleBuilder, double>(
                    memory_pool, i_table, i_src_col)));
          } else {
            ARROW_RETURN_NOT_OK(
                Status::Invalid("Unsupported data type: ", src_field->name()));
          }
        }
      }
    }

    // Build the result
    DCHECK_LE(n_rows, (uint64_t)std::numeric_limits<int64_t>::max());
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
  void AddRecordBatchRef(const std::shared_ptr<RecordBatch>& ref) {
    if (!_ptr2ref.count((uintptr_t)ref.get())) _ptr2ref[(uintptr_t)ref.get()] = ref;
  }

  template <class Builder, class PrimitiveType>
  Result<std::shared_ptr<Array>> MaterializePrimitiveColumn(MemoryPool* memory_pool,
                                                            size_t i_table,
                                                            col_index_t i_col) {
    Builder builder(memory_pool);
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
  // Advances the RHS as far as possible to be up to date for the current LHS timestamp
  bool UpdateRhs() {
    auto& lhs = *state_.at(0);
    auto lhs_latest_time = lhs.GetLatestTime();
    bool any_updated = false;
    for (size_t i = 1; i < state_.size(); ++i)
      any_updated |= state_[i]->AdvanceAndMemoize(lhs_latest_time);
    return any_updated;
  }

  // Returns false if RHS not up to date for LHS
  bool IsUpToDateWithLhsRow() const {
    auto& lhs = *state_[0];
    if (lhs.Empty()) return false;  // can't proceed if nothing on the LHS
    int64_t lhs_ts = lhs.GetLatestTime();
    for (size_t i = 1; i < state_.size(); ++i) {
      auto& rhs = *state_[i];
      if (!rhs.Finished()) {
        // If RHS is finished, then we know it's up to date
        if (rhs.Empty())
          return false;  // RHS isn't finished, but is empty --> not up to date
        if (lhs_ts >= rhs.GetLatestTime())
          return false;  // RHS isn't up to date (and not finished)
      }
    }
    return true;
  }

  Result<std::shared_ptr<RecordBatch>> ProcessInner() {
    DCHECK(!state_.empty());
    auto& lhs = *state_.at(0);

    // Construct new target table if needed
    CompositeReferenceTable<MAX_JOIN_TABLES> dst(state_.size());

    // Generate rows into the dst table until we either run out of data or hit the row
    // limit, or run out of input
    for (;;) {
      // If LHS is finished or empty then there's nothing we can do here
      if (lhs.Finished() || lhs.Empty()) break;

      // Advance each of the RHS as far as possible to be up to date for the LHS timestamp
      bool any_rhs_advanced = UpdateRhs();

      // If we have received enough inputs to produce the next output batch
      // (decided by IsUpToDateWithLhsRow), we will perform the join and
      // materialize the output batch. The join is done by advancing through
      // the LHS and adding joined row to rows_ (done by Emplace). Finally,
      // input batches that are no longer needed are removed to free up memory.
      if (IsUpToDateWithLhsRow()) {
        dst.Emplace(state_, options_.tolerance);
        if (!lhs.Advance()) break;  // if we can't advance LHS, we're done for this batch
      } else {
        if (!any_rhs_advanced) break;  // need to wait for new data
      }
    }

    // Prune memo entries that have expired (to bound memory consumption)
    if (!lhs.Empty()) {
      for (size_t i = 1; i < state_.size(); ++i) {
        state_[i]->RemoveMemoEntriesWithLesserTime(lhs.GetLatestTime() -
                                                   options_.tolerance);
      }
    }

    // Emit the batch
    if (dst.empty()) {
      return NULLPTR;
    } else {
      return dst.Materialize(plan()->exec_context()->memory_pool(), output_schema(),
                             state_);
    }
  }

  void Process() {
    std::lock_guard<std::mutex> guard(gate_);
    if (finished_.is_finished()) {
      return;
    }

    // Process batches while we have data
    for (;;) {
      Result<std::shared_ptr<RecordBatch>> result = ProcessInner();

      if (result.ok()) {
        auto out_rb = *result;
        if (!out_rb) break;
        ++batches_produced_;
        ExecBatch out_b(*out_rb);
        outputs_[0]->InputReceived(this, std::move(out_b));
      } else {
        StopProducing();
        ErrorIfNotOk(result.status());
        return;
      }
    }

    // Report to the output the total batch count, if we've already finished everything
    // (there are two places where this can happen: here and InputFinished)
    //
    // It may happen here in cases where InputFinished was called before we were finished
    // producing results (so we didn't know the output size at that time)
    if (state_.at(0)->Finished()) {
      StopProducing();
      outputs_[0]->InputFinished(this, batches_produced_);
    }
  }

  void ProcessThread() {
    for (;;) {
      if (!process_.Pop()) {
        return;
      }
      Process();
    }
  }

  static void ProcessThreadWrapper(AsofJoinNode* node) { node->ProcessThread(); }

 public:
  AsofJoinNode(ExecPlan* plan, NodeVector inputs, std::vector<std::string> input_labels,
               const AsofJoinNodeOptions& join_options,
               std::shared_ptr<Schema> output_schema);

  virtual ~AsofJoinNode() {
    process_.Push(false);  // poison pill
    process_thread_.join();
  }

  static arrow::Result<std::shared_ptr<Schema>> MakeOutputSchema(
      const std::vector<ExecNode*>& inputs, const AsofJoinNodeOptions& options) {
    std::vector<std::shared_ptr<arrow::Field>> fields;

    const auto& on_field_name = *options.on_key.name();
    const auto& by_field_name = *options.by_key.name();

    // Take all non-key, non-time RHS fields
    for (size_t j = 0; j < inputs.size(); ++j) {
      const auto& input_schema = inputs[j]->output_schema();
      const auto& on_field_ix = input_schema->GetFieldIndex(on_field_name);
      const auto& by_field_ix = input_schema->GetFieldIndex(by_field_name);

      if ((on_field_ix == -1) | (by_field_ix == -1)) {
        return Status::Invalid("Missing join key on table ", j);
      }

      for (int i = 0; i < input_schema->num_fields(); ++i) {
        const auto field = input_schema->field(i);
        if (field->name() == on_field_name) {
          if (kSupportedOnTypes_.find(field->type()) == kSupportedOnTypes_.end()) {
            return Status::Invalid("Unsupported type for on key: ", field->name());
          }
          // Only add on field from the left table
          if (j == 0) {
            fields.push_back(field);
          }
        } else if (field->name() == by_field_name) {
          if (kSupportedByTypes_.find(field->type()) == kSupportedByTypes_.end()) {
            return Status::Invalid("Unsupported type for by key: ", field->name());
          }
          // Only add by field from the left table
          if (j == 0) {
            fields.push_back(field);
          }
        } else {
          if (kSupportedDataTypes_.find(field->type()) == kSupportedDataTypes_.end()) {
            return Status::Invalid("Unsupported data type: ", field->name());
          }

          fields.push_back(field);
        }
      }
    }
    return std::make_shared<arrow::Schema>(fields);
  }

  static arrow::Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                       const ExecNodeOptions& options) {
    DCHECK_GE(inputs.size(), 2) << "Must have at least two inputs";

    const auto& join_options = checked_cast<const AsofJoinNodeOptions&>(options);
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Schema> output_schema,
                          MakeOutputSchema(inputs, join_options));

    std::vector<std::string> input_labels(inputs.size());
    input_labels[0] = "left";
    for (size_t i = 1; i < inputs.size(); ++i) {
      input_labels[i] = "right_" + std::to_string(i);
    }

    return plan->EmplaceNode<AsofJoinNode>(plan, inputs, std::move(input_labels),
                                           join_options, std::move(output_schema));
  }

  const char* kind_name() const override { return "AsofJoinNode"; }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    // Get the input
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());
    size_t k = std::find(inputs_.begin(), inputs_.end(), input) - inputs_.begin();

    // Put into the queue
    auto rb = *batch.ToRecordBatch(input->output_schema());
    state_.at(k)->Push(rb);
    process_.Push(true);
  }
  void ErrorReceived(ExecNode* input, Status error) override {
    outputs_[0]->ErrorReceived(this, std::move(error));
    StopProducing();
  }
  void InputFinished(ExecNode* input, int total_batches) override {
    {
      std::lock_guard<std::mutex> guard(gate_);
      ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());
      size_t k = std::find(inputs_.begin(), inputs_.end(), input) - inputs_.begin();
      state_.at(k)->set_total_batches(total_batches);
    }
    // Trigger a process call
    // The reason for this is that there are cases at the end of a table where we don't
    // know whether the RHS of the join is up-to-date until we know that the table is
    // finished.
    process_.Push(true);
  }
  Status StartProducing() override {
    finished_ = arrow::Future<>::Make();
    return Status::OK();
  }
  void PauseProducing(ExecNode* output, int32_t counter) override {}
  void ResumeProducing(ExecNode* output, int32_t counter) override {}
  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }
  void StopProducing() override { finished_.MarkFinished(); }
  arrow::Future<> finished() override { return finished_; }

 private:
  static const std::set<std::shared_ptr<DataType>> kSupportedOnTypes_;
  static const std::set<std::shared_ptr<DataType>> kSupportedByTypes_;
  static const std::set<std::shared_ptr<DataType>> kSupportedDataTypes_;

  arrow::Future<> finished_;
  // InputStates
  // Each input state correponds to an input table
  std::vector<std::unique_ptr<InputState>> state_;
  std::mutex gate_;
  AsofJoinNodeOptions options_;

  // Queue for triggering processing of a given input
  // (a false value is a poison pill)
  ConcurrentQueue<bool> process_;
  // Worker thread
  std::thread process_thread_;

  // In-progress batches produced
  int batches_produced_ = 0;
};

AsofJoinNode::AsofJoinNode(ExecPlan* plan, NodeVector inputs,
                           std::vector<std::string> input_labels,
                           const AsofJoinNodeOptions& join_options,
                           std::shared_ptr<Schema> output_schema)
    : ExecNode(plan, inputs, input_labels,
               /*output_schema=*/std::move(output_schema),
               /*num_outputs=*/1),
      options_(join_options),
      process_(),
      process_thread_(&AsofJoinNode::ProcessThreadWrapper, this) {
  for (size_t i = 0; i < inputs.size(); ++i)
    state_.push_back(::arrow::internal::make_unique<InputState>(
        inputs[i]->output_schema(), *options_.on_key.name(), *options_.by_key.name()));
  col_index_t dst_offset = 0;
  for (auto& state : state_)
    dst_offset = state->InitSrcToDstMapping(dst_offset, !!dst_offset);

  finished_ = arrow::Future<>::MakeFinished();
}

// Currently supported types
const std::set<std::shared_ptr<DataType>> AsofJoinNode::kSupportedOnTypes_ = {int64()};
const std::set<std::shared_ptr<DataType>> AsofJoinNode::kSupportedByTypes_ = {int32()};
const std::set<std::shared_ptr<DataType>> AsofJoinNode::kSupportedDataTypes_ = {
    int32(), int64(), float32(), float64()};

namespace internal {
void RegisterAsofJoinNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("asofjoin", AsofJoinNode::Make));
}
}  // namespace internal

}  // namespace compute
}  // namespace arrow
