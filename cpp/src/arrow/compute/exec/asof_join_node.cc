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
#include <unordered_set>

#include "arrow/compute/exec/asof_join.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/thread_pool.h"

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
//#include <arrow/io/util_internal.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/compute/exec/options.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/counting_semaphore.h>  // so we don't need to require C++20
#include <arrow/util/optional.h>
#include <arrow/util/thread_pool.h>
#include <algorithm>
#include <atomic>
#include <future>
#include <mutex>
#include <optional>
#include <thread>

#include <omp.h>

#include "concurrent_bounded_queue.h"

namespace arrow {
namespace compute {

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
      : _queue(QUEUE_CAPACITY),
        _wildcard_key(wildcard_key),
        _schema(schema),
        _time_col_index(
            schema->GetFieldIndex(time_col_name)),  // TODO: handle missing field name
        _key_col_index(
            schema->GetFieldIndex(key_col_name))  // TODO: handle missing field name
  {                                               /*nothing else*/
  }

  size_t init_src_to_dst_mapping(size_t dst_offset, bool skip_time_and_key_fields) {
    src_to_dst.resize(_schema->num_fields());
    for (int i = 0; i < _schema->num_fields(); ++i)
      if (!(skip_time_and_key_fields && is_time_or_key_column(i)))
        src_to_dst[i] = dst_offset++;
    return dst_offset;
  }

  const util::optional<col_index_t>& map_src_to_dst(col_index_t src) const {
    return src_to_dst[src];
  }

  bool is_time_or_key_column(col_index_t i) const {
    assert(i < _schema->num_fields());
    return (i == _time_col_index) || (i == _key_col_index);
  }

  // Gets the latest row index,  assuming the queue isn't empty
  row_index_t get_latest_row() const { return _latest_ref_row; }

  bool empty() const {
    if (_latest_ref_row > 0)
      return false;  // cannot be empty if ref row is >0 -- can avoid slow queue lock
                     // below
    return _queue.empty();
  }

  int count_batches_processed() const { return _batches_processed; }
  int count_total_batches() const { return _total_batches; }

  // Gets latest batch (precondition: must not be empty)
  const std::shared_ptr<arrow::RecordBatch>& get_latest_batch() const {
    return _queue.unsync_front();
  }
  KeyType get_latest_key() const {
    return _queue.unsync_front()
        ->column_data(_key_col_index)
        ->GetValues<KeyType>(1)[_latest_ref_row];
  }
  int64_t get_latest_time() const {
    return _queue.unsync_front()
        ->column_data(_time_col_index)
        ->GetValues<int64_t>(1)[_latest_ref_row];
  }

  bool finished() const { return _batches_processed == _total_batches; }

  bool advance() {
    // Returns true if able to advance, false if not.
    
    bool have_active_batch =
        (_latest_ref_row > 0 /*short circuit the lock on the queue*/) || !_queue.empty();
    if (have_active_batch) {
      // If we have an active batch
      if (++_latest_ref_row >= _queue.unsync_front()->num_rows()) {
        // hit the end of the batch, need to get the next batch if possible.
        ++_batches_processed;
        _latest_ref_row = 0;
        have_active_batch &= !_queue.try_pop();
        if (have_active_batch)
          assert(_queue.unsync_front()->num_rows() > 0);  // empty batches disallowed
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
      if (latest_time <=
          ts)  // if advance() returns true, then the latest_ts must also be valid
	// Keep advancing right table until we hit the latest row that has
	// timestamp <= ts. This is because we only need the latest row for the
	// match given a left ts.  However, for a futre      
        _memo.store(get_latest_batch(), _latest_ref_row, latest_time, get_latest_key());
      else
        break;  // hit a future timestamp -- done updating for now
      updated = true;
    } while (advance());
    return updated;
  }

  void push(const std::shared_ptr<arrow::RecordBatch>& rb) {
    if (rb->num_rows() > 0) {
      _queue.push(rb);
    } else {
      ++_batches_processed;  // don't enqueue empty batches, just record as processed
    }
  }

  util::optional<const MemoStore::Entry*> get_memo_entry_for_key(KeyType key) {
    auto r = _memo.get_entry_for_key(key);
    if (r.has_value()) return r;
    if (_wildcard_key.has_value()) r = _memo.get_entry_for_key(*_wildcard_key);
    return r;
  }

  util::optional<int64_t> get_memo_time_for_key(KeyType key) {
    auto r = get_memo_entry_for_key(key);
    return r.has_value() ? util::make_optional((*r)->_time) : util::nullopt;
  }

  void remove_memo_entries_with_lesser_time(int64_t ts) {
    _memo.remove_entries_with_lesser_time(ts);
  }

  const std::shared_ptr<Schema>& get_schema() const { return _schema; }

  void set_total_batches(int n) {
    assert(n >=
           0);  // not sure why arrow uses a signed int for this, but it should be >=0
    assert(_total_batches == -1);  // shouldn't be set more than once
    _total_batches = n;
  }

 private:
  // Pending record batches.  The latest is the front.  Batches cannot be empty.
  concurrent_bounded_queue<std::shared_ptr<RecordBatch>> _queue;

  // Wildcard key for this input, if applicable.
  util::optional<KeyType> _wildcard_key;
  
  // Schema associated with the input
  std::shared_ptr<Schema> _schema;
  
  // Total number of batches (only int because InputFinished uses int)
  int _total_batches = -1;
  
  // Number of batches processed so far (only int because InputFinished uses int)
  int _batches_processed = 0;
  
  // Index of the time col
  col_index_t _time_col_index;
  
  // Index of the key col
  col_index_t _key_col_index;

  // Index of the latest row reference within; if >0 then _queue cannot be empty
  row_index_t _latest_ref_row =
    0;  // must be < _queue.front()->num_rows() if _queue is non-empty
  
  // Stores latest known values for the various keys
  MemoStore _memo;
  
  // Mapping of source columns to destination columns
  std::vector<util::optional<col_index_t>> src_to_dst;
};

template <size_t MAX_TABLES>
struct CompositeReferenceRow {
  struct Entry {
    arrow::RecordBatch* _batch;  // can be NULL if there's no value
    row_index_t _row;
  };
  Entry _refs[MAX_TABLES];
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
  // Contains shared_ptr refs for all RecordBatches referred to by the contents of _rows
  std::unordered_map<uintptr_t, std::shared_ptr<RecordBatch>> _ptr2ref;

  // Row table references
  std::vector<CompositeReferenceRow<MAX_TABLES>> _rows;

  // Total number of tables in the composite table
  size_t _n_tables;

  // Adds a RecordBatch ref to the mapping, if needed
  void add_record_batch_ref(const std::shared_ptr<RecordBatch>& ref) {
    if (!_ptr2ref.count((uintptr_t)ref.get())) _ptr2ref[(uintptr_t)ref.get()] = ref;
  }

 public:
  CompositeReferenceTable(size_t n_tables) : _n_tables(n_tables) {
    assert(_n_tables >= 1);
    assert(_n_tables <= MAX_TABLES);
  }

  size_t n_rows() const { return _rows.size(); }

  // Adds the latest row from the input state as a new composite reference row
  // - LHS must have a valid key,timestep,and latest rows
  // - RHS must have valid data memo'ed for the key
  void emplace(std::vector<std::unique_ptr<InputState>>& in, int64_t tolerance) {
    assert(in.size() == _n_tables);

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
      assert(lhs_latest_batch->num_rows() <=
             MAX_ROWS_PER_BATCH);  // TODO: better error handling
      row_index_t new_batch_size = lhs_latest_batch->num_rows();
      row_index_t new_capacity = _rows.size() + new_batch_size;
      if (_rows.capacity() < new_capacity) _rows.reserve(new_capacity);
      // cerr << "new_batch_size=" << new_batch_size << " old_size=" << _rows.size() << "
      // new_capacity=" << _rows.capacity() << endl;
    }
    _rows.resize(_rows.size() + 1);
    auto& row = _rows.back();
    row._refs[0]._batch = lhs_latest_batch.get();
    row._refs[0]._row = lhs_latest_row;
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
          row._refs[i]._batch = entry->_batch.get();
          row._refs[i]._row = entry->_row;
          add_record_batch_ref(entry->_batch);
          continue;
        }
      }
      row._refs[i]._batch = NULL;
      row._refs[i]._row = 0;
    }
  }

 private:
  template <class Builder, class PrimitiveType>
  std::shared_ptr<Array> materialize_primitive_column(size_t i_table, size_t i_col) {
    Builder builder;
    // builder.Resize(_rows.size()); // <-- can't just do this -- need to set the bitmask
    builder.AppendEmptyValues(_rows.size());
    for (row_index_t i_row = 0; i_row < _rows.size(); ++i_row) {
      const auto& ref = _rows[i_row]._refs[i_table];
      if (ref._batch)
        builder[i_row] =
            ref._batch->column_data(i_col)->template GetValues<PrimitiveType>(
                1)[ref._row];
      // TODO: set null value if ref._batch is null -- currently we don't due to API
      // limitations of the builders.
    }
    std::shared_ptr<Array> result;
    if (!builder.Finish(&result).ok()) {
      std::cerr << "Error when creating Arrow array from builder\n";
      exit(-1);  // TODO: better error handling
    }
    return result;
  }

 public:
  // Materializes the current reference table into a target record batch
  std::shared_ptr<RecordBatch> materialize(
      const std::shared_ptr<arrow::Schema>& output_schema,
      const std::vector<std::unique_ptr<InputState>>& state) {
    // cerr << "materialize BEGIN\n";
    assert(state.size() == _n_tables);
    assert(state.size() >= 1);

    // Don't build empty batches
    size_t n_rows = _rows.size();
    if (!n_rows) return nullptr;

    // Count output columns (dbg sanitycheck)
    {
      int n_out_cols = 0;
      for (const auto& s : state) n_out_cols += s->get_schema()->num_fields();
      n_out_cols -=
          (state.size() - 1) * 2;  // remove column indices for key and time cols on RHS
      assert(n_out_cols == output_schema->num_fields());
    }

    // Instance the types we support
    std::shared_ptr<arrow::DataType> i32_type = arrow::int32();
    std::shared_ptr<arrow::DataType> i64_type = arrow::int64();
    std::shared_ptr<arrow::DataType> f64_type = arrow::float64();

    // Build the arrays column-by-column from our rows
    std::vector<std::shared_ptr<arrow::Array>> arrays(output_schema->num_fields());
    for (size_t i_table = 0; i_table < _n_tables; ++i_table) {
      int n_src_cols = state.at(i_table)->get_schema()->num_fields();
      {
        for (col_index_t i_src_col = 0; i_src_col < n_src_cols; ++i_src_col) {
          util::optional<size_t> i_dst_col_opt =
              state[i_table]->map_src_to_dst(i_src_col);
          if (!i_dst_col_opt) continue;
          col_index_t i_dst_col = *i_dst_col_opt;
          const auto& src_field = state[i_table]->get_schema()->field(i_src_col);
          const auto& dst_field = output_schema->field(i_dst_col);
          assert(src_field->type()->Equals(dst_field->type()));
          assert(src_field->name() == dst_field->name());
          const auto& field_type = src_field->type();
          if (field_type->Equals(i32_type)) {
            arrays.at(i_dst_col) =
                materialize_primitive_column<arrow::Int32Builder, int32_t>(i_table,
                                                                           i_src_col);
          } else if (field_type->Equals(i64_type)) {
            arrays.at(i_dst_col) =
                materialize_primitive_column<arrow::Int64Builder, int64_t>(i_table,
                                                                           i_src_col);
          } else if (field_type->Equals(f64_type)) {
            arrays.at(i_dst_col) =
                materialize_primitive_column<arrow::DoubleBuilder, double>(i_table,
                                                                           i_src_col);
          } else {
            std::cerr << "Unsupported data type: " << field_type->name() << "\n";
            exit(-1);  // TODO: validate elsewhere for better error handling
          }
        }
      }
    }

    // Build the result
    assert(sizeof(size_t) >= sizeof(int64_t));  // Make takes signed int64_t for num_rows
    // TODO: check n_rows for cast
    std::shared_ptr<arrow::RecordBatch> r =
        arrow::RecordBatch::Make(output_schema, (int64_t)n_rows, arrays);
    // cerr << "materialize END (ndstrows="<< (r?r->num_rows():-1) <<")\n";
    return r;
  }

  // Returns true if there are no rows
  bool empty() const { return _rows.empty(); }
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

  std::shared_ptr<RecordBatch> process_inner() {
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
        dst.emplace(_state, _options._tolerance);
        if (!lhs.advance()) break;  // if we can't advance LHS, we're done for this batch
      } else {
        if ((!any_advanced) && (_state.size() > 1)) break;  // need to wait for new data
      }
    }

    // Prune memo entries that have expired (to bound memory consumption)
    if (!lhs.empty())
      for (size_t i = 1; i < _state.size(); ++i)
        _state[i]->remove_memo_entries_with_lesser_time(lhs.get_latest_time() -
                                                        _options._tolerance);

    // Emit the batch
    std::shared_ptr<RecordBatch> r =
        dst.empty() ? nullptr : dst.materialize(output_schema(), _state);
    return r;
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
      std::shared_ptr<RecordBatch> out_rb = process_inner();
      if (!out_rb) break;
      ++_progress_batches_produced;
      ExecBatch out_b(*out_rb);
      outputs_[0]->InputReceived(this, std::move(out_b));
    }

    std::cerr << "process() end\n";

    // Report to the output the total batch count, if we've already finished everything
    // (there are two places where this can happen: here and InputFinished)
    //
    // It may happen here in cases where InputFinished was called before we were finished
    // producing results (so we didn't know the output size at that time)
    if (_state.at(0)->finished()) {
      // cerr << "LHS is finished\n";
      _total_batches_produced = util::make_optional<int>(_progress_batches_produced);
      std::cerr << "process() finished " << *_total_batches_produced << "\n";
      StopProducing();
      assert(_total_batches_produced.has_value());
      outputs_[0]->InputFinished(this, *_total_batches_produced);
    }
  }

  // Status process_thread(size_t /*thread_index*/, int64_t /*task_id*/) {
  //   std::cerr << "AsOfJoinNode::process_thread started.\n";
  //   auto result = _process.try_pop();

  //   if (result == util::nullopt) {
  //     std::cerr << "AsOfJoinNode::process_thread no inputs.\n";
  //     return Status::OK();
  //   } else {
  //     if (result.value()) {
  //       std::cerr << "AsOfJoinNode::process_thread process.\n";
  //       process();
  //     } else {
  //       std::cerr << "AsOfJoinNode::process_thread done.\n";
  //       return Status::OK();
  //     }
  //   }

  //   return Status::OK();
  // }

  // Status process_finished(size_t /*thread_index*/) {
  //   std::cerr << "AsOfJoinNode::process_finished started.\n";
  //   return Status::OK();
  // }

  void process_thread() {
    std::cerr << "AsOfMergeNode::process_thread started.\n";
    for (;;) {
      if (!_process.pop()) {
        std::cerr << "AsOfMergeNode::process_thread done.\n";
        return;
      }
      process();
    }
  }

  static void process_thread_wrapper(AsofJoinNode* node) { node->process_thread(); }

 public:
  AsofJoinNode(ExecPlan* plan, NodeVector inputs, const AsofJoinNodeOptions& join_options,
               std::shared_ptr<Schema> output_schema,
               std::unique_ptr<AsofJoinSchema> schema_mgr,
               std::unique_ptr<AsofJoinImpl> impl);

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
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<AsofJoinImpl> impl, AsofJoinImpl::MakeBasic());

    return plan->EmplaceNode<AsofJoinNode>(plan, inputs, join_options,
                                           std::move(output_schema),
                                           std::move(schema_mgr), std::move(impl));
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
      // is_finished=_state.at(k)->finished();
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
    // bool use_sync_execution = !(plan_->exec_context()->executor());
    // std::cerr << "StartScheduling\n";
    // std::cerr << "use_sync_execution: " << use_sync_execution << std::endl;
    // RETURN_NOT_OK(
    //               scheduler_->StartScheduling(0 /*thread index*/,
    //                                           std::move([this](std::function<Status(size_t)>
    //                                           func) -> Status {
    //                                                       return
    //                                                       this->ScheduleTaskCallback(std::move(func));
    //                                                     }),
    //                                           1,
    //                                           use_sync_execution
    //                                           )
    //               );
    // RETURN_NOT_OK(
    //               scheduler_->StartTaskGroup(0, task_group_process_, 1)
    //               );
    // std::cerr << "StartScheduling done\n";
    return Status::OK();
  }
  void PauseProducing(ExecNode* output) override {
    std::cout << "PauseProducing"
              << "\n";
  }
  void ResumeProducing(ExecNode* output) override {
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
  Future<> finished() override { return finished_; }

  // Status ScheduleTaskCallback(std::function<Status(size_t)> func) {
  //   auto executor = plan_->exec_context()->executor();
  //   if (executor) {
  //     RETURN_NOT_OK(executor->Spawn([this, func] {
  //       size_t thread_index = thread_indexer_();
  //       Status status = func(thread_index);
  //       if (!status.ok()) {
  //         StopProducing();
  //         ErrorIfNotOk(status);
  //         return;
  //       }
  //     }));
  //   } else {
  //     // We should not get here in serial execution mode
  //     ARROW_DCHECK(false);
  //   }
  //   return Status::OK();
  // }

 private:
  std::unique_ptr<AsofJoinSchema> schema_mgr_;
  std::unique_ptr<AsofJoinImpl> impl_;
  Future<> finished_;
  // InputStates
  // Each input state correponds to an input table
  // 
  std::vector<std::unique_ptr<InputState>> _state;
  std::mutex _gate;
  AsofJoinNodeOptions _options;

  // ThreadIndexer thread_indexer_;
  // std::unique_ptr<TaskScheduler> scheduler_;
  // int task_group_process_;

  // Queue for triggering processing of a given input
  // (a false value is a poison pill)
  concurrent_bounded_queue<bool> _process;
  // Worker thread
  std::thread _process_thread;

  // Total batches produced, once we've finished -- only known at completion time.
  util::optional<int> _total_batches_produced;

  // In-progress batches produced
  int _progress_batches_produced = 0;
};

std::shared_ptr<Schema> AsofJoinSchema::MakeOutputSchema(
    const std::vector<ExecNode*>& inputs, const AsofJoinNodeOptions& options) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  assert(inputs.size() > 1);

  // TODO: Deal with multi keys
  // std::vector<std::string> keys;
  // for (auto f: options.keys) {
  //   keys.emplace_back(*f.name());
  // }

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

  // Combine into a schema
  return std::make_shared<arrow::Schema>(fields);
}

AsofJoinNode::AsofJoinNode(ExecPlan* plan, NodeVector inputs,
                           const AsofJoinNodeOptions& join_options,
                           std::shared_ptr<Schema> output_schema,
                           std::unique_ptr<AsofJoinSchema> schema_mgr,
                           std::unique_ptr<AsofJoinImpl> impl)
    : ExecNode(plan, inputs, {"left", "right"},
               /*output_schema=*/std::move(output_schema),
               /*num_outputs=*/1),
      impl_(std::move(impl)),
      _options(join_options),
      _process(1),
      _process_thread(&AsofJoinNode::process_thread_wrapper, this) {
  std::cout << "AsofJoinNode created"
            << "\n";

  for (size_t i = 0; i < inputs.size(); ++i)
    _state.push_back(::arrow::internal::make_unique<InputState>(
        inputs[i]->output_schema(), *_options.time.name(), *_options.keys.name(),
        util::make_optional<KeyType>(0) /*TODO: make wildcard configuirable*/));
  size_t dst_offset = 0;
  for (auto& state : _state)
    dst_offset = state->init_src_to_dst_mapping(dst_offset, !!dst_offset);

  finished_ = Future<>::MakeFinished();

  // scheduler_ = TaskScheduler::Make();
  // task_group_process_ = scheduler_->RegisterTaskGroup(
  //                                                    [this](size_t thread_index,
  //                                                    int64_t task_id) -> Status {
  //                                                      return
  //                                                      process_thread(thread_index,
  //                                                      task_id);
  //                                                    },
  //                                                    [this](size_t thread_index) ->
  //                                                    Status {
  //                                                      return
  //                                                      process_finished(thread_index);
  //                                                    }
  //                                                    );
  // scheduler_->RegisterEnd();
}

namespace internal {
void RegisterAsofJoinNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("asofjoin", AsofJoinNode::Make));
}
}  // namespace internal

}  // namespace compute
}  // namespace arrow
