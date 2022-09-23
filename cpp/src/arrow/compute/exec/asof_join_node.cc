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
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/key_hash.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/light_array.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"

namespace arrow {
namespace compute {

template <typename T, typename V = typename T::value_type>
inline typename T::const_iterator std_find(const T& container, const V& val) {
  return std::find(container.begin(), container.end(), val);
}

template <typename T, typename V = typename T::value_type>
inline bool std_has(const T& container, const V& val) {
  return container.end() != std_find(container, val);
}

typedef uint64_t ByType;
typedef uint64_t OnType;
typedef uint64_t HashType;

// Maximum number of tables that can be joined
#define MAX_JOIN_TABLES 64
typedef uint64_t row_index_t;
typedef int col_index_t;

// normalize the value to 64-bits while preserving ordering of values
template <typename T, enable_if_t<std::is_integral<T>::value, bool> = true>
static inline uint64_t time_value(T t) {
  uint64_t bias = std::is_signed<T>::value ? (uint64_t)1 << (8 * sizeof(T) - 1) : 0;
  return t < 0 ? static_cast<uint64_t>(t + bias) : static_cast<uint64_t>(t);
}

// indicates normalization of a key value
template <typename T, enable_if_t<std::is_integral<T>::value, bool> = true>
static inline uint64_t key_value(T t) {
  return static_cast<uint64_t>(t);
}

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

  void Clear() {
    std::unique_lock<std::mutex> lock(mutex_);
    queue_ = std::queue<T>();
  }

  std::optional<T> TryPop() {
    // Try to pop the oldest value from the queue (or return nullopt if none)
    std::unique_lock<std::mutex> lock(mutex_);
    if (queue_.empty()) {
      return std::nullopt;
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
    OnType time;

    // Batch associated with the entry (perf is probably OK for this; batches change
    // rarely)
    std::shared_ptr<arrow::RecordBatch> batch;

    // Row associated with the entry
    row_index_t row;
  };

  std::unordered_map<ByType, Entry> entries_;

  void Store(const std::shared_ptr<RecordBatch>& batch, row_index_t row, OnType time,
             ByType key) {
    auto& e = entries_[key];
    // that we can do this assignment optionally, is why we
    // can get array with using shared_ptr above (the batch
    // shouldn't change that often)
    if (e.batch != batch) e.batch = batch;
    e.row = row;
    e.time = time;
  }

  std::optional<const Entry*> GetEntryForKey(ByType key) const {
    auto e = entries_.find(key);
    if (entries_.end() == e) return std::nullopt;
    return std::optional<const Entry*>(&e->second);
  }

  void RemoveEntriesWithLesserTime(OnType ts) {
    for (auto e = entries_.begin(); e != entries_.end();)
      if (e->second.time < ts)
        e = entries_.erase(e);
      else
        ++e;
  }
};

// a specialized higher-performance variation of Hashing64 logic from hash_join_node
// the code here avoids recreating objects that are independent of each batch processed
class KeyHasher {
  static constexpr int kMiniBatchLength = util::MiniBatch::kMiniBatchLength;

 public:
  explicit KeyHasher(const std::vector<col_index_t>& indices)
      : indices_(indices),
        metadata_(indices.size()),
        batch_(NULLPTR),
        hashes_(),
        ctx_(),
        column_arrays_(),
        stack_() {
    ctx_.stack = &stack_;
    column_arrays_.resize(indices.size());
  }

  Status Init(ExecContext* exec_context, const std::shared_ptr<arrow::Schema>& schema) {
    ctx_.hardware_flags = exec_context->cpu_info()->hardware_flags();
    const auto& fields = schema->fields();
    for (size_t k = 0; k < metadata_.size(); k++) {
      ARROW_ASSIGN_OR_RAISE(metadata_[k],
                            ColumnMetadataFromDataType(fields[indices_[k]]->type()));
    }
    return stack_.Init(exec_context->memory_pool(),
                       4 * kMiniBatchLength * sizeof(uint32_t));
  }

  const std::vector<HashType>& HashesFor(const RecordBatch* batch) {
    if (batch_ == batch) {
      return hashes_;
    }
    batch_ = NULLPTR;  // invalidate cached hashes for batch
    size_t batch_length = batch->num_rows();
    hashes_.resize(batch_length);
    for (int64_t i = 0; i < static_cast<int64_t>(batch_length); i += kMiniBatchLength) {
      int64_t length = std::min(static_cast<int64_t>(batch_length - i),
                                static_cast<int64_t>(kMiniBatchLength));
      for (size_t k = 0; k < indices_.size(); k++) {
        auto array_data = batch->column_data(indices_[k]);
        column_arrays_[k] =
            ColumnArrayFromArrayDataAndMetadata(array_data, metadata_[k], i, length);
      }
      Hashing64::HashMultiColumn(column_arrays_, &ctx_, hashes_.data() + i);
    }
    batch_ = batch;
    return hashes_;
  }

 private:
  std::vector<col_index_t> indices_;
  std::vector<KeyColumnMetadata> metadata_;
  const RecordBatch* batch_;
  std::vector<HashType> hashes_;
  LightContext ctx_;
  std::vector<KeyColumnArray> column_arrays_;
  util::TempVectorStack stack_;
};

class InputState {
  // InputState correponds to an input
  // Input record batches are queued up in InputState until processed and
  // turned into output record batches.

 public:
  InputState(bool must_hash, bool may_rehash, KeyHasher* key_hasher,
             const std::shared_ptr<arrow::Schema>& schema,
             const col_index_t time_col_index,
             const std::vector<col_index_t>& key_col_index)
      : queue_(),
        schema_(schema),
        time_col_index_(time_col_index),
        key_col_index_(key_col_index),
        time_type_id_(schema_->fields()[time_col_index_]->type()->id()),
        key_type_id_(key_col_index.size()),
        key_hasher_(key_hasher),
        must_hash_(must_hash),
        may_rehash_(may_rehash) {
    for (size_t k = 0; k < key_col_index_.size(); k++) {
      key_type_id_[k] = schema_->fields()[key_col_index_[k]]->type()->id();
    }
  }

  col_index_t InitSrcToDstMapping(col_index_t dst_offset, bool skip_time_and_key_fields) {
    src_to_dst_.resize(schema_->num_fields());
    for (int i = 0; i < schema_->num_fields(); ++i)
      if (!(skip_time_and_key_fields && IsTimeOrKeyColumn(i)))
        src_to_dst_[i] = dst_offset++;
    return dst_offset;
  }

  const std::optional<col_index_t>& MapSrcToDst(col_index_t src) const {
    return src_to_dst_[src];
  }

  bool IsTimeOrKeyColumn(col_index_t i) const {
    DCHECK_LT(i, schema_->num_fields());
    return (i == time_col_index_) || std_has(key_col_index_, i);
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

#define LATEST_VAL_CASE(id, val)                     \
  case Type::id: {                                   \
    using T = typename TypeIdTraits<Type::id>::Type; \
    using CType = typename TypeTraits<T>::CType;     \
    return val(data->GetValues<CType>(1)[row]);      \
  }

  inline ByType GetLatestKey() const {
    return GetLatestKey(queue_.UnsyncFront().get(), latest_ref_row_);
  }

  inline ByType GetLatestKey(const RecordBatch* batch, row_index_t row) const {
    if (must_hash_) {
      return key_hasher_->HashesFor(batch)[row];
    }
    if (key_col_index_.size() == 0) {
      return 0;
    }
    auto data = batch->column_data(key_col_index_[0]);
    switch (key_type_id_[0]) {
      LATEST_VAL_CASE(INT8, key_value)
      LATEST_VAL_CASE(INT16, key_value)
      LATEST_VAL_CASE(INT32, key_value)
      LATEST_VAL_CASE(INT64, key_value)
      LATEST_VAL_CASE(UINT8, key_value)
      LATEST_VAL_CASE(UINT16, key_value)
      LATEST_VAL_CASE(UINT32, key_value)
      LATEST_VAL_CASE(UINT64, key_value)
      LATEST_VAL_CASE(DATE32, key_value)
      LATEST_VAL_CASE(DATE64, key_value)
      LATEST_VAL_CASE(TIME32, key_value)
      LATEST_VAL_CASE(TIME64, key_value)
      LATEST_VAL_CASE(TIMESTAMP, key_value)
      default:
        DCHECK(false);
        return 0;  // cannot happen
    }
  }

  inline OnType GetLatestTime() const {
    return GetLatestTime(queue_.UnsyncFront().get(), latest_ref_row_);
  }

  inline ByType GetLatestTime(const RecordBatch* batch, row_index_t row) const {
    auto data = batch->column_data(time_col_index_);
    switch (time_type_id_) {
      LATEST_VAL_CASE(INT8, time_value)
      LATEST_VAL_CASE(INT16, time_value)
      LATEST_VAL_CASE(INT32, time_value)
      LATEST_VAL_CASE(INT64, time_value)
      LATEST_VAL_CASE(UINT8, time_value)
      LATEST_VAL_CASE(UINT16, time_value)
      LATEST_VAL_CASE(UINT32, time_value)
      LATEST_VAL_CASE(UINT64, time_value)
      LATEST_VAL_CASE(DATE32, time_value)
      LATEST_VAL_CASE(DATE64, time_value)
      LATEST_VAL_CASE(TIME32, time_value)
      LATEST_VAL_CASE(TIME64, time_value)
      LATEST_VAL_CASE(TIMESTAMP, time_value)
      default:
        DCHECK(false);
        return 0;  // cannot happen
    }
  }

#undef LATEST_VAL_CASE

  bool Finished() const { return batches_processed_ == total_batches_; }

  Result<bool> Advance() {
    // Try advancing to the next row and update latest_ref_row_
    // Returns true if able to advance, false if not.
    bool have_active_batch =
        (latest_ref_row_ > 0 /*short circuit the lock on the queue*/) || !queue_.Empty();

    if (have_active_batch) {
      OnType next_time = GetLatestTime();
      if (latest_time_ > next_time) {
        return Status::Invalid("AsofJoin does not allow out-of-order on-key values");
      }
      latest_time_ = next_time;
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
  Result<bool> AdvanceAndMemoize(OnType ts) {
    // Advance the right side row index until we reach the latest right row (for each key)
    // for the given left timestamp.

    // Check if already updated for TS (or if there is no latest)
    if (Empty()) return false;  // can't advance if empty

    // Not updated.  Try to update and possibly advance.
    bool advanced, updated = false;
    do {
      auto latest_time = GetLatestTime();
      // if Advance() returns true, then the latest_ts must also be valid
      // Keep advancing right table until we hit the latest row that has
      // timestamp <= ts. This is because we only need the latest row for the
      // match given a left ts.
      if (latest_time > ts) {
        break;  // hit a future timestamp -- done updating for now
      }
      auto rb = GetLatestBatch();
      if (may_rehash_ && rb->column_data(key_col_index_[0])->GetNullCount() > 0) {
        must_hash_ = true;
        may_rehash_ = false;
        Rehash();
      }
      memo_.Store(rb, latest_ref_row_, latest_time, GetLatestKey());
      updated = true;
      ARROW_ASSIGN_OR_RAISE(advanced, Advance());
    } while (advanced);
    return updated;
  }

  void Rehash() {
    MemoStore new_memo;
    for (const auto& entry : memo_.entries_) {
      const auto& e = entry.second;
      new_memo.Store(e.batch, e.row, e.time, GetLatestKey(e.batch.get(), e.row));
    }
    memo_ = new_memo;
  }

  Status Push(const std::shared_ptr<arrow::RecordBatch>& rb) {
    if (rb->num_rows() > 0) {
      queue_.Push(rb);
    } else {
      ++batches_processed_;  // don't enqueue empty batches, just record as processed
    }
    return Status::OK();
  }

  std::optional<const MemoStore::Entry*> GetMemoEntryForKey(ByType key) {
    return memo_.GetEntryForKey(key);
  }

  std::optional<OnType> GetMemoTimeForKey(ByType key) {
    auto r = GetMemoEntryForKey(key);
    if (r.has_value()) {
      return (*r)->time;
    } else {
      return std::nullopt;
    }
  }

  void RemoveMemoEntriesWithLesserTime(OnType ts) {
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
  std::vector<col_index_t> key_col_index_;
  // Type id of the time column
  Type::type time_type_id_;
  // Type id of the key column
  std::vector<Type::type> key_type_id_;
  // Hasher for key elements
  mutable KeyHasher* key_hasher_;
  // True if hashing is mandatory
  bool must_hash_;
  // True if by-key values may be rehashed
  bool may_rehash_;
  // Index of the latest row reference within; if >0 then queue_ cannot be empty
  // Must be < queue_.front()->num_rows() if queue_ is non-empty
  row_index_t latest_ref_row_ = 0;
  // Time of latest row
  OnType latest_time_ = std::numeric_limits<OnType>::lowest();
  // Stores latest known values for the various keys
  MemoStore memo_;
  // Mapping of source columns to destination columns
  std::vector<std::optional<col_index_t>> src_to_dst_;
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
  void Emplace(std::vector<std::unique_ptr<InputState>>& in, OnType tolerance) {
    DCHECK_EQ(in.size(), n_tables_);

    // Get the LHS key
    ByType key = in[0]->GetLatestKey();

    // Add row and setup LHS
    // (the LHS state comes just from the latest row of the LHS table)
    DCHECK(!in[0]->Empty());
    const std::shared_ptr<arrow::RecordBatch>& lhs_latest_batch = in[0]->GetLatestBatch();
    row_index_t lhs_latest_row = in[0]->GetLatestRow();
    OnType lhs_latest_time = in[0]->GetLatestTime();
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
      std::optional<const MemoStore::Entry*> opt_entry = in[i]->GetMemoEntryForKey(key);
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
          std::optional<col_index_t> i_dst_col_opt =
              state[i_table]->MapSrcToDst(i_src_col);
          if (!i_dst_col_opt) continue;
          col_index_t i_dst_col = *i_dst_col_opt;
          const auto& src_field = state[i_table]->get_schema()->field(i_src_col);
          const auto& dst_field = output_schema->field(i_dst_col);
          DCHECK(src_field->type()->Equals(dst_field->type()));
          DCHECK_EQ(src_field->name(), dst_field->name());
          const auto& field_type = src_field->type();

#define ASOFJOIN_MATERIALIZE_CASE(id)                                       \
  case Type::id: {                                                          \
    using T = typename TypeIdTraits<Type::id>::Type;                        \
    ARROW_ASSIGN_OR_RAISE(                                                  \
        arrays.at(i_dst_col),                                               \
        MaterializeColumn<T>(memory_pool, field_type, i_table, i_src_col)); \
    break;                                                                  \
  }

          switch (field_type->id()) {
            ASOFJOIN_MATERIALIZE_CASE(INT8)
            ASOFJOIN_MATERIALIZE_CASE(INT16)
            ASOFJOIN_MATERIALIZE_CASE(INT32)
            ASOFJOIN_MATERIALIZE_CASE(INT64)
            ASOFJOIN_MATERIALIZE_CASE(UINT8)
            ASOFJOIN_MATERIALIZE_CASE(UINT16)
            ASOFJOIN_MATERIALIZE_CASE(UINT32)
            ASOFJOIN_MATERIALIZE_CASE(UINT64)
            ASOFJOIN_MATERIALIZE_CASE(FLOAT)
            ASOFJOIN_MATERIALIZE_CASE(DOUBLE)
            ASOFJOIN_MATERIALIZE_CASE(DATE32)
            ASOFJOIN_MATERIALIZE_CASE(DATE64)
            ASOFJOIN_MATERIALIZE_CASE(TIME32)
            ASOFJOIN_MATERIALIZE_CASE(TIME64)
            ASOFJOIN_MATERIALIZE_CASE(TIMESTAMP)
            ASOFJOIN_MATERIALIZE_CASE(STRING)
            ASOFJOIN_MATERIALIZE_CASE(LARGE_STRING)
            ASOFJOIN_MATERIALIZE_CASE(BINARY)
            ASOFJOIN_MATERIALIZE_CASE(LARGE_BINARY)
            default:
              return Status::Invalid("Unsupported data type ",
                                     src_field->type()->ToString(), " for field ",
                                     src_field->name());
          }

#undef ASOFJOIN_MATERIALIZE_CASE
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

  template <class Type, class Builder = typename TypeTraits<Type>::BuilderType>
  enable_if_fixed_width_type<Type, Status> static BuilderAppend(
      Builder& builder, const std::shared_ptr<ArrayData>& source, row_index_t row) {
    if (source->IsNull(row)) {
      builder.UnsafeAppendNull();
      return Status::OK();
    }
    using CType = typename TypeTraits<Type>::CType;
    builder.UnsafeAppend(source->template GetValues<CType>(1)[row]);
    return Status::OK();
  }

  template <class Type, class Builder = typename TypeTraits<Type>::BuilderType>
  enable_if_base_binary<Type, Status> static BuilderAppend(
      Builder& builder, const std::shared_ptr<ArrayData>& source, row_index_t row) {
    if (source->IsNull(row)) {
      return builder.AppendNull();
    }
    using offset_type = typename Type::offset_type;
    const uint8_t* data = source->buffers[2]->data();
    const offset_type* offsets = source->GetValues<offset_type>(1);
    const offset_type offset0 = offsets[row];
    const offset_type offset1 = offsets[row + 1];
    return builder.Append(data + offset0, offset1 - offset0);
  }

  template <class Type, class Builder = typename TypeTraits<Type>::BuilderType>
  Result<std::shared_ptr<Array>> MaterializeColumn(MemoryPool* memory_pool,
                                                   const std::shared_ptr<DataType>& type,
                                                   size_t i_table, col_index_t i_col) {
    ARROW_ASSIGN_OR_RAISE(auto a_builder, MakeBuilder(type, memory_pool));
    Builder& builder = *checked_cast<Builder*>(a_builder.get());
    ARROW_RETURN_NOT_OK(builder.Reserve(rows_.size()));
    for (row_index_t i_row = 0; i_row < rows_.size(); ++i_row) {
      const auto& ref = rows_[i_row].refs[i_table];
      if (ref.batch) {
        Status st =
            BuilderAppend<Type, Builder>(builder, ref.batch->column_data(i_col), ref.row);
        ARROW_RETURN_NOT_OK(st);
      } else {
        builder.UnsafeAppendNull();
      }
    }
    std::shared_ptr<Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
  }
};

// TODO: Currently, AsofJoinNode uses 64-bit hashing which leads to a non-negligible
// probability of collision, which can cause incorrect results when many different by-key
// values are processed. Thus, AsofJoinNode is currently limited to about 100k by-keys for
// guaranteeing this probability is below 1 in a billion. The fix is 128-bit hashing.
// See ARROW-17653
class AsofJoinNode : public ExecNode {
  // Advances the RHS as far as possible to be up to date for the current LHS timestamp
  Result<bool> UpdateRhs() {
    auto& lhs = *state_.at(0);
    auto lhs_latest_time = lhs.GetLatestTime();
    bool any_updated = false;
    for (size_t i = 1; i < state_.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(bool advanced, state_[i]->AdvanceAndMemoize(lhs_latest_time));
      any_updated |= advanced;
    }
    return any_updated;
  }

  // Returns false if RHS not up to date for LHS
  bool IsUpToDateWithLhsRow() const {
    auto& lhs = *state_[0];
    if (lhs.Empty()) return false;  // can't proceed if nothing on the LHS
    OnType lhs_ts = lhs.GetLatestTime();
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
      ARROW_ASSIGN_OR_RAISE(bool any_rhs_advanced, UpdateRhs());

      // If we have received enough inputs to produce the next output batch
      // (decided by IsUpToDateWithLhsRow), we will perform the join and
      // materialize the output batch. The join is done by advancing through
      // the LHS and adding joined row to rows_ (done by Emplace). Finally,
      // input batches that are no longer needed are removed to free up memory.
      if (IsUpToDateWithLhsRow()) {
        dst.Emplace(state_, tolerance_);
        ARROW_ASSIGN_OR_RAISE(bool advanced, lhs.Advance());
        if (!advanced) break;  // if we can't advance LHS, we're done for this batch
      } else {
        if (!any_rhs_advanced) break;  // need to wait for new data
      }
    }

    // Prune memo entries that have expired (to bound memory consumption)
    if (!lhs.Empty()) {
      for (size_t i = 1; i < state_.size(); ++i) {
        state_[i]->RemoveMemoEntriesWithLesserTime(lhs.GetLatestTime() - tolerance_);
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
      outputs_[0]->InputFinished(this, batches_produced_);
      finished_.MarkFinished();
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
               const std::vector<col_index_t>& indices_of_on_key,
               const std::vector<std::vector<col_index_t>>& indices_of_by_key,
               OnType tolerance, std::shared_ptr<Schema> output_schema,
               std::vector<std::unique_ptr<KeyHasher>> key_hashers, bool must_hash,
               bool may_rehash);

  Status Init() override {
    auto inputs = this->inputs();
    for (size_t i = 0; i < inputs.size(); i++) {
      RETURN_NOT_OK(key_hashers_[i]->Init(plan()->exec_context(), output_schema()));
      state_.push_back(std::make_unique<InputState>(
          must_hash_, may_rehash_, key_hashers_[i].get(), inputs[i]->output_schema(),
          indices_of_on_key_[i], indices_of_by_key_[i]));
    }

    col_index_t dst_offset = 0;
    for (auto& state : state_)
      dst_offset = state->InitSrcToDstMapping(dst_offset, !!dst_offset);

    return Status::OK();
  }

  virtual ~AsofJoinNode() {
    process_.Push(false);  // poison pill
    process_thread_.join();
  }

  const std::vector<col_index_t>& indices_of_on_key() { return indices_of_on_key_; }
  const std::vector<std::vector<col_index_t>>& indices_of_by_key() {
    return indices_of_by_key_;
  }

  static Status is_valid_on_field(const std::shared_ptr<Field>& field) {
    switch (field->type()->id()) {
      case Type::INT8:
      case Type::INT16:
      case Type::INT32:
      case Type::INT64:
      case Type::UINT8:
      case Type::UINT16:
      case Type::UINT32:
      case Type::UINT64:
      case Type::DATE32:
      case Type::DATE64:
      case Type::TIME32:
      case Type::TIME64:
      case Type::TIMESTAMP:
        return Status::OK();
      default:
        return Status::Invalid("Unsupported type for on-key ", field->name(), " : ",
                               field->type()->ToString());
    }
  }

  static Status is_valid_by_field(const std::shared_ptr<Field>& field) {
    switch (field->type()->id()) {
      case Type::INT8:
      case Type::INT16:
      case Type::INT32:
      case Type::INT64:
      case Type::UINT8:
      case Type::UINT16:
      case Type::UINT32:
      case Type::UINT64:
      case Type::DATE32:
      case Type::DATE64:
      case Type::TIME32:
      case Type::TIME64:
      case Type::TIMESTAMP:
      case Type::STRING:
      case Type::LARGE_STRING:
      case Type::BINARY:
      case Type::LARGE_BINARY:
        return Status::OK();
      default:
        return Status::Invalid("Unsupported type for by-key ", field->name(), " : ",
                               field->type()->ToString());
    }
  }

  static Status is_valid_data_field(const std::shared_ptr<Field>& field) {
    switch (field->type()->id()) {
      case Type::INT8:
      case Type::INT16:
      case Type::INT32:
      case Type::INT64:
      case Type::UINT8:
      case Type::UINT16:
      case Type::UINT32:
      case Type::UINT64:
      case Type::FLOAT:
      case Type::DOUBLE:
      case Type::DATE32:
      case Type::DATE64:
      case Type::TIME32:
      case Type::TIME64:
      case Type::TIMESTAMP:
      case Type::STRING:
      case Type::LARGE_STRING:
      case Type::BINARY:
      case Type::LARGE_BINARY:
        return Status::OK();
      default:
        return Status::Invalid("Unsupported type for data field ", field->name(), " : ",
                               field->type()->ToString());
    }
  }

  static arrow::Result<std::shared_ptr<Schema>> MakeOutputSchema(
      const std::vector<ExecNode*>& inputs,
      const std::vector<col_index_t>& indices_of_on_key,
      const std::vector<std::vector<col_index_t>>& indices_of_by_key) {
    std::vector<std::shared_ptr<arrow::Field>> fields;

    size_t n_by = indices_of_by_key[0].size();
    const DataType* on_key_type = NULLPTR;
    std::vector<const DataType*> by_key_type(n_by, NULLPTR);
    // Take all non-key, non-time RHS fields
    for (size_t j = 0; j < inputs.size(); ++j) {
      const auto& input_schema = inputs[j]->output_schema();
      const auto& on_field_ix = indices_of_on_key[j];
      const auto& by_field_ix = indices_of_by_key[j];

      if ((on_field_ix == -1) || std_has(by_field_ix, -1)) {
        return Status::Invalid("Missing join key on table ", j);
      }

      const auto& on_field = input_schema->fields()[on_field_ix];
      std::vector<const Field*> by_field(n_by);
      for (size_t k = 0; k < n_by; k++) {
        by_field[k] = input_schema->fields()[by_field_ix[k]].get();
      }

      if (on_key_type == NULLPTR) {
        on_key_type = on_field->type().get();
      } else if (*on_key_type != *on_field->type()) {
        return Status::Invalid("Expected on-key type ", *on_key_type, " but got ",
                               *on_field->type(), " for field ", on_field->name(),
                               " in input ", j);
      }
      for (size_t k = 0; k < n_by; k++) {
        if (by_key_type[k] == NULLPTR) {
          by_key_type[k] = by_field[k]->type().get();
        } else if (*by_key_type[k] != *by_field[k]->type()) {
          return Status::Invalid("Expected on-key type ", *by_key_type[k], " but got ",
                                 *by_field[k]->type(), " for field ", by_field[k]->name(),
                                 " in input ", j);
        }
      }

      for (int i = 0; i < input_schema->num_fields(); ++i) {
        const auto field = input_schema->field(i);
        if (i == on_field_ix) {
          ARROW_RETURN_NOT_OK(is_valid_on_field(field));
          // Only add on field from the left table
          if (j == 0) {
            fields.push_back(field);
          }
        } else if (std_has(by_field_ix, i)) {
          ARROW_RETURN_NOT_OK(is_valid_by_field(field));
          // Only add by field from the left table
          if (j == 0) {
            fields.push_back(field);
          }
        } else {
          ARROW_RETURN_NOT_OK(is_valid_data_field(field));
          fields.push_back(field);
        }
      }
    }
    return std::make_shared<arrow::Schema>(fields);
  }

  static inline Result<col_index_t> FindColIndex(const Schema& schema,
                                                 const FieldRef& field_ref,
                                                 std::string_view key_kind) {
    auto match_res = field_ref.FindOne(schema);
    if (!match_res.ok()) {
      return Status::Invalid("Bad join key on table : ", match_res.status().message());
    }
    ARROW_ASSIGN_OR_RAISE(auto match, match_res);
    if (match.indices().size() != 1) {
      return Status::Invalid("AsOfJoinNode does not support a nested ", key_kind, "-key ",
                             field_ref.ToString());
    }
    return match.indices()[0];
  }

  static arrow::Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                       const ExecNodeOptions& options) {
    DCHECK_GE(inputs.size(), 2) << "Must have at least two inputs";

    const auto& join_options = checked_cast<const AsofJoinNodeOptions&>(options);
    if (join_options.tolerance < 0) {
      return Status::Invalid("AsOfJoin tolerance must be non-negative but is ",
                             join_options.tolerance);
    }

    size_t n_input = inputs.size(), n_by = join_options.by_key.size();
    std::vector<std::string> input_labels(n_input);
    std::vector<col_index_t> indices_of_on_key(n_input);
    std::vector<std::vector<col_index_t>> indices_of_by_key(
        n_input, std::vector<col_index_t>(n_by));
    for (size_t i = 0; i < n_input; ++i) {
      input_labels[i] = i == 0 ? "left" : "right_" + std::to_string(i);
      const Schema& input_schema = *inputs[i]->output_schema();
      ARROW_ASSIGN_OR_RAISE(indices_of_on_key[i],
                            FindColIndex(input_schema, join_options.on_key, "on"));
      for (size_t k = 0; k < n_by; k++) {
        ARROW_ASSIGN_OR_RAISE(indices_of_by_key[i][k],
                              FindColIndex(input_schema, join_options.by_key[k], "by"));
      }
    }

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Schema> output_schema,
                          MakeOutputSchema(inputs, indices_of_on_key, indices_of_by_key));

    std::vector<std::unique_ptr<KeyHasher>> key_hashers;
    for (size_t i = 0; i < n_input; i++) {
      key_hashers.push_back(
          std::make_unique<KeyHasher>(indices_of_by_key[i]));
    }
    bool must_hash =
        n_by > 1 ||
        (n_by == 1 &&
         !is_primitive(
             inputs[0]->output_schema()->field(indices_of_by_key[0][0])->type()->id()));
    bool may_rehash = n_by == 1 && !must_hash;
    return plan->EmplaceNode<AsofJoinNode>(
        plan, inputs, std::move(input_labels), std::move(indices_of_on_key),
        std::move(indices_of_by_key), time_value(join_options.tolerance),
        std::move(output_schema), std::move(key_hashers), must_hash, may_rehash);
  }

  const char* kind_name() const override { return "AsofJoinNode"; }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    // Get the input
    ARROW_DCHECK(std_has(inputs_, input));
    size_t k = std_find(inputs_, input) - inputs_.begin();

    // Put into the queue
    auto rb = *batch.ToRecordBatch(input->output_schema());
    Status st = state_.at(k)->Push(rb);
    if (!st.ok()) {
      ErrorReceived(input, st);
      return;
    }
    process_.Push(true);
  }
  void ErrorReceived(ExecNode* input, Status error) override {
    outputs_[0]->ErrorReceived(this, std::move(error));
  }
  void InputFinished(ExecNode* input, int total_batches) override {
    {
      std::lock_guard<std::mutex> guard(gate_);
      ARROW_DCHECK(std_has(inputs_, input));
      size_t k = std_find(inputs_, input) - inputs_.begin();
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
  void StopProducing() override {
    process_.Clear();
    process_.Push(false);
  }
  arrow::Future<> finished() override { return finished_; }

 private:
  arrow::Future<> finished_;
  std::vector<col_index_t> indices_of_on_key_;
  std::vector<std::vector<col_index_t>> indices_of_by_key_;
  std::vector<std::unique_ptr<KeyHasher>> key_hashers_;
  bool must_hash_;
  bool may_rehash_;
  // InputStates
  // Each input state correponds to an input table
  std::vector<std::unique_ptr<InputState>> state_;
  std::mutex gate_;
  OnType tolerance_;

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
                           const std::vector<col_index_t>& indices_of_on_key,
                           const std::vector<std::vector<col_index_t>>& indices_of_by_key,
                           OnType tolerance, std::shared_ptr<Schema> output_schema,
                           std::vector<std::unique_ptr<KeyHasher>> key_hashers,
                           bool must_hash, bool may_rehash)
    : ExecNode(plan, inputs, input_labels,
               /*output_schema=*/std::move(output_schema),
               /*num_outputs=*/1),
      indices_of_on_key_(std::move(indices_of_on_key)),
      indices_of_by_key_(std::move(indices_of_by_key)),
      key_hashers_(std::move(key_hashers)),
      must_hash_(must_hash),
      may_rehash_(may_rehash),
      tolerance_(tolerance),
      process_(),
      process_thread_(&AsofJoinNode::ProcessThreadWrapper, this) {
  finished_ = arrow::Future<>::MakeFinished();
}

namespace internal {
void RegisterAsofJoinNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("asofjoin", AsofJoinNode::Make));
}
}  // namespace internal

}  // namespace compute
}  // namespace arrow
