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

#include "arrow/acero/asof_join_node.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <deque>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/acero/accumulation_queue.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/exec_plan_internal.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/time_series_util.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/util.h"
#include "arrow/compute/key_hash_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/string.h"

namespace arrow {

using compute::ExecBatch;
using compute::NullPlacement;
using compute::SortKey;
using compute::SortOrder;
using internal::checked_cast;
using internal::ToChars;

namespace acero {
namespace {

using OnType = uint64_t;
using col_index_t = int;
using Task = util::SequencingQueue::Task;

enum class CandidateMode { Latest, Ordered };

class Tolerance {
 public:
  struct Bounds {
    OnType lower;
    OnType upper;
  };

  explicit Tolerance(AsofJoinNodeOptions::ToleranceRange tolerance,
                     bool prefer_earlier_on_tie)
      : lower_(tolerance.lower),
        upper_(tolerance.upper),
        prefer_earlier_on_tie_(prefer_earlier_on_tie) {}

  CandidateMode mode() const {
    return upper_ <= 0 ? CandidateMode::Latest : CandidateMode::Ordered;
  }

  bool prefer_earlier_on_tie() const { return prefer_earlier_on_tie_; }

  std::optional<Bounds> BoundsFor(OnType left) const {
    const OffsetResult lower = AddOffset(left, lower_);
    const OffsetResult upper = AddOffset(left, upper_);
    if (lower.above_max || upper.below_min) {
      return std::nullopt;
    }
    DCHECK_LE(lower.value, upper.value);
    return Bounds{lower.value, upper.value};
  }

 private:
  struct OffsetResult {
    OnType value;
    bool below_min = false;
    bool above_max = false;
  };

  static uint64_t Magnitude(int64_t offset) {
    return offset >= 0 ? static_cast<uint64_t>(offset)
                       : static_cast<uint64_t>(-(offset + 1)) + uint64_t{1};
  }

  static OffsetResult AddOffset(OnType value, int64_t offset) {
    const uint64_t magnitude = Magnitude(offset);
    if (offset < 0) {
      if (value < magnitude) {
        return OffsetResult{0, /*below_min=*/true};
      }
      return OffsetResult{value - magnitude};
    }
    if (value > std::numeric_limits<OnType>::max() - magnitude) {
      return OffsetResult{std::numeric_limits<OnType>::max(), /*below_min=*/false,
                          /*above_max=*/true};
    }
    return OffsetResult{value + magnitude};
  }

  const int64_t lower_;
  const int64_t upper_;
  const bool prefer_earlier_on_tie_;
};

template <Type::type kTypeId>
Result<std::optional<OnType>> ReadTimeValue(const Datum& value, int64_t row) {
  using ArrowType = typename TypeIdTraits<kTypeId>::Type;
  using CType = typename TypeTraits<ArrowType>::CType;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  if (value.is_scalar()) {
    const auto& scalar = checked_cast<const ScalarType&>(*value.scalar());
    if (!scalar.is_valid) {
      return std::nullopt;
    }
    return NormalizeTime(static_cast<CType>(scalar.value));
  }
  if (value.is_array()) {
    ArraySpan array(*value.array());
    if (array.IsNull(row)) {
      return std::nullopt;
    }
    return NormalizeTime(array.GetValues<CType>(1)[row]);
  }
  return Status::Invalid("AsofJoin on-key must be an array or scalar, but got ",
                         ::arrow::ToString(value.kind()));
}

Result<std::optional<OnType>> ReadTimeValue(const Datum& value, int64_t row) {
  switch (value.type()->id()) {
#define ASOF_TIME_CASE(ID) \
  case Type::ID:           \
    return ReadTimeValue<Type::ID>(value, row)
    ASOF_TIME_CASE(INT8);
    ASOF_TIME_CASE(INT16);
    ASOF_TIME_CASE(INT32);
    ASOF_TIME_CASE(INT64);
    ASOF_TIME_CASE(UINT8);
    ASOF_TIME_CASE(UINT16);
    ASOF_TIME_CASE(UINT32);
    ASOF_TIME_CASE(UINT64);
    ASOF_TIME_CASE(DATE32);
    ASOF_TIME_CASE(DATE64);
    ASOF_TIME_CASE(TIME32);
    ASOF_TIME_CASE(TIME64);
    ASOF_TIME_CASE(TIMESTAMP);
#undef ASOF_TIME_CASE
    default:
      return Status::Invalid("Unsupported AsofJoin on-key type ",
                             value.type()->ToString());
  }
}

struct PreparedKeyColumn {
  int64_t Row(int64_t row) const { return is_scalar ? 0 : row; }

  std::shared_ptr<Array> values;
  bool is_scalar;
};

struct PreparedBatch {
  uint64_t Hash(int64_t row) const { return hashes.empty() ? 0 : hashes[row]; }

  bool KeysEqual(int64_t row, const PreparedBatch& other, int64_t other_row) const {
    DCHECK_EQ(key_columns.size(), other.key_columns.size());
    for (size_t i = 0; i < key_columns.size(); ++i) {
      const PreparedKeyColumn& left = key_columns[i];
      const PreparedKeyColumn& right = other.key_columns[i];
      const int64_t left_row = left.Row(row);
      const int64_t right_row = right.Row(other_row);
      if (!left.values->RangeEquals(*right.values, left_row, left_row + 1, right_row)) {
        return false;
      }
    }
    return true;
  }

  ExecBatch batch;
  std::vector<std::optional<OnType>> times;
  std::vector<PreparedKeyColumn> key_columns;
  std::vector<uint64_t> hashes;
};

Result<std::shared_ptr<PreparedBatch>> PrepareBatch(
    ExecBatch batch, col_index_t on_key, const std::vector<col_index_t>& by_keys,
    compute::ExecContext* ctx) {
  if (batch.length > std::numeric_limits<int32_t>::max()) {
    return Status::CapacityError("AsofJoin input batch has too many rows: ",
                                 batch.length);
  }

  auto prepared = std::make_shared<PreparedBatch>();
  prepared->batch = std::move(batch);
  prepared->times.reserve(prepared->batch.length);
  for (int64_t row = 0; row < prepared->batch.length; ++row) {
    ARROW_ASSIGN_OR_RAISE(auto time, ReadTimeValue(prepared->batch.values[on_key], row));
    prepared->times.push_back(time);
  }

  if (!by_keys.empty()) {
    std::vector<Datum> key_values;
    key_values.reserve(by_keys.size());
    prepared->key_columns.reserve(by_keys.size());
    for (col_index_t by_key : by_keys) {
      const Datum& value = prepared->batch.values[by_key];
      if (value.is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(
            auto scalar_array,
            MakeArrayFromScalar(*value.scalar(), /*length=*/1, ctx->memory_pool()));
        prepared->key_columns.push_back({std::move(scalar_array), true});
        ARROW_ASSIGN_OR_RAISE(auto hash_array,
                              MakeArrayFromScalar(*value.scalar(), prepared->batch.length,
                                                  ctx->memory_pool()));
        key_values.emplace_back(std::move(hash_array));
      } else {
        key_values.push_back(value);
        prepared->key_columns.push_back({MakeArray(value.array()), false});
      }
    }

    prepared->hashes.resize(prepared->batch.length);
    if (prepared->batch.length > 0) {
      ExecBatch key_batch(std::move(key_values), prepared->batch.length);
      ::arrow::util::TempVectorStack temp_stack;
      ARROW_RETURN_NOT_OK(temp_stack.Init(ctx->memory_pool(),
                                          compute::Hashing64::kHashBatchTempStackUsage));
      std::vector<compute::KeyColumnArray> column_arrays;
      ARROW_RETURN_NOT_OK(
          compute::Hashing64::HashBatch(key_batch, prepared->hashes.data(), column_arrays,
                                        ctx->cpu_info()->hardware_flags(), &temp_stack,
                                        /*start_row=*/0, prepared->batch.length));
    }
  }
  return prepared;
}

struct RowRef {
  std::shared_ptr<PreparedBatch> batch;
  int64_t row;
};

bool KeysEqual(const RowRef& left, const RowRef& right) {
  return left.batch->KeysEqual(left.row, *right.batch, right.row);
}

struct FlowAction {
  enum class Kind { None, Pause, Resume };

  void Apply() const {
    if (kind == Kind::Pause) {
      input->PauseProducing(output, counter);
    } else if (kind == Kind::Resume) {
      input->ResumeProducing(output, counter);
    }
  }

  Kind kind = Kind::None;
  ExecNode* input = nullptr;
  ExecNode* output = nullptr;
  int32_t counter = 0;
};

class AsofJoinNode;

class InputState final : public util::SequencingQueue::Processor {
 public:
  InputState(AsofJoinNode* node, size_t index, ExecNode* input, col_index_t on_key,
             std::vector<col_index_t> by_keys,
             std::optional<NullPlacement> null_placement);

  Status InsertBatch(ExecBatch batch);
  Result<std::optional<Task>> Process(ExecBatch token) override;
  void Schedule(Task task) override;

  FlowAction BatchBuffered();
  FlowAction BatchConsumed();
  FlowAction Shutdown();

 private:
  FlowAction SetUpstreamPausedUnlocked(bool paused);
  Status ValidateTimes(const PreparedBatch& batch);

  static constexpr size_t kLowWatermark = 4;
  static constexpr size_t kHighWatermark = 8;

  AsofJoinNode* node_;
  size_t index_;
  ExecNode* input_;
  col_index_t on_key_;
  std::vector<col_index_t> by_keys_;
  std::unique_ptr<util::SequencingQueue> sequencer_;

  std::mutex prepared_mutex_;
  std::unordered_map<int64_t, std::shared_ptr<PreparedBatch>> prepared_;

  std::optional<OnType> last_time_;
  std::optional<NullPlacement> null_placement_;
  bool saw_trailing_null_ = false;

  std::mutex flow_mutex_;
  size_t buffered_batches_ = 0;
  bool upstream_paused_ = false;
  int32_t outgoing_counter_ = 0;
  bool shutdown_ = false;
};

class RhsLane {
 public:
  RhsLane(AsofJoinNode* node, size_t lane_index, size_t input_index,
          std::vector<col_index_t> payload_columns, Tolerance tolerance, MemoryPool* pool)
      : node_(node),
        lane_index_(lane_index),
        input_index_(input_index),
        payload_columns_(std::move(payload_columns)),
        tolerance_(tolerance),
        pool_(pool) {}

  Result<std::optional<Task>> Enqueue(std::shared_ptr<PreparedBatch> batch);
  Result<std::optional<Task>> SetTotal(int total_batches);
  Result<Task> Assign(std::shared_ptr<PreparedBatch> left);
  void Stop();

 private:
  enum class Phase {
    // No left batch is assigned.  Assign() installs a job, posts Run(), and moves to
    // Claimed; Stop() moves to Stopped.
    NoJob,

    // The current job reached the end of available RHS data.  It waits for Enqueue()
    // or RHS completion; the waking producer posts Run() and moves to Claimed.
    // Stop() moves to Stopped.
    Waiting,

    // One posted or running Run() task exclusively owns the job, preventing duplicate
    // runners and lost wakes.  Blocking moves to Waiting, completing the job moves to
    // NoJob, and Stop() moves to Stopped.
    Claimed,

    // The lane is terminal.  It ignores new data and assignments and never exits.
    Stopped,
  };

  struct SelectionRun {
    std::optional<RowRef> source;
    int64_t length;
  };

  struct Job {
    explicit Job(std::shared_ptr<PreparedBatch> left) : left(std::move(left)) {}

    void AppendMatch(std::optional<RowRef> match) {
      if (!selections.empty()) {
        SelectionRun& previous = selections.back();
        if ((!previous.source && !match) ||
            (previous.source && match && previous.source->batch == match->batch &&
             previous.source->row + previous.length == match->row)) {
          ++previous.length;
          return;
        }
      }
      selections.push_back({std::move(match), 1});
    }

    std::shared_ptr<PreparedBatch> left;
    int64_t left_row = 0;
    std::vector<SelectionRun> selections;
  };

  struct Candidate {
    RowRef row;
    uint64_t version;
  };

  struct OrderedCandidate {
    RowRef row;
    OnType time;
    uint64_t version;
  };

  struct OrderedCandidates {
    std::deque<OrderedCandidate> rows;
  };

  struct ExpiryEntry {
    OnType time;
    uint64_t hash;
    uint64_t version;
  };

  struct PeekResult {
    enum class Kind { Row, Blocked, End };
    Kind kind;
    RowRef row;
  };

  Status Run();
  Result<PeekResult> PeekNext();
  void ConsumeNext();
  bool StreamEndedUnlocked() const;
  bool WaitOrRetry();
  void RememberBackward(const RowRef& row, OnType time);
  void ExpireBackward(OnType lower_bound);
  void RememberOrdered(const RowRef& row, OnType time);
  void ExpireOrdered(OnType lower_bound);
  std::optional<RowRef> MatchBackward(const RowRef& key) const;
  std::optional<RowRef> MatchOrdered(const RowRef& key, OnType left_time) const;
  Result<std::vector<Datum>> Materialize(const Job& job) const;

  AsofJoinNode* node_;
  size_t lane_index_;
  size_t input_index_;
  std::vector<col_index_t> payload_columns_;
  Tolerance tolerance_;
  MemoryPool* pool_;

  std::mutex mutex_;
  Phase phase_ = Phase::NoJob;
  std::shared_ptr<Job> job_;
  std::deque<std::shared_ptr<PreparedBatch>> batches_;
  // Only the lane owner touches the active batch and row.  Producers append to
  // batches_ under mutex_ and ownership transfers between lane tasks through phase_.
  std::shared_ptr<PreparedBatch> current_batch_;
  int64_t current_row_ = 0;
  int received_batches_ = 0;
  std::optional<int> total_batches_;

  uint64_t next_version_ = 0;
  std::unordered_map<uint64_t, std::vector<Candidate>> backward_candidates_;
  std::deque<ExpiryEntry> backward_expiry_;
  std::unordered_map<uint64_t, std::vector<OrderedCandidates>> ordered_candidates_;
  std::deque<ExpiryEntry> ordered_expiry_;
};

// No left batch is active.  It waits for an activatable queued batch and then moves to
// Matching; exhausting the left input with an empty queue, or Stop(), moves to Terminal.
struct WaitingForLeft {};

// Owns the active left batch while RHS lanes match it in parallel.  It waits for every
// lane result; the last result assembles the output and moves to OutputInFlight.  Stop()
// moves to Terminal.
struct Matching {
  explicit Matching(std::shared_ptr<PreparedBatch> left, size_t lane_count)
      : left(std::move(left)), results(lane_count), remaining(lane_count) {}
  std::shared_ptr<PreparedBatch> left;
  std::vector<std::optional<std::vector<Datum>>> results;
  size_t remaining;
};

// Keeps the active left batch alive while its assembled output is delivered.  When
// downstream InputReceived succeeds, output completion moves to WaitingForLeft.  Stop()
// moves to Terminal.
struct OutputInFlight {
  explicit OutputInFlight(std::shared_ptr<PreparedBatch> left) : left(std::move(left)) {}
  std::shared_ptr<PreparedBatch> left;
};

// No coordinator work remains.  Normal left-input exhaustion or Stop() enters this
// state; subsequent coordinator events are ignored and it never exits.
struct Terminal {};

// Controls whether WaitingForLeft may activate another batch.
enum class LeftGate {
  // Downstream accepts output, so a queued left batch may activate immediately.
  // PauseProducing moves to Paused; sequencing the complete left input moves to Flushing.
  Open,

  // Downstream backpressure holds queued left batches at the generation boundary.  It
  // waits for ResumeProducing and returns to Open, unless left completion moves it to
  // Flushing first.
  Paused,

  // Every declared left batch is sequenced, so the fixed tail drains without waiting for
  // downstream resume.  RHS lanes may still wait for right data; this gate never exits.
  Flushing,
};

using CoordinatorState = std::variant<WaitingForLeft, Matching, OutputInFlight, Terminal>;

class AsofJoinNode : public ExecNode {
 public:
  AsofJoinNode(ExecPlan* plan, NodeVector inputs, std::vector<std::string> input_labels,
               std::vector<col_index_t> on_keys,
               std::vector<std::vector<col_index_t>> by_keys,
               std::vector<std::optional<NullPlacement>> input_null_placements,
               AsofJoinNodeOptions join_options, std::shared_ptr<Schema> output_schema,
               Ordering output_ordering)
      : ExecNode(plan, std::move(inputs), std::move(input_labels),
                 std::move(output_schema)),
        ordering_(std::move(output_ordering)),
        on_keys_(std::move(on_keys)),
        by_keys_(std::move(by_keys)),
        input_null_placements_(std::move(input_null_placements)),
        tolerance_(join_options.tolerance, join_options.prefer_earlier_on_tie) {}

  Status Init() override {
    ARROW_RETURN_NOT_OK(ExecNode::Init());
    input_states_.reserve(inputs_.size());
    rhs_lanes_.reserve(inputs_.size() - 1);
    for (size_t i = 0; i < inputs_.size(); ++i) {
      input_states_.push_back(std::make_unique<InputState>(
          this, i, inputs_[i], on_keys_[i], by_keys_[i], input_null_placements_[i]));
      if (i == 0) {
        continue;
      }
      std::vector<col_index_t> payload_columns;
      for (int column = 0; column < inputs_[i]->output_schema()->num_fields(); ++column) {
        if (column != on_keys_[i] && std::find(by_keys_[i].begin(), by_keys_[i].end(),
                                               column) == by_keys_[i].end()) {
          payload_columns.push_back(column);
        }
      }
      rhs_lanes_.push_back(
          std::make_unique<RhsLane>(this, i - 1, i, std::move(payload_columns),
                                    tolerance_, plan()->query_context()->memory_pool()));
    }
    return Status::OK();
  }

  const char* kind_name() const override { return "AsofJoinNode"; }
  const Ordering& ordering() const override { return ordering_; }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    if (terminal_.load()) {
      return Status::OK();
    }
    if (batch.index == compute::kUnsequencedIndex) {
      return Status::Invalid("AsofJoin requires sequenced input");
    }
    auto it = std::find(inputs_.begin(), inputs_.end(), input);
    if (it == inputs_.end()) {
      return Status::Invalid("AsofJoin received a batch from an unknown input");
    }
    return input_states_[it - inputs_.begin()]->InsertBatch(std::move(batch));
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    if (terminal_.load()) {
      return Status::OK();
    }
    if (total_batches < 0) {
      return Status::Invalid("AsofJoin input reported a negative batch count");
    }
    auto it = std::find(inputs_.begin(), inputs_.end(), input);
    if (it == inputs_.end()) {
      return Status::Invalid("AsofJoin received completion from an unknown input");
    }
    size_t index = static_cast<size_t>(it - inputs_.begin());
    if (index == 0) {
      return LeftInputFinished(total_batches);
    }
    ARROW_ASSIGN_OR_RAISE(auto task, rhs_lanes_[index - 1]->SetTotal(total_batches));
    if (task) {
      return std::move(*task)();
    }
    return Status::OK();
  }

  Status StartProducing() override { return Status::OK(); }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    std::lock_guard lock(coordinator_mutex_);
    if (std::holds_alternative<Terminal>(coordinator_) ||
        counter <= downstream_counter_) {
      return;
    }
    downstream_counter_ = counter;
    if (left_gate_ != LeftGate::Flushing) {
      left_gate_ = LeftGate::Paused;
    }
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    bool activate = false;
    {
      std::lock_guard lock(coordinator_mutex_);
      if (std::holds_alternative<Terminal>(coordinator_) ||
          counter <= downstream_counter_) {
        return;
      }
      downstream_counter_ = counter;
      activate = left_gate_ == LeftGate::Paused;
      if (left_gate_ != LeftGate::Flushing) {
        left_gate_ = LeftGate::Open;
      }
    }
    if (activate) {
      Schedule([this] { return ActivateAfterResume(); }, "AsofJoinNode::Resume");
    }
  }

  Status StopProducingImpl() override {
    terminal_.store(true);
    {
      std::lock_guard lock(coordinator_mutex_);
      coordinator_ = Terminal{};
      left_batches_.clear();
    }
    for (auto& lane : rhs_lanes_) {
      lane->Stop();
    }
    return StopInputs();
  }

  void Schedule(Task task, std::string_view name = "AsofJoinNode::Lane") {
    plan()->query_context()->ScheduleTask(std::move(task), name);
  }

  Result<std::optional<Task>> OnSequenced(size_t input_index,
                                          std::shared_ptr<PreparedBatch> batch) {
    if (terminal_.load()) {
      return Task([this, input_index] {
        InputBatchConsumed(input_index);
        return Status::OK();
      });
    }
    if (input_index == 0) {
      ARROW_RETURN_NOT_OK(OnLeftSequenced(std::move(batch)));
      return std::nullopt;
    }
    return rhs_lanes_[input_index - 1]->Enqueue(std::move(batch));
  }

  void InputBatchConsumed(size_t input_index) {
    input_states_[input_index]->BatchConsumed().Apply();
  }

  bool IsTerminal() const { return terminal_.load(); }

  Status LaneCompleted(size_t lane_index, std::vector<Datum> values) {
    ExecBatch output_batch;
    {
      std::lock_guard lock(coordinator_mutex_);
      auto* matching = std::get_if<Matching>(&coordinator_);
      if (matching == nullptr) {
        return Status::OK();
      }
      if (lane_index >= matching->results.size() || matching->results[lane_index]) {
        return Status::Invalid("AsofJoin lane completed an unexpected job");
      }
      matching->results[lane_index] = std::move(values);
      if (--matching->remaining != 0) {
        return Status::OK();
      }

      auto left = matching->left;
      std::vector<Datum> output_values = left->batch.values;
      for (auto& lane_result : matching->results) {
        DCHECK(lane_result.has_value());
        for (Datum& value : *lane_result) {
          output_values.push_back(std::move(value));
        }
      }
      output_batch = ExecBatch(std::move(output_values), left->batch.length);
      output_batch.index = left->batch.index;
      coordinator_ = OutputInFlight{std::move(left)};
    }

    ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(output_batch)));
    return OutputDelivered();
  }

  static Status IsValidOnField(const std::shared_ptr<Field>& field) {
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

  static Status IsValidByField(const std::shared_ptr<Field>& field) {
    switch (field->type()->id()) {
      case Type::BOOL:
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
      case Type::FIXED_SIZE_BINARY:
      case Type::DECIMAL32:
      case Type::DECIMAL64:
      case Type::DECIMAL128:
      case Type::DECIMAL256:
        return Status::OK();
      default:
        return Status::Invalid("Unsupported type for by-key ", field->name(), " : ",
                               field->type()->ToString());
    }
  }

  static Result<std::shared_ptr<Schema>> MakeOutputSchema(
      const std::vector<std::shared_ptr<Schema>>& input_schemas,
      const std::vector<col_index_t>& on_keys,
      const std::vector<std::vector<col_index_t>>& by_keys) {
    if (input_schemas.size() < 2 || input_schemas.size() != on_keys.size() ||
        input_schemas.size() != by_keys.size()) {
      return Status::Invalid("AsofJoin requires matching schemas and keys for at least ",
                             "two inputs");
    }

    const size_t by_key_count = by_keys[0].size();
    const DataType* on_type = nullptr;
    std::vector<const DataType*> by_types(by_key_count, nullptr);
    std::vector<std::shared_ptr<Field>> fields;

    for (size_t input_index = 0; input_index < input_schemas.size(); ++input_index) {
      const auto& input_schema = input_schemas[input_index];
      col_index_t on_key = on_keys[input_index];
      if (on_key < 0 || on_key >= input_schema->num_fields() ||
          by_keys[input_index].size() != by_key_count) {
        return Status::Invalid("Missing join key on table ", input_index);
      }
      const auto& on_field = input_schema->field(on_key);
      if (on_type == nullptr) {
        ARROW_RETURN_NOT_OK(IsValidOnField(on_field));
        on_type = on_field->type().get();
      } else if (*on_type != *on_field->type()) {
        return Status::Invalid("Expected on-key type ", *on_type, " but got ",
                               *on_field->type(), " for field ", on_field->name(),
                               " in input ", input_index);
      }

      for (size_t key_index = 0; key_index < by_key_count; ++key_index) {
        col_index_t by_key = by_keys[input_index][key_index];
        if (by_key < 0 || by_key >= input_schema->num_fields()) {
          return Status::Invalid("Missing join key on table ", input_index);
        }
        const auto& by_field = input_schema->field(by_key);
        if (by_types[key_index] == nullptr) {
          ARROW_RETURN_NOT_OK(IsValidByField(by_field));
          by_types[key_index] = by_field->type().get();
        } else if (*by_types[key_index] != *by_field->type()) {
          return Status::Invalid("Expected by-key type ", *by_types[key_index],
                                 " but got ", *by_field->type(), " for field ",
                                 by_field->name(), " in input ", input_index);
        }
      }

      for (int column = 0; column < input_schema->num_fields(); ++column) {
        bool is_key = column == on_key ||
                      std::find(by_keys[input_index].begin(), by_keys[input_index].end(),
                                column) != by_keys[input_index].end();
        if (input_index == 0 || !is_key) {
          auto field = input_schema->field(column);
          fields.push_back(input_index == 0 ? field : field->WithNullable(true));
        }
      }
    }
    return std::make_shared<Schema>(std::move(fields));
  }

  static Result<col_index_t> FindColIndex(const Schema& schema, const FieldRef& field_ref,
                                          std::string_view key_kind) {
    auto match_result = field_ref.FindOne(schema);
    if (!match_result.ok()) {
      return Status::Invalid("Bad join key on table : ", match_result.status().message());
    }
    ARROW_ASSIGN_OR_RAISE(auto match, std::move(match_result));
    if (match.indices().size() != 1) {
      return Status::Invalid("AsOfJoinNode does not support a nested ", key_kind, "-key ",
                             field_ref.ToString());
    }
    return match.indices()[0];
  }

  static Result<size_t> GetByKeySize(
      const std::vector<asofjoin::AsofJoinKeys>& input_keys) {
    if (input_keys.size() < 2) {
      return Status::Invalid("AsofJoin requires at least two inputs");
    }
    const size_t size = input_keys[0].by_key.size();
    for (const auto& keys : input_keys) {
      if (keys.by_key.size() != size) {
        return Status::Invalid("inconsistent size of by-key across inputs");
      }
    }
    return size;
  }

  static Result<std::vector<col_index_t>> GetIndicesOfOnKey(
      const std::vector<std::shared_ptr<Schema>>& input_schemas,
      const std::vector<asofjoin::AsofJoinKeys>& input_keys) {
    if (input_schemas.size() != input_keys.size()) {
      return Status::Invalid("mismatching number of input schema and keys");
    }
    std::vector<col_index_t> indices(input_schemas.size());
    for (size_t i = 0; i < input_schemas.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(indices[i],
                            FindColIndex(*input_schemas[i], input_keys[i].on_key, "on"));
    }
    return indices;
  }

  static Result<std::vector<std::vector<col_index_t>>> GetIndicesOfByKey(
      const std::vector<std::shared_ptr<Schema>>& input_schemas,
      const std::vector<asofjoin::AsofJoinKeys>& input_keys) {
    if (input_schemas.size() != input_keys.size()) {
      return Status::Invalid("mismatching number of input schema and keys");
    }
    ARROW_ASSIGN_OR_RAISE(size_t by_key_count, GetByKeySize(input_keys));
    std::vector<std::vector<col_index_t>> indices(input_schemas.size(),
                                                  std::vector<col_index_t>(by_key_count));
    for (size_t input_index = 0; input_index < input_schemas.size(); ++input_index) {
      for (size_t key_index = 0; key_index < by_key_count; ++key_index) {
        ARROW_ASSIGN_OR_RAISE(
            indices[input_index][key_index],
            FindColIndex(*input_schemas[input_index],
                         input_keys[input_index].by_key[key_index], "by"));
      }
    }
    return indices;
  }

  static Result<std::optional<NullPlacement>> ValidateInputOrdering(const ExecNode& input,
                                                                    col_index_t on_key,
                                                                    size_t input_index) {
    const Ordering& ordering = input.ordering();
    if (ordering.is_unordered()) {
      return Status::Invalid("AsofJoin input ", input_index,
                             " has no meaningful ordering");
    }
    if (ordering.is_implicit()) {
      return std::nullopt;
    }

    DCHECK(!ordering.sort_keys().empty());
    const SortKey& leading_key = ordering.sort_keys().front();
    auto match_result = leading_key.target.FindOne(*input.output_schema());
    if (!match_result.ok()) {
      return Status::Invalid(
          "AsofJoin input ", input_index,
          " has an invalid leading sort key: ", match_result.status().message());
    }
    ARROW_ASSIGN_OR_RAISE(auto match, std::move(match_result));
    if (leading_key.order != SortOrder::Ascending || match.indices().size() != 1 ||
        match.indices()[0] != on_key) {
      return Status::Invalid("AsofJoin input ", input_index,
                             " must be ordered by its ascending on-key");
    }
    return ordering.null_placement().value_or(leading_key.null_placement);
  }

  static Result<Ordering> NormalizeOutputOrdering(const ExecNode& left) {
    const Ordering& ordering = left.ordering();
    if (ordering.is_implicit() || ordering.is_unordered()) {
      return ordering;
    }

    std::vector<SortKey> sort_keys;
    sort_keys.reserve(ordering.sort_keys().size());
    for (const SortKey& sort_key : ordering.sort_keys()) {
      ARROW_ASSIGN_OR_RAISE(auto path, sort_key.target.FindOne(*left.output_schema()));
      sort_keys.emplace_back(FieldRef(std::move(path)), sort_key.order,
                             ordering.null_placement().value_or(sort_key.null_placement));
    }
    return Ordering(std::move(sort_keys));
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    const auto& join_options = checked_cast<const AsofJoinNodeOptions&>(options);
    if (inputs.size() < 2 || inputs.size() != join_options.input_keys.size()) {
      return Status::Invalid("AsofJoin requires one key specification per input and at ",
                             "least two inputs");
    }
    if (join_options.tolerance.lower > join_options.tolerance.upper) {
      return Status::Invalid("AsofJoin tolerance lower bound must not exceed its upper ",
                             "bound");
    }
    ARROW_RETURN_NOT_OK(GetByKeySize(join_options.input_keys).status());

    std::vector<std::string> input_labels(inputs.size());
    std::vector<std::shared_ptr<Schema>> input_schemas(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
      input_labels[i] = i == 0 ? "left" : "right_" + ToChars(i);
      input_schemas[i] = inputs[i]->output_schema();
    }
    ARROW_ASSIGN_OR_RAISE(auto on_keys,
                          GetIndicesOfOnKey(input_schemas, join_options.input_keys));
    ARROW_ASSIGN_OR_RAISE(auto by_keys,
                          GetIndicesOfByKey(input_schemas, join_options.input_keys));
    std::vector<std::optional<NullPlacement>> input_null_placements(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(input_null_placements[i],
                            ValidateInputOrdering(*inputs[i], on_keys[i], i));
    }
    ARROW_ASSIGN_OR_RAISE(auto output_ordering, NormalizeOutputOrdering(*inputs.front()));
    ARROW_ASSIGN_OR_RAISE(auto output_schema,
                          MakeOutputSchema(input_schemas, on_keys, by_keys));
    return plan->EmplaceNode<AsofJoinNode>(
        plan, std::move(inputs), std::move(input_labels), std::move(on_keys),
        std::move(by_keys), std::move(input_null_placements), join_options,
        std::move(output_schema), std::move(output_ordering));
  }

 private:
  void MaybeEnterFlushingUnlocked() {
    if (left_total_batches_ && left_received_batches_ == *left_total_batches_) {
      left_gate_ = LeftGate::Flushing;
    }
  }

  Result<std::vector<Task>> ActivateNextUnlocked() {
    DCHECK(std::holds_alternative<WaitingForLeft>(coordinator_));
    DCHECK(!left_batches_.empty());
    auto left = std::move(left_batches_.front());
    left_batches_.pop_front();
    coordinator_ = Matching{left, rhs_lanes_.size()};

    std::vector<Task> tasks;
    tasks.reserve(rhs_lanes_.size());
    for (auto& lane : rhs_lanes_) {
      ARROW_ASSIGN_OR_RAISE(Task task, lane->Assign(left));
      tasks.push_back(std::move(task));
    }
    return tasks;
  }

  Status ActivateOrFinishUnlocked(std::vector<Task>* tasks, bool* finish) {
    if (!std::holds_alternative<WaitingForLeft>(coordinator_)) {
      return Status::OK();
    }
    if (!left_batches_.empty()) {
      if (left_gate_ == LeftGate::Paused) {
        return Status::OK();
      }
      ARROW_ASSIGN_OR_RAISE(*tasks, ActivateNextUnlocked());
      return Status::OK();
    }
    if (left_batches_.empty() && left_total_batches_ &&
        left_received_batches_ == *left_total_batches_) {
      coordinator_ = Terminal{};
      terminal_.store(true);
      *finish = true;
    }
    return Status::OK();
  }

  Status ActivateAfterResume() {
    std::vector<Task> tasks;
    bool finish = false;
    {
      std::lock_guard lock(coordinator_mutex_);
      if (std::holds_alternative<Terminal>(coordinator_) ||
          left_gate_ == LeftGate::Paused) {
        return Status::OK();
      }
      ARROW_RETURN_NOT_OK(ActivateOrFinishUnlocked(&tasks, &finish));
    }
    ScheduleAll(std::move(tasks));
    return finish ? FinishNormally() : Status::OK();
  }

  Status OnLeftSequenced(std::shared_ptr<PreparedBatch> batch) {
    std::vector<Task> tasks;
    bool finish = false;
    {
      std::lock_guard lock(coordinator_mutex_);
      if (std::holds_alternative<Terminal>(coordinator_)) {
        // This callback runs under SequencingQueue's mutex.  Defer flow-control
        // callbacks until that mutex has been released, since an upstream resume may
        // synchronously produce another batch.
        Schedule([this] {
          InputBatchConsumed(0);
          return Status::OK();
        });
        return Status::OK();
      }
      ++left_received_batches_;
      if (left_total_batches_ && left_received_batches_ > *left_total_batches_) {
        return Status::Invalid("AsofJoin left input produced more batches than declared");
      }
      left_batches_.push_back(std::move(batch));
      MaybeEnterFlushingUnlocked();
      ARROW_RETURN_NOT_OK(ActivateOrFinishUnlocked(&tasks, &finish));
    }
    DCHECK(!finish);
    // If this batch opened a generation, post work for every RHS lane.  RHS arrival,
    // in contrast, can continue inline when it wakes a waiting lane.
    ScheduleAll(std::move(tasks));
    return Status::OK();
  }

  Status LeftInputFinished(int total_batches) {
    std::vector<Task> tasks;
    bool finish = false;
    {
      std::lock_guard lock(coordinator_mutex_);
      if (std::holds_alternative<Terminal>(coordinator_)) {
        return Status::OK();
      }
      if (left_total_batches_) {
        return *left_total_batches_ == total_batches
                   ? Status::OK()
                   : Status::Invalid("AsofJoin left input changed its total batch count");
      }
      if (left_received_batches_ > total_batches) {
        return Status::Invalid("AsofJoin left input declared too few batches");
      }
      left_total_batches_ = total_batches;
      MaybeEnterFlushingUnlocked();
      ARROW_RETURN_NOT_OK(ActivateOrFinishUnlocked(&tasks, &finish));
    }
    ScheduleAll(std::move(tasks));
    return finish ? FinishNormally() : Status::OK();
  }

  Status OutputDelivered() {
    std::shared_ptr<PreparedBatch> left;
    std::vector<Task> tasks;
    bool finish = false;
    {
      std::lock_guard lock(coordinator_mutex_);
      auto* output = std::get_if<OutputInFlight>(&coordinator_);
      if (output == nullptr) {
        return Status::OK();
      }
      left = output->left;
      ++batches_produced_;
      coordinator_ = WaitingForLeft{};
      ARROW_RETURN_NOT_OK(ActivateOrFinishUnlocked(&tasks, &finish));
    }

    InputBatchConsumed(0);
    if (finish) {
      return FinishNormally();
    }
    // An output boundary is the hard yield point between left-batch generations.  A
    // downstream pause prevents the next one from opening until the LHS is exhausted;
    // then Flushing drains the finite tail.
    ScheduleAll(std::move(tasks));
    return Status::OK();
  }

  void ScheduleAll(std::vector<Task> tasks) {
    for (Task& task : tasks) {
      Schedule(std::move(task));
    }
  }

  Status FinishNormally() {
    Status status = output_->InputFinished(this, batches_produced_);
    status &= StopInputs();
    return status;
  }

  Status StopInputs() {
    Status status = Status::OK();
    for (auto& input_state : input_states_) {
      input_state->Shutdown().Apply();
    }
    for (ExecNode* input : inputs_) {
      status &= input->StopProducing();
    }
    return status;
  }

  const Ordering ordering_;
  std::vector<col_index_t> on_keys_;
  std::vector<std::vector<col_index_t>> by_keys_;
  std::vector<std::optional<NullPlacement>> input_null_placements_;
  Tolerance tolerance_;
  std::vector<std::unique_ptr<InputState>> input_states_;
  std::vector<std::unique_ptr<RhsLane>> rhs_lanes_;

  std::mutex coordinator_mutex_;
  CoordinatorState coordinator_ = WaitingForLeft{};
  LeftGate left_gate_ = LeftGate::Open;
  int32_t downstream_counter_ = std::numeric_limits<int32_t>::min();
  std::deque<std::shared_ptr<PreparedBatch>> left_batches_;
  int left_received_batches_ = 0;
  std::optional<int> left_total_batches_;
  int batches_produced_ = 0;
  std::atomic<bool> terminal_{false};
};

InputState::InputState(AsofJoinNode* node, size_t index, ExecNode* input,
                       col_index_t on_key, std::vector<col_index_t> by_keys,
                       std::optional<NullPlacement> null_placement)
    : node_(node),
      index_(index),
      input_(input),
      on_key_(on_key),
      by_keys_(std::move(by_keys)),
      sequencer_(util::SequencingQueue::Make(this)),
      null_placement_(null_placement) {}

Status InputState::InsertBatch(ExecBatch batch) {
  if (batch.index == compute::kUnsequencedIndex) {
    return Status::Invalid("AsofJoin requires sequenced input");
  }
  const int64_t index = batch.index;
  const int64_t length = batch.length;
  ARROW_ASSIGN_OR_RAISE(auto prepared,
                        PrepareBatch(std::move(batch), on_key_, by_keys_,
                                     node_->plan()->query_context()->exec_context()));
  {
    std::lock_guard lock(prepared_mutex_);
    if (!prepared_.emplace(index, std::move(prepared)).second) {
      return Status::Invalid("AsofJoin received duplicate batch index ", index,
                             " on input ", index_);
    }
  }
  ExecBatch token({}, length);
  token.index = index;
  return sequencer_->InsertBatch(std::move(token));
}

Result<std::optional<Task>> InputState::Process(ExecBatch token) {
  std::shared_ptr<PreparedBatch> prepared;
  {
    std::lock_guard lock(prepared_mutex_);
    auto it = prepared_.find(token.index);
    if (it == prepared_.end()) {
      return Status::Invalid("AsofJoin lost prepared batch ", token.index, " on input ",
                             index_);
    }
    prepared = std::move(it->second);
    prepared_.erase(it);
  }
  Status validation = ValidateTimes(*prepared);
  if (!validation.ok()) {
    return validation;
  }

  // Only sequenced batches count toward backpressure.  Counting physical arrivals can
  // deadlock with a reordering input: the high watermark may be reached by later
  // batches while that input still holds the missing earlier batch.  Apply the action
  // from the follow-up task because Process runs under SequencingQueue's mutex.
  FlowAction buffered = BatchBuffered();
  ARROW_ASSIGN_OR_RAISE(auto task, node_->OnSequenced(index_, std::move(prepared)));
  if (buffered.kind == FlowAction::Kind::None) {
    return task;
  }
  return Task([buffered, task = std::move(task)]() mutable {
    buffered.Apply();
    return task ? std::move(*task)() : Status::OK();
  });
}

void InputState::Schedule(Task task) { node_->Schedule(std::move(task)); }

Status InputState::ValidateTimes(const PreparedBatch& batch) {
  for (const auto& time : batch.times) {
    if (!time) {
      if (null_placement_ == NullPlacement::AtStart && last_time_) {
        return Status::Invalid("AsofJoin does not allow out-of-order on-key values");
      }
      if (null_placement_ == NullPlacement::AtEnd) {
        saw_trailing_null_ = true;
      }
      continue;
    }
    if ((null_placement_ == NullPlacement::AtEnd && saw_trailing_null_) ||
        (last_time_ && *time < *last_time_)) {
      return Status::Invalid("AsofJoin does not allow out-of-order on-key values");
    }
    last_time_ = *time;
  }
  return Status::OK();
}

FlowAction InputState::SetUpstreamPausedUnlocked(bool paused) {
  if (upstream_paused_ == paused || shutdown_) {
    return {};
  }
  upstream_paused_ = paused;
  return {paused ? FlowAction::Kind::Pause : FlowAction::Kind::Resume, input_, node_,
          ++outgoing_counter_};
}

FlowAction InputState::BatchBuffered() {
  std::lock_guard lock(flow_mutex_);
  if (shutdown_) {
    return {};
  }
  ++buffered_batches_;
  return buffered_batches_ >= kHighWatermark ? SetUpstreamPausedUnlocked(true)
                                             : FlowAction{};
}

FlowAction InputState::BatchConsumed() {
  std::lock_guard lock(flow_mutex_);
  if (shutdown_) {
    return {};
  }
  DCHECK_GT(buffered_batches_, 0);
  if (buffered_batches_ > 0) {
    --buffered_batches_;
  }
  return buffered_batches_ <= kLowWatermark ? SetUpstreamPausedUnlocked(false)
                                            : FlowAction{};
}

FlowAction InputState::Shutdown() {
  std::lock_guard lock(flow_mutex_);
  if (shutdown_) {
    return {};
  }
  shutdown_ = true;
  upstream_paused_ = false;
  buffered_batches_ = 0;
  // Always send a final, newer resume so a delayed pause cannot strand an input.
  return {FlowAction::Kind::Resume, input_, node_, ++outgoing_counter_};
}

bool RhsLane::StreamEndedUnlocked() const {
  return total_batches_.has_value() && received_batches_ == *total_batches_;
}

Result<std::optional<Task>> RhsLane::Enqueue(std::shared_ptr<PreparedBatch> batch) {
  bool claim = false;
  {
    std::lock_guard lock(mutex_);
    if (phase_ == Phase::Stopped) {
      return std::nullopt;
    }
    if (total_batches_ && received_batches_ >= *total_batches_) {
      return Status::Invalid("AsofJoin right input produced more batches than declared");
    }
    batches_.push_back(std::move(batch));
    ++received_batches_;
    if (phase_ == Phase::Waiting) {
      phase_ = Phase::Claimed;
      claim = true;
    }
  }
  if (!claim) {
    return std::nullopt;
  }
  return Task([this] { return Run(); });
}

Result<std::optional<Task>> RhsLane::SetTotal(int total_batches) {
  bool claim = false;
  {
    std::lock_guard lock(mutex_);
    if (phase_ == Phase::Stopped) {
      return std::nullopt;
    }
    if (total_batches_ && *total_batches_ != total_batches) {
      return Status::Invalid("AsofJoin right input changed its total batch count");
    }
    if (received_batches_ > total_batches) {
      return Status::Invalid("AsofJoin right input declared too few batches");
    }
    total_batches_ = total_batches;
    if (phase_ == Phase::Waiting && StreamEndedUnlocked()) {
      phase_ = Phase::Claimed;
      claim = true;
    }
  }
  if (!claim) {
    return std::nullopt;
  }
  return Task([this] { return Run(); });
}

Result<Task> RhsLane::Assign(std::shared_ptr<PreparedBatch> left) {
  std::lock_guard lock(mutex_);
  if (phase_ == Phase::Stopped) {
    return Task([] { return Status::OK(); });
  }
  if (phase_ != Phase::NoJob || job_) {
    return Status::Invalid("AsofJoin RHS lane was assigned overlapping left batches");
  }
  job_ = std::make_shared<Job>(std::move(left));
  phase_ = Phase::Claimed;
  return Task([this] { return Run(); });
}

void RhsLane::Stop() {
  std::lock_guard lock(mutex_);
  phase_ = Phase::Stopped;
}

Result<RhsLane::PeekResult> RhsLane::PeekNext() {
  for (;;) {
    if (node_->IsTerminal()) {
      return PeekResult{PeekResult::Kind::End, {}};
    }
    if (current_batch_) {
      if (current_row_ < current_batch_->batch.length) {
        return PeekResult{PeekResult::Kind::Row, RowRef{current_batch_, current_row_}};
      }
      current_batch_.reset();
      current_row_ = 0;
      node_->InputBatchConsumed(input_index_);
    }

    {
      std::lock_guard lock(mutex_);
      if (phase_ == Phase::Stopped) {
        return PeekResult{PeekResult::Kind::End, {}};
      }
      if (batches_.empty()) {
        return PeekResult{
            StreamEndedUnlocked() ? PeekResult::Kind::End : PeekResult::Kind::Blocked,
            {}};
      }
      current_batch_ = std::move(batches_.front());
      batches_.pop_front();
    }
  }
}

void RhsLane::ConsumeNext() {
  DCHECK(current_batch_);
  DCHECK_LT(current_row_, current_batch_->batch.length);
  if (++current_row_ == current_batch_->batch.length) {
    current_batch_.reset();
    current_row_ = 0;
    node_->InputBatchConsumed(input_index_);
  }
}

bool RhsLane::WaitOrRetry() {
  std::lock_guard lock(mutex_);
  if (phase_ == Phase::Stopped) {
    return false;
  }
  if (!batches_.empty() || StreamEndedUnlocked()) {
    return true;
  }
  phase_ = Phase::Waiting;
  return false;
}

void RhsLane::RememberBackward(const RowRef& row, OnType time) {
  const uint64_t hash = row.batch->Hash(row.row);
  const uint64_t version = ++next_version_;
  auto& candidates = backward_candidates_[hash];
  auto candidate =
      std::find_if(candidates.begin(), candidates.end(),
                   [&](const Candidate& value) { return KeysEqual(row, value.row); });
  if (candidate == candidates.end()) {
    candidates.push_back(Candidate{row, version});
  } else {
    *candidate = Candidate{row, version};
  }
  backward_expiry_.push_back(ExpiryEntry{time, hash, version});
}

void RhsLane::ExpireBackward(OnType lower_bound) {
  while (!backward_expiry_.empty() && backward_expiry_.front().time < lower_bound) {
    ExpiryEntry expired = std::move(backward_expiry_.front());
    backward_expiry_.pop_front();
    auto bucket = backward_candidates_.find(expired.hash);
    if (bucket == backward_candidates_.end()) {
      continue;
    }
    auto& candidates = bucket->second;
    auto candidate = std::find_if(
        candidates.begin(), candidates.end(),
        [&](const Candidate& value) { return value.version == expired.version; });
    if (candidate != candidates.end()) {
      candidates.erase(candidate);
    }
    if (candidates.empty()) {
      backward_candidates_.erase(bucket);
    }
  }
}

void RhsLane::RememberOrdered(const RowRef& row, OnType time) {
  const uint64_t hash = row.batch->Hash(row.row);
  const uint64_t version = ++next_version_;
  auto& bucket = ordered_candidates_[hash];
  auto candidates =
      std::find_if(bucket.begin(), bucket.end(), [&](const OrderedCandidates& value) {
        return !value.rows.empty() && KeysEqual(row, value.rows.front().row);
      });
  if (candidates == bucket.end()) {
    bucket.emplace_back();
    candidates = std::prev(bucket.end());
  }
  candidates->rows.push_back(OrderedCandidate{row, time, version});
  ordered_expiry_.push_back(ExpiryEntry{time, hash, version});
}

void RhsLane::ExpireOrdered(OnType lower_bound) {
  while (!ordered_expiry_.empty() && ordered_expiry_.front().time < lower_bound) {
    ExpiryEntry expired = std::move(ordered_expiry_.front());
    ordered_expiry_.pop_front();
    auto bucket = ordered_candidates_.find(expired.hash);
    if (bucket == ordered_candidates_.end()) {
      continue;
    }
    auto& states = bucket->second;
    auto candidates =
        std::find_if(states.begin(), states.end(), [&](const OrderedCandidates& value) {
          return !value.rows.empty() && value.rows.front().version == expired.version;
        });
    if (candidates != states.end()) {
      candidates->rows.pop_front();
      if (candidates->rows.empty()) {
        states.erase(candidates);
      }
    }
    if (states.empty()) {
      ordered_candidates_.erase(bucket);
    }
  }
}

std::optional<RowRef> RhsLane::MatchBackward(const RowRef& key) const {
  auto bucket = backward_candidates_.find(key.batch->Hash(key.row));
  if (bucket == backward_candidates_.end()) {
    return std::nullopt;
  }
  auto candidate =
      std::find_if(bucket->second.begin(), bucket->second.end(),
                   [&](const Candidate& value) { return KeysEqual(key, value.row); });
  return candidate == bucket->second.end() ? std::nullopt
                                           : std::optional<RowRef>(candidate->row);
}

std::optional<RowRef> RhsLane::MatchOrdered(const RowRef& key, OnType left_time) const {
  auto bucket = ordered_candidates_.find(key.batch->Hash(key.row));
  if (bucket == ordered_candidates_.end()) {
    return std::nullopt;
  }
  auto candidates = std::find_if(
      bucket->second.begin(), bucket->second.end(), [&](const OrderedCandidates& value) {
        return !value.rows.empty() && KeysEqual(key, value.rows.front().row);
      });
  if (candidates == bucket->second.end()) {
    return std::nullopt;
  }

  const auto& rows = candidates->rows;
  auto later = std::lower_bound(rows.begin(), rows.end(), left_time,
                                [](const OrderedCandidate& candidate, OnType time) {
                                  return candidate.time < time;
                                });
  if (later == rows.begin()) {
    return later->row;
  }
  if (later == rows.end()) {
    return rows.back().row;
  }
  if (later->time == left_time) {
    return later->row;
  }

  const auto earlier = std::prev(later);
  const OnType earlier_distance = left_time - earlier->time;
  const OnType later_distance = later->time - left_time;
  if (earlier_distance < later_distance ||
      (earlier_distance == later_distance && tolerance_.prefer_earlier_on_tie())) {
    return earlier->row;
  }
  return later->row;
}

Status RhsLane::Run() {
  std::shared_ptr<Job> job;
  {
    std::lock_guard lock(mutex_);
    if (phase_ != Phase::Claimed || !job_) {
      return Status::OK();
    }
    job = job_;
  }

  while (job->left_row < job->left->batch.length) {
    if (node_->IsTerminal()) {
      return Status::OK();
    }
    const int64_t left_row = job->left_row;
    const auto& left_time = job->left->times[left_row];
    if (!left_time) {
      job->AppendMatch(std::nullopt);
      ++job->left_row;
      continue;
    }

    const auto bounds = tolerance_.BoundsFor(*left_time);
    if (!bounds) {
      job->AppendMatch(std::nullopt);
      ++job->left_row;
      continue;
    }

    const RowRef key{job->left, left_row};
    if (tolerance_.mode() == CandidateMode::Latest) {
      ExpireBackward(bounds->lower);
    } else {
      ExpireOrdered(bounds->lower);
    }

    for (;;) {
      ARROW_ASSIGN_OR_RAISE(PeekResult next, PeekNext());
      if (next.kind != PeekResult::Kind::Row) {
        if (next.kind == PeekResult::Kind::Blocked) {
          if (WaitOrRetry()) {
            continue;
          }
          return Status::OK();
        }
        break;
      }

      const auto& right_time = next.row.batch->times[next.row.row];
      if (!right_time) {
        ConsumeNext();
        continue;
      }
      if (*right_time > bounds->upper) {
        break;
      }

      if (*right_time >= bounds->lower) {
        if (tolerance_.mode() == CandidateMode::Latest) {
          RememberBackward(next.row, *right_time);
        } else {
          RememberOrdered(next.row, *right_time);
        }
      }
      ConsumeNext();
    }

    job->AppendMatch(tolerance_.mode() == CandidateMode::Latest
                         ? MatchBackward(key)
                         : MatchOrdered(key, *left_time));
    ++job->left_row;
  }

  ARROW_ASSIGN_OR_RAISE(auto values, Materialize(*job));
  {
    std::lock_guard lock(mutex_);
    if (phase_ == Phase::Stopped) {
      return Status::OK();
    }
    if (job_ != job || phase_ != Phase::Claimed) {
      return Status::Invalid("AsofJoin RHS lane lost ownership of its job");
    }
    job_.reset();
    phase_ = Phase::NoJob;
  }
  return node_->LaneCompleted(lane_index_, std::move(values));
}

Result<std::vector<Datum>> RhsLane::Materialize(const Job& job) const {
  std::vector<Datum> values;
  values.reserve(payload_columns_.size());
  if (payload_columns_.empty()) {
    return values;
  }

  const auto& input_schema = node_->inputs()[input_index_]->output_schema();
  for (col_index_t column : payload_columns_) {
    ARROW_ASSIGN_OR_RAISE(auto builder,
                          MakeBuilder(input_schema->field(column)->type(), pool_));
    ARROW_RETURN_NOT_OK(builder->Reserve(job.left->batch.length));
    const ArrayData* current_source = nullptr;
    std::optional<ArraySpan> current_span;
    for (const SelectionRun& selection : job.selections) {
      if (!selection.source) {
        ARROW_RETURN_NOT_OK(builder->AppendNulls(selection.length));
        continue;
      }

      const RowRef& match = *selection.source;
      const Datum& source = match.batch->batch.values[column];
      if (source.is_scalar()) {
        ARROW_RETURN_NOT_OK(builder->AppendScalar(*source.scalar(), selection.length));
        continue;
      }
      if (!source.is_array()) {
        return Status::Invalid(
            "AsofJoin RHS payload must be an array or scalar, but got ",
            ::arrow::ToString(source.kind()));
      }

      if (source.array().get() != current_source) {
        current_source = source.array().get();
        current_span.emplace(*source.array());
      }
      Status status =
          builder->AppendArraySlice(*current_span, match.row, selection.length);
      if (status.IsNotImplemented()) {
        auto source_array = MakeArray(source.array());
        for (int64_t row = match.row; row < match.row + selection.length; ++row) {
          ARROW_ASSIGN_OR_RAISE(auto scalar, source_array->GetScalar(row));
          ARROW_RETURN_NOT_OK(builder->AppendScalar(*scalar));
        }
      } else {
        ARROW_RETURN_NOT_OK(status);
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto array, builder->Finish());
    values.emplace_back(std::move(array));
  }
  return values;
}

}  // namespace

namespace internal {
void RegisterAsofJoinNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("asofjoin", AsofJoinNode::Make));
}
}  // namespace internal

namespace asofjoin {

Result<std::shared_ptr<Schema>> MakeOutputSchema(
    const std::vector<std::shared_ptr<Schema>>& input_schema,
    const std::vector<AsofJoinKeys>& input_keys) {
  ARROW_ASSIGN_OR_RAISE(auto on_keys,
                        AsofJoinNode::GetIndicesOfOnKey(input_schema, input_keys));
  ARROW_ASSIGN_OR_RAISE(auto by_keys,
                        AsofJoinNode::GetIndicesOfByKey(input_schema, input_keys));
  return AsofJoinNode::MakeOutputSchema(input_schema, on_keys, by_keys);
}

}  // namespace asofjoin
}  // namespace acero
}  // namespace arrow
