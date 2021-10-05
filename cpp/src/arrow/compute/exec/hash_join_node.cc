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

#include <unordered_set>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/hash_join.h"
#include "arrow/compute/exec/hash_join_dict.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

Result<std::vector<FieldRef>> HashJoinSchema::VectorDiff(const Schema& schema,
                                                         const std::vector<FieldRef>& a,
                                                         const std::vector<FieldRef>& b) {
  std::unordered_set<int> b_paths;
  for (size_t i = 0; i < b.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, b[i].FindOne(schema));
    b_paths.insert(match[0]);
  }

  std::vector<FieldRef> result;

  for (size_t i = 0; i < a.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, a[i].FindOne(schema));
    bool is_found = (b_paths.find(match[0]) != b_paths.end());
    if (!is_found) {
      result.push_back(a[i]);
    }
  }

  return result;
}

Status HashJoinSchema::Init(JoinType join_type, const Schema& left_schema,
                            const std::vector<FieldRef>& left_keys,
                            const Schema& right_schema,
                            const std::vector<FieldRef>& right_keys,
                            const std::string& left_field_name_prefix,
                            const std::string& right_field_name_prefix) {
  std::vector<FieldRef> left_output;
  if (join_type != JoinType::RIGHT_SEMI && join_type != JoinType::RIGHT_ANTI) {
    const FieldVector& left_fields = left_schema.fields();
    left_output.resize(left_fields.size());
    for (size_t i = 0; i < left_fields.size(); ++i) {
      left_output[i] = FieldRef(static_cast<int>(i));
    }
  }
  // Repeat the same for the right side
  std::vector<FieldRef> right_output;
  if (join_type != JoinType::LEFT_SEMI && join_type != JoinType::LEFT_ANTI) {
    const FieldVector& right_fields = right_schema.fields();
    right_output.resize(right_fields.size());
    for (size_t i = 0; i < right_fields.size(); ++i) {
      right_output[i] = FieldRef(static_cast<int>(i));
    }
  }
  return Init(join_type, left_schema, left_keys, left_output, right_schema, right_keys,
              right_output, left_field_name_prefix, right_field_name_prefix);
}

Status HashJoinSchema::Init(JoinType join_type, const Schema& left_schema,
                            const std::vector<FieldRef>& left_keys,
                            const std::vector<FieldRef>& left_output,
                            const Schema& right_schema,
                            const std::vector<FieldRef>& right_keys,
                            const std::vector<FieldRef>& right_output,
                            const std::string& left_field_name_prefix,
                            const std::string& right_field_name_prefix) {
  RETURN_NOT_OK(ValidateSchemas(join_type, left_schema, left_keys, left_output,
                                right_schema, right_keys, right_output,
                                left_field_name_prefix, right_field_name_prefix));

  std::vector<HashJoinProjection> handles;
  std::vector<const std::vector<FieldRef>*> field_refs;

  handles.push_back(HashJoinProjection::KEY);
  field_refs.push_back(&left_keys);
  ARROW_ASSIGN_OR_RAISE(auto left_payload,
                        VectorDiff(left_schema, left_output, left_keys));
  handles.push_back(HashJoinProjection::PAYLOAD);
  field_refs.push_back(&left_payload);
  handles.push_back(HashJoinProjection::OUTPUT);
  field_refs.push_back(&left_output);

  RETURN_NOT_OK(
      proj_maps[0].Init(HashJoinProjection::INPUT, left_schema, handles, field_refs));

  handles.clear();
  field_refs.clear();

  handles.push_back(HashJoinProjection::KEY);
  field_refs.push_back(&right_keys);
  ARROW_ASSIGN_OR_RAISE(auto right_payload,
                        VectorDiff(right_schema, right_output, right_keys));
  handles.push_back(HashJoinProjection::PAYLOAD);
  field_refs.push_back(&right_payload);
  handles.push_back(HashJoinProjection::OUTPUT);
  field_refs.push_back(&right_output);

  RETURN_NOT_OK(
      proj_maps[1].Init(HashJoinProjection::INPUT, right_schema, handles, field_refs));

  return Status::OK();
}

Status HashJoinSchema::ValidateSchemas(JoinType join_type, const Schema& left_schema,
                                       const std::vector<FieldRef>& left_keys,
                                       const std::vector<FieldRef>& left_output,
                                       const Schema& right_schema,
                                       const std::vector<FieldRef>& right_keys,
                                       const std::vector<FieldRef>& right_output,
                                       const std::string& left_field_name_prefix,
                                       const std::string& right_field_name_prefix) {
  // Checks for key fields:
  // 1. Key field refs must match exactly one input field
  // 2. Same number of key fields on left and right
  // 3. At least one key field
  // 4. Equal data types for corresponding key fields
  // 5. Dictionary type is not supported in a key field
  // 6. Some other data types may not be allowed in a key field
  //
  if (left_keys.size() != right_keys.size()) {
    return Status::Invalid("Different number of key fields on left (", left_keys.size(),
                           ") and right (", right_keys.size(), ") side of the join");
  }
  if (left_keys.size() < 1) {
    return Status::Invalid("Join key cannot be empty");
  }
  for (size_t i = 0; i < left_keys.size() + right_keys.size(); ++i) {
    bool left_side = i < left_keys.size();
    const FieldRef& field_ref =
        left_side ? left_keys[i] : right_keys[i - left_keys.size()];
    Result<FieldPath> result = field_ref.FindOne(left_side ? left_schema : right_schema);
    if (!result.ok()) {
      return Status::Invalid("No match or multiple matches for key field reference ",
                             field_ref.ToString(), left_side ? " on left " : " on right ",
                             "side of the join");
    }
    const FieldPath& match = result.ValueUnsafe();
    const std::shared_ptr<DataType>& type =
        (left_side ? left_schema.fields() : right_schema.fields())[match[0]]->type();
    if ((type->id() != Type::BOOL && !is_fixed_width(type->id()) &&
         !is_binary_like(type->id())) ||
        is_large_binary_like(type->id())) {
      return Status::Invalid("Data type ", type->ToString(),
                             " is not supported in join key field");
    }
  }
  for (size_t i = 0; i < left_keys.size(); ++i) {
    const FieldRef& left_ref = left_keys[i];
    const FieldRef& right_ref = right_keys[i];
    int left_id = left_ref.FindOne(left_schema).ValueUnsafe()[0];
    int right_id = right_ref.FindOne(right_schema).ValueUnsafe()[0];
    const std::shared_ptr<DataType>& left_type = left_schema.fields()[left_id]->type();
    const std::shared_ptr<DataType>& right_type = right_schema.fields()[right_id]->type();
    if (!HashJoinDictUtil::KeyDataTypesValid(left_type, right_type)) {
      return Status::Invalid(
          "Incompatible data types for corresponding join field keys: ",
          left_ref.ToString(), " of type ", left_type->ToString(), " and ",
          right_ref.ToString(), " of type ", right_type->ToString());
    }
  }

  // Check for output fields:
  // 1. Output field refs must match exactly one input field
  // 2. At least one output field
  // 3. Dictionary type is not supported in an output field
  // 4. Left semi/anti join (right semi/anti join) must not output fields from right
  // (left)
  // 5. No name collisions in output fields after adding (potentially empty)
  // prefixes to left and right output
  //
  if (left_output.empty() && right_output.empty()) {
    return Status::Invalid("Join must output at least one field");
  }
  if (join_type == JoinType::LEFT_SEMI || join_type == JoinType::LEFT_ANTI) {
    if (!right_output.empty()) {
      return Status::Invalid(
          join_type == JoinType::LEFT_SEMI ? "Left semi join " : "Left anti-semi join ",
          "may not output fields from right side");
    }
  }
  if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
    if (!left_output.empty()) {
      return Status::Invalid(join_type == JoinType::RIGHT_SEMI ? "Right semi join "
                                                               : "Right anti-semi join ",
                             "may not output fields from left side");
    }
  }
  for (size_t i = 0; i < left_output.size() + right_output.size(); ++i) {
    bool left_side = i < left_output.size();
    const FieldRef& field_ref =
        left_side ? left_output[i] : right_output[i - left_output.size()];
    Result<FieldPath> result = field_ref.FindOne(left_side ? left_schema : right_schema);
    if (!result.ok()) {
      return Status::Invalid("No match or multiple matches for output field reference ",
                             field_ref.ToString(), left_side ? " on left " : " on right ",
                             "side of the join");
    }
  }
  return Status::OK();
}

std::shared_ptr<Schema> HashJoinSchema::MakeOutputSchema(
    const std::string& left_field_name_prefix,
    const std::string& right_field_name_prefix) {
  std::vector<std::shared_ptr<Field>> fields;
  int left_size = proj_maps[0].num_cols(HashJoinProjection::OUTPUT);
  int right_size = proj_maps[1].num_cols(HashJoinProjection::OUTPUT);
  fields.resize(left_size + right_size);

  for (int i = 0; i < left_size + right_size; ++i) {
    bool is_left = (i < left_size);
    int side = (is_left ? 0 : 1);
    int input_field_id = proj_maps[side]
                             .map(HashJoinProjection::OUTPUT, HashJoinProjection::INPUT)
                             .get(is_left ? i : i - left_size);
    const std::string& input_field_name =
        proj_maps[side].field_name(HashJoinProjection::INPUT, input_field_id);
    const std::shared_ptr<DataType>& input_data_type =
        proj_maps[side].data_type(HashJoinProjection::INPUT, input_field_id);

    std::string output_field_name =
        (is_left ? left_field_name_prefix : right_field_name_prefix) + input_field_name;

    // All fields coming out of join are marked as nullable.
    fields[i] =
        std::make_shared<Field>(output_field_name, input_data_type, true /*nullable*/);
  }
  return std::make_shared<Schema>(std::move(fields));
}

class HashJoinNode : public ExecNode {
 public:
  HashJoinNode(ExecPlan* plan, NodeVector inputs, const HashJoinNodeOptions& join_options,
               std::shared_ptr<Schema> output_schema,
               std::unique_ptr<HashJoinSchema> schema_mgr,
               std::unique_ptr<HashJoinImpl> impl)
      : ExecNode(plan, inputs, {"left", "right"},
                 /*output_schema=*/std::move(output_schema),
                 /*num_outputs=*/1),
        join_type_(join_options.join_type),
        key_cmp_(join_options.key_cmp),
        schema_mgr_(std::move(schema_mgr)),
        impl_(std::move(impl)) {
    complete_.store(false);
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    // Number of input exec nodes must be 2
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 2, "HashJoinNode"));

    std::unique_ptr<HashJoinSchema> schema_mgr =
        ::arrow::internal::make_unique<HashJoinSchema>();

    const auto& join_options = checked_cast<const HashJoinNodeOptions&>(options);

    // This will also validate input schemas
    if (join_options.output_all) {
      RETURN_NOT_OK(schema_mgr->Init(
          join_options.join_type, *(inputs[0]->output_schema()), join_options.left_keys,
          *(inputs[1]->output_schema()), join_options.right_keys,
          join_options.output_prefix_for_left, join_options.output_prefix_for_right));
    } else {
      RETURN_NOT_OK(schema_mgr->Init(
          join_options.join_type, *(inputs[0]->output_schema()), join_options.left_keys,
          join_options.left_output, *(inputs[1]->output_schema()),
          join_options.right_keys, join_options.right_output,
          join_options.output_prefix_for_left, join_options.output_prefix_for_right));
    }

    // Generate output schema
    std::shared_ptr<Schema> output_schema = schema_mgr->MakeOutputSchema(
        join_options.output_prefix_for_left, join_options.output_prefix_for_right);

    // Create hash join implementation object
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<HashJoinImpl> impl, HashJoinImpl::MakeBasic());

    return plan->EmplaceNode<HashJoinNode>(plan, inputs, join_options,
                                           std::move(output_schema),
                                           std::move(schema_mgr), std::move(impl));
  }

  const char* kind_name() const override { return "HashJoinNode"; }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());

    if (complete_.load()) {
      return;
    }

    size_t thread_index = thread_indexer_();
    int side = (input == inputs_[0]) ? 0 : 1;
    {
      Status status = impl_->InputReceived(thread_index, side, std::move(batch));
      if (!status.ok()) {
        StopProducing();
        ErrorIfNotOk(status);
        return;
      }
    }
    if (batch_count_[side].Increment()) {
      Status status = impl_->InputFinished(thread_index, side);
      if (!status.ok()) {
        StopProducing();
        ErrorIfNotOk(status);
        return;
      }
    }
  }

  void ErrorReceived(ExecNode* input, Status error) override {
    DCHECK_EQ(input, inputs_[0]);
    StopProducing();
    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int total_batches) override {
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());

    size_t thread_index = thread_indexer_();
    int side = (input == inputs_[0]) ? 0 : 1;

    if (batch_count_[side].SetTotal(total_batches)) {
      Status status = impl_->InputFinished(thread_index, side);
      if (!status.ok()) {
        StopProducing();
        ErrorIfNotOk(status);
        return;
      }
    }
  }

  Status StartProducing() override {
    finished_ = Future<>::Make();

    bool use_sync_execution = !(plan_->exec_context()->executor());
    size_t num_threads = use_sync_execution ? 1 : thread_indexer_.Capacity();

    RETURN_NOT_OK(impl_->Init(
        plan_->exec_context(), join_type_, use_sync_execution, num_threads,
        schema_mgr_.get(), key_cmp_,
        [this](ExecBatch batch) { this->OutputBatchCallback(batch); },
        [this](int64_t total_num_batches) { this->FinishedCallback(total_num_batches); },
        [this](std::function<Status(size_t)> func) -> Status {
          return this->ScheduleTaskCallback(std::move(func));
        }));
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override {
    bool expected = false;
    if (complete_.compare_exchange_strong(expected, true)) {
      for (auto&& input : inputs_) {
        input->StopProducing(this);
      }
      impl_->Abort([this]() { finished_.MarkFinished(); });
    }
  }

  Future<> finished() override { return finished_; }

 private:
  void OutputBatchCallback(ExecBatch batch) {
    outputs_[0]->InputReceived(this, std::move(batch));
  }

  void FinishedCallback(int64_t total_num_batches) {
    bool expected = false;
    if (complete_.compare_exchange_strong(expected, true)) {
      outputs_[0]->InputFinished(this, static_cast<int>(total_num_batches));
      finished_.MarkFinished();
    }
  }

  Status ScheduleTaskCallback(std::function<Status(size_t)> func) {
    auto executor = plan_->exec_context()->executor();
    if (executor) {
      RETURN_NOT_OK(executor->Spawn([this, func] {
        size_t thread_index = thread_indexer_();
        Status status = func(thread_index);
        if (!status.ok()) {
          StopProducing();
          ErrorIfNotOk(status);
          return;
        }
      }));
    } else {
      // We should not get here in serial execution mode
      ARROW_DCHECK(false);
    }
    return Status::OK();
  }

 private:
  AtomicCounter batch_count_[2];
  std::atomic<bool> complete_;
  Future<> finished_ = Future<>::MakeFinished();
  JoinType join_type_;
  std::vector<JoinKeyCmp> key_cmp_;
  ThreadIndexer thread_indexer_;
  std::unique_ptr<HashJoinSchema> schema_mgr_;
  std::unique_ptr<HashJoinImpl> impl_;
};

namespace internal {

void RegisterHashJoinNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("hashjoin", HashJoinNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
