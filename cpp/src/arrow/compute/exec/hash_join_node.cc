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

// Check if a type is supported in a join (as either a key or non-key column)
bool HashJoinSchema::IsTypeSupported(const DataType& type) {
  const Type::type id = type.id();
  if (id == Type::DICTIONARY) {
    return IsTypeSupported(*checked_cast<const DictionaryType&>(type).value_type());
  }
  return is_fixed_width(id) || is_binary_like(id) || is_large_binary_like(id);
}

Result<std::vector<FieldRef>> HashJoinSchema::ComputePayload(
    const Schema& schema, const std::vector<FieldRef>& output,
    const std::vector<FieldRef>& filter, const std::vector<FieldRef>& keys) {
  // payload = (output + filter) - keys, with no duplicates
  std::unordered_set<int> payload_fields;
  for (auto ref : output) {
    ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOne(schema));
    payload_fields.insert(match[0]);
  }

  for (auto ref : filter) {
    ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOne(schema));
    payload_fields.insert(match[0]);
  }

  for (auto ref : keys) {
    ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOne(schema));
    payload_fields.erase(match[0]);
  }

  std::vector<FieldRef> payload_refs;
  for (auto ref : output) {
    ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOne(schema));
    if (payload_fields.find(match[0]) != payload_fields.end()) {
      payload_refs.push_back(ref);
      payload_fields.erase(match[0]);
    }
  }
  for (auto ref : filter) {
    ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOne(schema));
    if (payload_fields.find(match[0]) != payload_fields.end()) {
      payload_refs.push_back(ref);
      payload_fields.erase(match[0]);
    }
  }
  return payload_refs;
}

Status HashJoinSchema::Init(JoinType join_type, const Schema& left_schema,
                            const std::vector<FieldRef>& left_keys,
                            const Schema& right_schema,
                            const std::vector<FieldRef>& right_keys,
                            const Expression& filter,
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
              right_output, filter, left_field_name_prefix, right_field_name_prefix);
}

Status HashJoinSchema::Init(
    JoinType join_type, const Schema& left_schema, const std::vector<FieldRef>& left_keys,
    const std::vector<FieldRef>& left_output, const Schema& right_schema,
    const std::vector<FieldRef>& right_keys, const std::vector<FieldRef>& right_output,
    const Expression& filter, const std::string& left_field_name_prefix,
    const std::string& right_field_name_prefix) {
  RETURN_NOT_OK(ValidateSchemas(join_type, left_schema, left_keys, left_output,
                                right_schema, right_keys, right_output,
                                left_field_name_prefix, right_field_name_prefix));

  std::vector<HashJoinProjection> handles;
  std::vector<const std::vector<FieldRef>*> field_refs;

  std::vector<FieldRef> left_filter, right_filter;
  RETURN_NOT_OK(
      CollectFilterColumns(left_filter, right_filter, filter, left_schema, right_schema));

  handles.push_back(HashJoinProjection::KEY);
  field_refs.push_back(&left_keys);

  ARROW_ASSIGN_OR_RAISE(auto left_payload,
                        ComputePayload(left_schema, left_output, left_filter, left_keys));
  handles.push_back(HashJoinProjection::PAYLOAD);
  field_refs.push_back(&left_payload);

  handles.push_back(HashJoinProjection::FILTER);
  field_refs.push_back(&left_filter);

  handles.push_back(HashJoinProjection::OUTPUT);
  field_refs.push_back(&left_output);

  RETURN_NOT_OK(
      proj_maps[0].Init(HashJoinProjection::INPUT, left_schema, handles, field_refs));

  handles.clear();
  field_refs.clear();

  handles.push_back(HashJoinProjection::KEY);
  field_refs.push_back(&right_keys);

  ARROW_ASSIGN_OR_RAISE(auto right_payload, ComputePayload(right_schema, right_output,
                                                           right_filter, right_keys));
  handles.push_back(HashJoinProjection::PAYLOAD);
  field_refs.push_back(&right_payload);

  handles.push_back(HashJoinProjection::FILTER);
  field_refs.push_back(&right_filter);

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
  // 5. Some data types may not be allowed in a key field or non-key field
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
    if (!IsTypeSupported(*type)) {
      return Status::Invalid("Data type ", *type, " is not supported in join key field");
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
  for (const auto& field : left_schema.fields()) {
    const auto& type = *field->type();
    if (!IsTypeSupported(type)) {
      return Status::Invalid("Data type ", type,
                             " is not supported in join non-key field");
    }
  }
  for (const auto& field : right_schema.fields()) {
    const auto& type = *field->type();
    if (!IsTypeSupported(type)) {
      return Status::Invalid("Data type ", type,
                             " is not supported in join non-key field");
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

Result<Expression> HashJoinSchema::BindFilter(Expression filter,
                                              const Schema& left_schema,
                                              const Schema& right_schema) {
  if (filter.IsBound() || filter == literal(true)) {
    return std::move(filter);
  }
  // Step 1: Construct filter schema
  FieldVector fields;
  auto left_f_to_i =
      proj_maps[0].map(HashJoinProjection::FILTER, HashJoinProjection::INPUT);
  auto right_f_to_i =
      proj_maps[1].map(HashJoinProjection::FILTER, HashJoinProjection::INPUT);

  auto AppendFieldsInMap = [&fields](const SchemaProjectionMap& map,
                                     const Schema& schema) {
    for (int i = 0; i < map.num_cols; i++) {
      int input_idx = map.get(i);
      fields.push_back(schema.fields()[input_idx]);
    }
  };
  AppendFieldsInMap(left_f_to_i, left_schema);
  AppendFieldsInMap(right_f_to_i, right_schema);
  Schema filter_schema(fields);

  // Step 2: Rewrite expression to use filter schema
  auto left_i_to_f =
      proj_maps[0].map(HashJoinProjection::INPUT, HashJoinProjection::FILTER);
  auto right_i_to_f =
      proj_maps[1].map(HashJoinProjection::INPUT, HashJoinProjection::FILTER);
  filter = RewriteFilterToUseFilterSchema(left_f_to_i.num_cols, left_i_to_f, right_i_to_f,
                                          filter);

  // Step 3: Bind
  ARROW_ASSIGN_OR_RAISE(filter, filter.Bind(filter_schema));
  if (filter.type()->id() != Type::BOOL) {
    return Status::TypeError("Filter expression must evaluate to bool, but ",
                             filter.ToString(), " evaluates to ",
                             filter.type()->ToString());
  }
  return std::move(filter);
}

Expression HashJoinSchema::RewriteFilterToUseFilterSchema(
    const int right_filter_offset, const SchemaProjectionMap& left_to_filter,
    const SchemaProjectionMap& right_to_filter, const Expression& filter) {
  if (const Expression::Call* c = filter.call()) {
    std::vector<Expression> args = c->arguments;
    for (size_t i = 0; i < args.size(); i++)
      args[i] = RewriteFilterToUseFilterSchema(right_filter_offset, left_to_filter,
                                               right_to_filter, args[i]);
    return call(c->function_name, args, c->options);
  } else if (const FieldRef* r = filter.field_ref()) {
    if (const FieldPath* path = r->field_path()) {
      auto indices = path->indices();
      if (indices[0] >= left_to_filter.num_cols) {
        indices[0] -= left_to_filter.num_cols;  // Convert to index into right schema
        indices[0] =
            right_to_filter.get(indices[0]) +
            right_filter_offset;  // Convert right schema index to filter schema index
      } else {
        indices[0] = left_to_filter.get(
            indices[0]);  // Convert left schema index to filter schema index
      }
      return field_ref({std::move(indices)});
    }
  }
  return filter;
}

Status HashJoinSchema::CollectFilterColumns(std::vector<FieldRef>& left_filter,
                                            std::vector<FieldRef>& right_filter,
                                            const Expression& filter,
                                            const Schema& left_schema,
                                            const Schema& right_schema) {
  std::vector<FieldRef> nonunique_refs = FieldsInExpression(filter);

  std::unordered_set<FieldPath, FieldPath::Hash> left_seen_paths;
  std::unordered_set<FieldPath, FieldPath::Hash> right_seen_paths;
  for (const FieldRef& ref : nonunique_refs) {
    if (const FieldPath* path = ref.field_path()) {
      std::vector<int> indices = path->indices();
      if (indices[0] >= left_schema.num_fields()) {
        indices[0] -= left_schema.num_fields();
        FieldPath corrected_path(std::move(indices));
        if (right_seen_paths.find(*path) == right_seen_paths.end()) {
          right_filter.push_back(corrected_path);
          right_seen_paths.emplace(std::move(corrected_path));
        }
      } else if (left_seen_paths.find(*path) == left_seen_paths.end()) {
        left_filter.push_back(ref);
        left_seen_paths.emplace(std::move(indices));
      }
    } else {
      ARROW_DCHECK(ref.IsName());
      ARROW_ASSIGN_OR_RAISE(auto left_match, ref.FindOneOrNone(left_schema));
      ARROW_ASSIGN_OR_RAISE(auto right_match, ref.FindOneOrNone(right_schema));
      bool in_left = !left_match.empty();
      bool in_right = !right_match.empty();
      if (in_left && in_right) {
        return Status::Invalid("FieldRef", ref.ToString(),
                               "was found in both left and right schemas");
      } else if (!in_left && !in_right) {
        return Status::Invalid("FieldRef", ref.ToString(),
                               "was not found in either left or right schema");
      }

      ARROW_DCHECK(in_left != in_right);
      auto& target_array = in_left ? left_filter : right_filter;
      auto& target_set = in_left ? left_seen_paths : right_seen_paths;
      auto& target_match = in_left ? left_match : right_match;

      if (target_set.find(target_match) == target_set.end()) {
        target_array.push_back(ref);
        target_set.emplace(std::move(target_match));
      }
    }
  }
  return Status::OK();
}

class HashJoinNode : public ExecNode {
 public:
  HashJoinNode(ExecPlan* plan, NodeVector inputs, const HashJoinNodeOptions& join_options,
               std::shared_ptr<Schema> output_schema,
               std::unique_ptr<HashJoinSchema> schema_mgr, Expression filter,
               std::unique_ptr<HashJoinImpl> impl)
      : ExecNode(plan, inputs, {"left", "right"},
                 /*output_schema=*/std::move(output_schema),
                 /*num_outputs=*/1),
        join_type_(join_options.join_type),
        key_cmp_(join_options.key_cmp),
        filter_(std::move(filter)),
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

    const auto& left_schema = *(inputs[0]->output_schema());
    const auto& right_schema = *(inputs[1]->output_schema());
    // This will also validate input schemas
    if (join_options.output_all) {
      RETURN_NOT_OK(schema_mgr->Init(
          join_options.join_type, left_schema, join_options.left_keys, right_schema,
          join_options.right_keys, join_options.filter,
          join_options.output_prefix_for_left, join_options.output_prefix_for_right));
    } else {
      RETURN_NOT_OK(schema_mgr->Init(
          join_options.join_type, left_schema, join_options.left_keys,
          join_options.left_output, right_schema, join_options.right_keys,
          join_options.right_output, join_options.filter,
          join_options.output_prefix_for_left, join_options.output_prefix_for_right));
    }

    ARROW_ASSIGN_OR_RAISE(
        Expression filter,
        schema_mgr->BindFilter(join_options.filter, left_schema, right_schema));

    // Generate output schema
    std::shared_ptr<Schema> output_schema = schema_mgr->MakeOutputSchema(
        join_options.output_prefix_for_left, join_options.output_prefix_for_right);

    // Create hash join implementation object
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<HashJoinImpl> impl, HashJoinImpl::MakeBasic());

    return plan->EmplaceNode<HashJoinNode>(
        plan, inputs, join_options, std::move(output_schema), std::move(schema_mgr),
        std::move(filter), std::move(impl));
  }

  const char* kind_name() const override { return "HashJoinNode"; }

  void InputReceived(ExecNode* input, ExecBatch batch) override {
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());
    if (complete_.load()) {
      return;
    }

    size_t thread_index = thread_indexer_();
    int side = (input == inputs_[0]) ? 0 : 1;

    EVENT(span_, "InputReceived", {{"batch.length", batch.length}, {"side", side}});
    util::tracing::Span span;
    START_SPAN_WITH_PARENT(span, span_, "InputReceived",
                           {{"batch.length", batch.length}});

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
    EVENT(span_, "ErrorReceived", {{"error", error.message()}});
    DCHECK_EQ(input, inputs_[0]);
    StopProducing();
    outputs_[0]->ErrorReceived(this, std::move(error));
  }

  void InputFinished(ExecNode* input, int total_batches) override {
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());

    size_t thread_index = thread_indexer_();
    int side = (input == inputs_[0]) ? 0 : 1;

    EVENT(span_, "InputFinished", {{"side", side}, {"batches.length", total_batches}});

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
    START_SPAN(span_, std::string(kind_name()) + ":" + label(),
               {{"node.label", label()},
                {"node.detail", ToString()},
                {"node.kind", kind_name()}});
    finished_ = Future<>::Make();
    END_SPAN_ON_FUTURE_COMPLETION(span_, finished_, this);

    bool use_sync_execution = !(plan_->exec_context()->executor());
    size_t num_threads = use_sync_execution ? 1 : thread_indexer_.Capacity();

    RETURN_NOT_OK(impl_->Init(
        plan_->exec_context(), join_type_, use_sync_execution, num_threads,
        schema_mgr_.get(), key_cmp_, filter_,
        [this](ExecBatch batch) { this->OutputBatchCallback(batch); },
        [this](int64_t total_num_batches) { this->FinishedCallback(total_num_batches); },
        [this](std::function<Status(size_t)> func) -> Status {
          return this->ScheduleTaskCallback(std::move(func));
        }));
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override { EVENT(span_, "PauseProducing"); }

  void ResumeProducing(ExecNode* output) override { EVENT(span_, "ResumeProducing"); }

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override {
    EVENT(span_, "StopProducing");
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
  JoinType join_type_;
  std::vector<JoinKeyCmp> key_cmp_;
  Expression filter_;
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
