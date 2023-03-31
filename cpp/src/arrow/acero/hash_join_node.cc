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

#include <memory>
#include <mutex>
#include <unordered_set>
#include <utility>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/hash_join.h"
#include "arrow/acero/hash_join_dict.h"
#include "arrow/acero/hash_join_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/schema_util.h"
#include "arrow/acero/util.h"
#include "arrow/compute/key_hash.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

using compute::field_ref;
using compute::FilterOptions;
using compute::Hashing32;
using compute::KeyColumnArray;

namespace acero {

// Check if a type is supported in a join (as either a key or non-key column)
bool HashJoinSchema::IsTypeSupported(const DataType& type) {
  const Type::type id = type.id();
  if (id == Type::DICTIONARY) {
    return IsTypeSupported(*checked_cast<const DictionaryType&>(type).value_type());
  }
  if (id == Type::EXTENSION) {
    return IsTypeSupported(*checked_cast<const ExtensionType&>(type).storage_type());
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
                            const std::string& left_field_name_suffix,
                            const std::string& right_field_name_suffix) {
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
              right_output, filter, left_field_name_suffix, right_field_name_suffix);
}

Status HashJoinSchema::Init(
    JoinType join_type, const Schema& left_schema, const std::vector<FieldRef>& left_keys,
    const std::vector<FieldRef>& left_output, const Schema& right_schema,
    const std::vector<FieldRef>& right_keys, const std::vector<FieldRef>& right_output,
    const Expression& filter, const std::string& left_field_name_suffix,
    const std::string& right_field_name_suffix) {
  RETURN_NOT_OK(ValidateSchemas(join_type, left_schema, left_keys, left_output,
                                right_schema, right_keys, right_output,
                                left_field_name_suffix, right_field_name_suffix));

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
                                       const std::string& left_field_name_suffix,
                                       const std::string& right_field_name_suffix) {
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
  // suffixes to left and right output
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
    const std::string& left_field_name_suffix,
    const std::string& right_field_name_suffix) {
  std::vector<std::shared_ptr<Field>> fields;
  int left_size = proj_maps[0].num_cols(HashJoinProjection::OUTPUT);
  int right_size = proj_maps[1].num_cols(HashJoinProjection::OUTPUT);
  fields.resize(left_size + right_size);

  std::unordered_multimap<std::string, int> left_field_map;
  left_field_map.reserve(left_size);
  for (int i = 0; i < left_size; ++i) {
    int side = 0;  // left
    int input_field_id =
        proj_maps[side].map(HashJoinProjection::OUTPUT, HashJoinProjection::INPUT).get(i);
    const std::string& input_field_name =
        proj_maps[side].field_name(HashJoinProjection::INPUT, input_field_id);
    const std::shared_ptr<DataType>& input_data_type =
        proj_maps[side].data_type(HashJoinProjection::INPUT, input_field_id);
    left_field_map.insert({input_field_name, i});
    // insert left table field
    fields[i] =
        std::make_shared<Field>(input_field_name, input_data_type, true /*nullable*/);
  }

  for (int i = 0; i < right_size; ++i) {
    int side = 1;  // right
    int input_field_id =
        proj_maps[side].map(HashJoinProjection::OUTPUT, HashJoinProjection::INPUT).get(i);
    const std::string& input_field_name =
        proj_maps[side].field_name(HashJoinProjection::INPUT, input_field_id);
    const std::shared_ptr<DataType>& input_data_type =
        proj_maps[side].data_type(HashJoinProjection::INPUT, input_field_id);
    // search the map and add suffix to the elements which
    // are present both in left and right tables
    auto search_it = left_field_map.equal_range(input_field_name);
    bool match_found = false;
    for (auto search = search_it.first; search != search_it.second; ++search) {
      match_found = true;
      auto left_val = search->first;
      auto left_index = search->second;
      auto left_field = fields[left_index];
      // update left table field with suffix
      fields[left_index] =
          std::make_shared<Field>(input_field_name + left_field_name_suffix,
                                  left_field->type(), true /*nullable*/);
      // insert right table field with suffix
      fields[left_size + i] = std::make_shared<Field>(
          input_field_name + right_field_name_suffix, input_data_type, true /*nullable*/);
    }

    if (!match_found) {
      // insert right table field without suffix
      fields[left_size + i] =
          std::make_shared<Field>(input_field_name, input_data_type, true /*nullable*/);
    }
  }
  return std::make_shared<Schema>(std::move(fields));
}

Result<Expression> HashJoinSchema::BindFilter(Expression filter,
                                              const Schema& left_schema,
                                              const Schema& right_schema,
                                              ExecContext* exec_context) {
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
  ARROW_ASSIGN_OR_RAISE(filter, filter.Bind(filter_schema, exec_context));
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

Status ValidateHashJoinNodeOptions(const HashJoinNodeOptions& join_options) {
  if (join_options.key_cmp.empty() || join_options.left_keys.empty() ||
      join_options.right_keys.empty()) {
    return Status::Invalid("key_cmp and keys cannot be empty");
  }

  if ((join_options.key_cmp.size() != join_options.left_keys.size()) ||
      (join_options.key_cmp.size() != join_options.right_keys.size())) {
    return Status::Invalid("key_cmp and keys must have the same size");
  }

  return Status::OK();
}

class HashJoinNode;

// This is a struct encapsulating things related to Bloom filters and pushing them around
// between HashJoinNodes. The general strategy is to notify other joins at plan-creation
// time for that join to expect a Bloom filter. Once the full build side has been
// accumulated for a given join, it will build the Bloom filter and push it to its
// pushdown target. Once a join has received all of its Bloom filters, it will evaluate it
// on every batch that has been queued so far as well as any new probe-side batch that
// comes in.
struct BloomFilterPushdownContext {
  using RegisterTaskGroupCallback = std::function<int(
      std::function<Status(size_t, int64_t)>, std::function<Status(size_t)>)>;
  using StartTaskGroupCallback = std::function<Status(int, int64_t)>;
  using BuildFinishedCallback = std::function<Status(size_t, AccumulationQueue)>;
  using FiltersReceivedCallback = std::function<Status(size_t)>;
  using FilterFinishedCallback = std::function<Status(size_t, AccumulationQueue)>;
  void Init(HashJoinNode* owner, size_t num_threads,
            RegisterTaskGroupCallback register_task_group_callback,
            StartTaskGroupCallback start_task_group_callback,
            FiltersReceivedCallback on_bloom_filters_received, bool disable_bloom_filter,
            bool use_sync_execution);

  Status StartProducing(size_t thread_index);

  void ExpectBloomFilter() { eval_.num_expected_bloom_filters_ += 1; }

  // Builds the Bloom filter, taking ownership of the batches until the build
  // is done.
  Status BuildBloomFilter(size_t thread_index, AccumulationQueue batches,
                          BuildFinishedCallback on_finished);

  // Sends the Bloom filter to the pushdown target.
  Status PushBloomFilter(size_t thread_index);

  // Receives a Bloom filter and its associated column map.
  Status ReceiveBloomFilter(size_t thread_index,
                            std::unique_ptr<BlockedBloomFilter> filter,
                            std::vector<int> column_map) {
    bool proceed;
    {
      std::lock_guard<std::mutex> guard(eval_.receive_mutex_);
      eval_.received_filters_.emplace_back(std::move(filter));
      eval_.received_maps_.emplace_back(std::move(column_map));
      proceed = eval_.num_expected_bloom_filters_ == eval_.received_filters_.size();

      ARROW_DCHECK_EQ(eval_.received_filters_.size(), eval_.received_maps_.size());
      ARROW_DCHECK_LE(eval_.received_filters_.size(), eval_.num_expected_bloom_filters_);
    }
    if (proceed) {
      return eval_.all_received_callback_(thread_index);
    }
    return Status::OK();
  }

  // Evaluates the Bloom filter on a group of batches, taking ownership of them
  // until the whole filtering process is complete.
  Status FilterBatches(size_t thread_index, AccumulationQueue batches,
                       FilterFinishedCallback on_finished) {
    eval_.batches_ = std::move(batches);
    eval_.on_finished_ = std::move(on_finished);

    if (eval_.num_expected_bloom_filters_ == 0)
      return eval_.on_finished_(thread_index, std::move(eval_.batches_));

    return start_task_group_callback_(eval_.task_id_,
                                      /*num_tasks=*/eval_.batches_.batch_count());
  }

  // Applies all Bloom filters on the input batch.
  Status FilterSingleBatch(size_t thread_index, ExecBatch* batch_ptr) {
    ExecBatch& batch = *batch_ptr;
    if (eval_.num_expected_bloom_filters_ == 0 || batch.length == 0) return Status::OK();

    int64_t bit_vector_bytes = bit_util::BytesForBits(batch.length);
    std::vector<uint8_t> selected(bit_vector_bytes);
    std::vector<uint32_t> hashes(batch.length);
    std::vector<uint8_t> bv(bit_vector_bytes);

    ARROW_ASSIGN_OR_RAISE(arrow::util::TempVectorStack * stack,
                          ctx_->GetTempStack(thread_index));

    // Start with full selection for the current batch
    memset(selected.data(), 0xff, bit_vector_bytes);
    for (size_t ifilter = 0; ifilter < eval_.num_expected_bloom_filters_; ifilter++) {
      std::vector<Datum> keys(eval_.received_maps_[ifilter].size());
      for (size_t i = 0; i < keys.size(); i++) {
        int input_idx = eval_.received_maps_[ifilter][i];
        keys[i] = batch[input_idx];
        if (keys[i].is_scalar()) {
          ARROW_ASSIGN_OR_RAISE(
              keys[i],
              MakeArrayFromScalar(*keys[i].scalar(), batch.length, ctx_->memory_pool()));
        }
      }
      ARROW_ASSIGN_OR_RAISE(ExecBatch key_batch, ExecBatch::Make(std::move(keys)));
      std::vector<KeyColumnArray> temp_column_arrays;
      RETURN_NOT_OK(Hashing32::HashBatch(key_batch, hashes.data(), temp_column_arrays,
                                         ctx_->cpu_info()->hardware_flags(), stack, 0,
                                         key_batch.length));

      eval_.received_filters_[ifilter]->Find(ctx_->cpu_info()->hardware_flags(),
                                             key_batch.length, hashes.data(), bv.data());
      arrow::internal::BitmapAnd(bv.data(), 0, selected.data(), 0, key_batch.length, 0,
                                 selected.data());
    }
    auto selected_buffer = std::make_unique<Buffer>(selected.data(), bit_vector_bytes);
    ArrayData selected_arraydata(boolean(), batch.length,
                                 {nullptr, std::move(selected_buffer)});
    Datum selected_datum(selected_arraydata);
    FilterOptions options;
    size_t first_nonscalar = batch.values.size();
    for (size_t i = 0; i < batch.values.size(); i++) {
      if (!batch.values[i].is_scalar()) {
        ARROW_ASSIGN_OR_RAISE(batch.values[i], Filter(batch.values[i], selected_datum,
                                                      options, ctx_->exec_context()));
        first_nonscalar = std::min(first_nonscalar, i);
        ARROW_DCHECK_EQ(batch.values[i].length(), batch.values[first_nonscalar].length());
      }
    }
    // If they're all Scalar, then the length of the batch is the number of set bits
    if (first_nonscalar == batch.values.size())
      batch.length = arrow::internal::CountSetBits(selected.data(), 0, batch.length);
    else
      batch.length = batch.values[first_nonscalar].length();
    return Status::OK();
  }

 private:
  Status BuildBloomFilter_exec_task(size_t thread_index, int64_t task_id);

  Status BuildBloomFilter_on_finished(size_t thread_index) {
    return build_.on_finished_(thread_index, std::move(build_.batches_));
  }

  // The Bloom filter is built on the build side of some upstream join. For a join to
  // evaluate the Bloom filter on its input columns, it has to rearrange its input columns
  // to match the column order of the Bloom filter.
  //
  // The first part of the pair is the HashJoin to actually perform the pushdown into.
  // The second part is a mapping such that column_map[i] is the index of key i in
  // the first part's input.
  // If we should disable Bloom filter, returns nullptr and an empty vector, and sets
  // the disable_bloom_filter_ flag.
  std::pair<HashJoinNode*, std::vector<int>> GetPushdownTarget(HashJoinNode* start);

  StartTaskGroupCallback start_task_group_callback_;
  bool disable_bloom_filter_;
  HashJoinSchema* schema_mgr_;
  QueryContext* ctx_;

  struct {
    int task_id_;
    std::unique_ptr<BloomFilterBuilder> builder_;
    AccumulationQueue batches_;
    BuildFinishedCallback on_finished_;
  } build_;

  struct {
    std::unique_ptr<BlockedBloomFilter> bloom_filter_;
    HashJoinNode* pushdown_target_;
    std::vector<int> column_map_;
  } push_;

  struct {
    int task_id_;
    size_t num_expected_bloom_filters_ = 0;
    std::mutex receive_mutex_;
    std::vector<std::unique_ptr<BlockedBloomFilter>> received_filters_;
    std::vector<std::vector<int>> received_maps_;
    AccumulationQueue batches_;
    FiltersReceivedCallback all_received_callback_;
    FilterFinishedCallback on_finished_;
  } eval_;
};
bool HashJoinSchema::HasDictionaries() const {
  for (int side = 0; side <= 1; ++side) {
    for (int icol = 0; icol < proj_maps[side].num_cols(HashJoinProjection::INPUT);
         ++icol) {
      const std::shared_ptr<DataType>& column_type =
          proj_maps[side].data_type(HashJoinProjection::INPUT, icol);
      if (column_type->id() == Type::DICTIONARY) {
        return true;
      }
    }
  }
  return false;
}

bool HashJoinSchema::HasLargeBinary() const {
  for (int side = 0; side <= 1; ++side) {
    for (int icol = 0; icol < proj_maps[side].num_cols(HashJoinProjection::INPUT);
         ++icol) {
      const std::shared_ptr<DataType>& column_type =
          proj_maps[side].data_type(HashJoinProjection::INPUT, icol);
      if (is_large_binary_like(column_type->id())) {
        return true;
      }
    }
  }
  return false;
}

class HashJoinNode : public ExecNode, public TracedNode {
 public:
  HashJoinNode(ExecPlan* plan, NodeVector inputs, const HashJoinNodeOptions& join_options,
               std::shared_ptr<Schema> output_schema,
               std::unique_ptr<HashJoinSchema> schema_mgr, Expression filter,
               std::unique_ptr<HashJoinImpl> impl)
      : ExecNode(plan, inputs, {"left", "right"},
                 /*output_schema=*/std::move(output_schema)),
        TracedNode(this),
        join_type_(join_options.join_type),
        key_cmp_(join_options.key_cmp),
        filter_(std::move(filter)),
        schema_mgr_(std::move(schema_mgr)),
        impl_(std::move(impl)),
        disable_bloom_filter_(join_options.disable_bloom_filter) {
    complete_.store(false);
  }

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    // Number of input exec nodes must be 2
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 2, "HashJoinNode"));

    std::unique_ptr<HashJoinSchema> schema_mgr = std::make_unique<HashJoinSchema>();

    const auto& join_options = checked_cast<const HashJoinNodeOptions&>(options);
    RETURN_NOT_OK(ValidateHashJoinNodeOptions(join_options));

    const auto& left_schema = *(inputs[0]->output_schema());
    const auto& right_schema = *(inputs[1]->output_schema());

    // This will also validate input schemas
    if (join_options.output_all) {
      RETURN_NOT_OK(schema_mgr->Init(
          join_options.join_type, left_schema, join_options.left_keys, right_schema,
          join_options.right_keys, join_options.filter,
          join_options.output_suffix_for_left, join_options.output_suffix_for_right));
    } else {
      RETURN_NOT_OK(schema_mgr->Init(
          join_options.join_type, left_schema, join_options.left_keys,
          join_options.left_output, right_schema, join_options.right_keys,
          join_options.right_output, join_options.filter,
          join_options.output_suffix_for_left, join_options.output_suffix_for_right));
    }

    ARROW_ASSIGN_OR_RAISE(
        Expression filter,
        schema_mgr->BindFilter(join_options.filter, left_schema, right_schema,
                               plan->query_context()->exec_context()));

    // Generate output schema
    std::shared_ptr<Schema> output_schema = schema_mgr->MakeOutputSchema(
        join_options.output_suffix_for_left, join_options.output_suffix_for_right);

    // Create hash join implementation object
    // SwissJoin does not support:
    // a) 64-bit string offsets
    // b) residual predicates
    // c) dictionaries
    //
    bool use_swiss_join;
#if ARROW_LITTLE_ENDIAN
    use_swiss_join = (filter == literal(true)) && !schema_mgr->HasDictionaries() &&
                     !schema_mgr->HasLargeBinary();
#else
    use_swiss_join = false;
#endif
    std::unique_ptr<HashJoinImpl> impl;
    if (use_swiss_join) {
      ARROW_ASSIGN_OR_RAISE(impl, HashJoinImpl::MakeSwiss());
    } else {
      ARROW_ASSIGN_OR_RAISE(impl, HashJoinImpl::MakeBasic());
    }

    return plan->EmplaceNode<HashJoinNode>(
        plan, inputs, join_options, std::move(output_schema), std::move(schema_mgr),
        std::move(filter), std::move(impl));
  }

  const char* kind_name() const override { return "HashJoinNode"; }

  Status OnBuildSideBatch(size_t thread_index, ExecBatch batch) {
    std::lock_guard<std::mutex> guard(build_side_mutex_);
    build_accumulator_.InsertBatch(std::move(batch));
    return Status::OK();
  }

  Status OnBuildSideFinished(size_t thread_index) {
    return pushdown_context_.BuildBloomFilter(
        thread_index, std::move(build_accumulator_),
        [this](size_t thread_index, AccumulationQueue batches) {
          return OnBloomFilterFinished(thread_index, std::move(batches));
        });
  }

  Status OnBloomFilterFinished(size_t thread_index, AccumulationQueue batches) {
    RETURN_NOT_OK(pushdown_context_.PushBloomFilter(thread_index));
    return impl_->BuildHashTable(
        thread_index, std::move(batches),
        [this](size_t thread_index) { return OnHashTableFinished(thread_index); });
  }

  Status OnHashTableFinished(size_t thread_index) {
    bool should_probe;
    {
      std::lock_guard<std::mutex> guard(probe_side_mutex_);
      should_probe = queued_batches_filtered_ && !hash_table_ready_;
      hash_table_ready_ = true;
    }
    if (should_probe) {
      return ProbeQueuedBatches(thread_index);
    }
    return Status::OK();
  }

  Status OnProbeSideBatch(size_t thread_index, ExecBatch batch) {
    {
      std::lock_guard<std::mutex> guard(probe_side_mutex_);
      if (!bloom_filters_ready_) {
        probe_accumulator_.InsertBatch(std::move(batch));
        return Status::OK();
      }
    }
    RETURN_NOT_OK(pushdown_context_.FilterSingleBatch(thread_index, &batch));

    {
      std::lock_guard<std::mutex> guard(probe_side_mutex_);
      if (!hash_table_ready_) {
        probe_accumulator_.InsertBatch(std::move(batch));
        return Status::OK();
      }
    }
    RETURN_NOT_OK(impl_->ProbeSingleBatch(thread_index, std::move(batch)));
    return Status::OK();
  }

  Status OnProbeSideFinished(size_t thread_index) {
    bool probing_finished;
    {
      std::lock_guard<std::mutex> guard(probe_side_mutex_);
      probing_finished = queued_batches_probed_ && !probe_side_finished_;
      probe_side_finished_ = true;
    }
    if (probing_finished) return impl_->ProbingFinished(thread_index);
    return Status::OK();
  }

  Status OnFiltersReceived(size_t thread_index) {
    std::unique_lock<std::mutex> guard(probe_side_mutex_);
    bloom_filters_ready_ = true;
    AccumulationQueue batches = std::move(probe_accumulator_);
    guard.unlock();
    return pushdown_context_.FilterBatches(
        thread_index, std::move(batches),
        [this](size_t thread_index, AccumulationQueue batches) {
          return OnQueuedBatchesFiltered(thread_index, std::move(batches));
        });
  }

  Status OnQueuedBatchesFiltered(size_t thread_index, AccumulationQueue batches) {
    bool should_probe;
    {
      std::lock_guard<std::mutex> guard(probe_side_mutex_);
      probe_accumulator_.Concatenate(std::move(batches));
      should_probe = !queued_batches_filtered_ && hash_table_ready_;
      queued_batches_filtered_ = true;
    }
    if (should_probe) {
      return ProbeQueuedBatches(thread_index);
    }
    return Status::OK();
  }

  Status ProbeQueuedBatches(size_t thread_index) {
    {
      std::lock_guard<std::mutex> guard(probe_side_mutex_);
      queued_batches_to_probe_ = std::move(probe_accumulator_);
    }
    return plan_->query_context()->StartTaskGroup(task_group_probe_,
                                                  queued_batches_to_probe_.batch_count());
  }

  Status OnQueuedBatchesProbed(size_t thread_index) {
    queued_batches_to_probe_.Clear();
    bool probing_finished;
    {
      std::lock_guard<std::mutex> guard(probe_side_mutex_);
      probing_finished = !queued_batches_probed_ && probe_side_finished_;
      queued_batches_probed_ = true;
    }
    if (probing_finished) return impl_->ProbingFinished(thread_index);
    return Status::OK();
  }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());
    if (complete_.load()) {
      return Status::OK();
    }

    size_t thread_index = plan_->query_context()->GetThreadIndex();
    int side = (input == inputs_[0]) ? 0 : 1;

    if (side == 0) {
      ARROW_RETURN_NOT_OK(OnProbeSideBatch(thread_index, std::move(batch)));
    } else {
      ARROW_RETURN_NOT_OK(OnBuildSideBatch(thread_index, std::move(batch)));
    }

    if (batch_count_[side].Increment()) {
      if (side == 0) {
        return OnProbeSideFinished(thread_index);
      } else {
        return OnBuildSideFinished(thread_index);
      }
    }
    return Status::OK();
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    ARROW_DCHECK(std::find(inputs_.begin(), inputs_.end(), input) != inputs_.end());
    size_t thread_index = plan_->query_context()->GetThreadIndex();
    int side = (input == inputs_[0]) ? 0 : 1;

    if (batch_count_[side].SetTotal(total_batches)) {
      if (side == 0) {
        return OnProbeSideFinished(thread_index);
      } else {
        return OnBuildSideFinished(thread_index);
      }
    }
    return Status::OK();
  }

  Status Init() override {
    QueryContext* ctx = plan_->query_context();
    if (ctx->options().use_legacy_batching) {
      return Status::Invalid(
          "The plan was configured to use legacy batching but contained a join node "
          "which is incompatible with legacy batching");
    }

    bool use_sync_execution = ctx->executor()->GetCapacity() == 1;
    // TODO(ARROW-15732)
    // Each side of join might have an IO thread being called from. Once this is fixed
    // we will change it back to just the CPU's thread pool capacity.
    size_t num_threads = (GetCpuThreadPoolCapacity() + io::GetIOThreadPoolCapacity() + 1);

    pushdown_context_.Init(
        this, num_threads,
        [ctx](std::function<Status(size_t, int64_t)> fn,
              std::function<Status(size_t)> on_finished) {
          return ctx->RegisterTaskGroup(std::move(fn), std::move(on_finished));
        },
        [ctx](int task_group_id, int64_t num_tasks) {
          return ctx->StartTaskGroup(task_group_id, num_tasks);
        },
        [this](size_t thread_index) { return OnFiltersReceived(thread_index); },
        disable_bloom_filter_, use_sync_execution);

    RETURN_NOT_OK(impl_->Init(
        ctx, join_type_, num_threads, &(schema_mgr_->proj_maps[0]),
        &(schema_mgr_->proj_maps[1]), key_cmp_, filter_,
        [ctx](std::function<Status(size_t, int64_t)> fn,
              std::function<Status(size_t)> on_finished) {
          return ctx->RegisterTaskGroup(std::move(fn), std::move(on_finished));
        },
        [ctx](int task_group_id, int64_t num_tasks) {
          return ctx->StartTaskGroup(task_group_id, num_tasks);
        },
        [this](int64_t, ExecBatch batch) { return this->OutputBatchCallback(batch); },
        [this](int64_t total_num_batches) {
          return this->FinishedCallback(total_num_batches);
        }));

    task_group_probe_ = ctx->RegisterTaskGroup(
        [this](size_t thread_index, int64_t task_id) -> Status {
          return impl_->ProbeSingleBatch(thread_index,
                                         std::move(queued_batches_to_probe_[task_id]));
        },
        [this](size_t thread_index) -> Status {
          return OnQueuedBatchesProbed(thread_index);
        });

    return Status::OK();
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    RETURN_NOT_OK(
        pushdown_context_.StartProducing(plan_->query_context()->GetThreadIndex()));
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-16246)
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-16246)
  }

  Status StopProducingImpl() override {
    bool expected = false;
    if (complete_.compare_exchange_strong(expected, true)) {
      impl_->Abort([]() {});
    }
    return Status::OK();
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    return "implementation=" + impl_->ToString();
  }

 private:
  Status OutputBatchCallback(ExecBatch batch) {
    return output_->InputReceived(this, std::move(batch));
  }

  Status FinishedCallback(int64_t total_num_batches) {
    bool expected = false;
    if (complete_.compare_exchange_strong(expected, true)) {
      return output_->InputFinished(this, static_cast<int>(total_num_batches));
    }
    return Status::OK();
  }

 private:
  AtomicCounter batch_count_[2];
  std::atomic<bool> complete_;
  JoinType join_type_;
  std::vector<JoinKeyCmp> key_cmp_;
  Expression filter_;
  std::unique_ptr<HashJoinSchema> schema_mgr_;
  std::unique_ptr<HashJoinImpl> impl_;
  util::AccumulationQueue build_accumulator_;
  util::AccumulationQueue probe_accumulator_;
  util::AccumulationQueue queued_batches_to_probe_;

  std::mutex build_side_mutex_;
  std::mutex probe_side_mutex_;

  int task_group_probe_;
  bool bloom_filters_ready_ = false;
  bool hash_table_ready_ = false;
  bool queued_batches_filtered_ = false;
  bool queued_batches_probed_ = false;
  bool probe_side_finished_ = false;

  friend struct BloomFilterPushdownContext;
  bool disable_bloom_filter_;
  BloomFilterPushdownContext pushdown_context_;
};

void BloomFilterPushdownContext::Init(
    HashJoinNode* owner, size_t num_threads,
    RegisterTaskGroupCallback register_task_group_callback,
    StartTaskGroupCallback start_task_group_callback,
    FiltersReceivedCallback on_bloom_filters_received, bool disable_bloom_filter,
    bool use_sync_execution) {
  schema_mgr_ = owner->schema_mgr_.get();
  ctx_ = owner->plan_->query_context();
  disable_bloom_filter_ = disable_bloom_filter;
  std::tie(push_.pushdown_target_, push_.column_map_) = GetPushdownTarget(owner);
  eval_.all_received_callback_ = std::move(on_bloom_filters_received);
  if (!disable_bloom_filter_) {
    ARROW_CHECK(push_.pushdown_target_);
    push_.bloom_filter_ = std::make_unique<BlockedBloomFilter>();
    push_.pushdown_target_->pushdown_context_.ExpectBloomFilter();

    build_.builder_ = BloomFilterBuilder::Make(
        use_sync_execution ? BloomFilterBuildStrategy::SINGLE_THREADED
                           : BloomFilterBuildStrategy::PARALLEL);

    build_.task_id_ = register_task_group_callback(
        [this](size_t thread_index, int64_t task_id) {
          return BuildBloomFilter_exec_task(thread_index, task_id);
        },
        [this](size_t thread_index) {
          return BuildBloomFilter_on_finished(thread_index);
        });
  }

  eval_.task_id_ = register_task_group_callback(
      [this](size_t thread_index, int64_t task_id) {
        return FilterSingleBatch(thread_index, &eval_.batches_[task_id]);
      },
      [this](size_t thread_index) {
        return eval_.on_finished_(thread_index, std::move(eval_.batches_));
      });
  start_task_group_callback_ = std::move(start_task_group_callback);
}

Status BloomFilterPushdownContext::StartProducing(size_t thread_index) {
  if (eval_.num_expected_bloom_filters_ == 0)
    return eval_.all_received_callback_(thread_index);
  return Status::OK();
}

Status BloomFilterPushdownContext::BuildBloomFilter(size_t thread_index,
                                                    AccumulationQueue batches,
                                                    BuildFinishedCallback on_finished) {
  build_.batches_ = std::move(batches);
  build_.on_finished_ = std::move(on_finished);

  if (disable_bloom_filter_)
    return build_.on_finished_(thread_index, std::move(build_.batches_));

  RETURN_NOT_OK(build_.builder_->Begin(
      /*num_threads=*/ctx_->max_concurrency(), ctx_->cpu_info()->hardware_flags(),
      ctx_->memory_pool(), build_.batches_.row_count(), build_.batches_.batch_count(),
      push_.bloom_filter_.get()));

  return start_task_group_callback_(build_.task_id_,
                                    /*num_tasks=*/build_.batches_.batch_count());
}

Status BloomFilterPushdownContext::PushBloomFilter(size_t thread_index) {
  if (!disable_bloom_filter_)
    return push_.pushdown_target_->pushdown_context_.ReceiveBloomFilter(
        thread_index, std::move(push_.bloom_filter_), std::move(push_.column_map_));
  return Status::OK();
}

Status BloomFilterPushdownContext::BuildBloomFilter_exec_task(size_t thread_index,
                                                              int64_t task_id) {
  const ExecBatch& input_batch = build_.batches_[task_id];
  SchemaProjectionMap key_to_in =
      schema_mgr_->proj_maps[1].map(HashJoinProjection::KEY, HashJoinProjection::INPUT);
  std::vector<Datum> key_columns(key_to_in.num_cols);
  for (size_t i = 0; i < key_columns.size(); i++) {
    int input_idx = key_to_in.get(static_cast<int>(i));
    key_columns[i] = input_batch[input_idx];
    if (key_columns[i].is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(key_columns[i],
                            MakeArrayFromScalar(*key_columns[i].scalar(),
                                                input_batch.length, ctx_->memory_pool()));
    }
  }
  ARROW_ASSIGN_OR_RAISE(ExecBatch key_batch, ExecBatch::Make(std::move(key_columns)));

  ARROW_ASSIGN_OR_RAISE(arrow::util::TempVectorStack * stack,
                        ctx_->GetTempStack(thread_index));
  arrow::util::TempVectorHolder<uint32_t> hash_holder(
      stack, arrow::util::MiniBatch::kMiniBatchLength);
  uint32_t* hashes = hash_holder.mutable_data();
  for (int64_t i = 0; i < key_batch.length;
       i += arrow::util::MiniBatch::kMiniBatchLength) {
    int64_t length =
        std::min(static_cast<int64_t>(key_batch.length - i),
                 static_cast<int64_t>(arrow::util::MiniBatch::kMiniBatchLength));

    std::vector<KeyColumnArray> temp_column_arrays;
    RETURN_NOT_OK(Hashing32::HashBatch(key_batch, hashes, temp_column_arrays,
                                       ctx_->cpu_info()->hardware_flags(), stack, i,
                                       length));
    RETURN_NOT_OK(build_.builder_->PushNextBatch(thread_index, length, hashes));
  }
  return Status::OK();
}

std::pair<HashJoinNode*, std::vector<int>> BloomFilterPushdownContext::GetPushdownTarget(
    HashJoinNode* start) {
#if !ARROW_LITTLE_ENDIAN
  // TODO (ARROW-16591): Debug bloom_filter.cc to enable on Big endian. It probably just
  // needs a few byte swaps in the proper spots.
  disable_bloom_filter_ = true;
  return {nullptr, {}};
#else
  if (disable_bloom_filter_) return {nullptr, {}};
  JoinType join_type = start->join_type_;

  // A build-side Bloom filter tells us if a row is definitely not in the build side.
  // This allows us to early-eliminate rows or early-accept rows depending on the type
  // of join. Left Outer Join and Full Outer Join output all rows, so a build-side Bloom
  // filter would only allow us to early-output. Left Antijoin outputs only if there is
  // no match, so again early output. We don't implement early output for now, so we
  // must disallow these types of joins.
  bool bloom_filter_does_not_apply_to_join = join_type == JoinType::LEFT_ANTI ||
                                             join_type == JoinType::LEFT_OUTER ||
                                             join_type == JoinType::FULL_OUTER;
  disable_bloom_filter_ = disable_bloom_filter_ || bloom_filter_does_not_apply_to_join;

  // Bloom filter currently doesn't support dictionaries.
  for (int side = 0; side <= 1 && !disable_bloom_filter_; side++) {
    SchemaProjectionMap keys_to_input = start->schema_mgr_->proj_maps[side].map(
        HashJoinProjection::KEY, HashJoinProjection::INPUT);
    // Bloom filter currently doesn't support dictionaries.
    for (int i = 0; i < keys_to_input.num_cols; i++) {
      int idx = keys_to_input.get(i);
      bool is_dict = start->inputs_[side]->output_schema()->field(idx)->type()->id() ==
                     Type::DICTIONARY;
      if (is_dict) {
        disable_bloom_filter_ = true;
        break;
      }
    }
  }

  bool all_comparisons_is = true;
  for (JoinKeyCmp cmp : start->key_cmp_) all_comparisons_is &= (cmp == JoinKeyCmp::IS);

  if ((join_type == JoinType::RIGHT_OUTER || join_type == JoinType::FULL_OUTER) &&
      all_comparisons_is)
    disable_bloom_filter_ = true;

  if (disable_bloom_filter_) return {nullptr, {}};

  // We currently only push Bloom filters on the probe side, and only if that input is
  // also a join.
  SchemaProjectionMap probe_key_to_input = start->schema_mgr_->proj_maps[0].map(
      HashJoinProjection::KEY, HashJoinProjection::INPUT);
  int num_keys = probe_key_to_input.num_cols;

  // A mapping such that bloom_to_target[i] is the index of key i in the pushdown
  // target's input
  std::vector<int> bloom_to_target(num_keys);
  HashJoinNode* pushdown_target = start;
  for (int i = 0; i < num_keys; i++) bloom_to_target[i] = probe_key_to_input.get(i);

  for (ExecNode* candidate = start->inputs()[0];
       candidate->kind_name() == start->kind_name(); candidate = candidate->inputs()[0]) {
    auto* candidate_as_join = checked_cast<HashJoinNode*>(candidate);
    SchemaProjectionMap candidate_output_to_input =
        candidate_as_join->schema_mgr_->proj_maps[0].map(HashJoinProjection::OUTPUT,
                                                         HashJoinProjection::INPUT);

    // Check if any of the keys are missing, if they are, break
    bool break_outer = false;
    for (int i = 0; i < num_keys; i++) {
      // Since all of the probe side columns are before the build side columns,
      // if the index of an output is greater than the number of probe-side input
      // columns, it must have come from the candidate's build side.
      if (bloom_to_target[i] >= candidate_output_to_input.num_cols) {
        break_outer = true;
        break;
      }
      int candidate_input_idx = candidate_output_to_input.get(bloom_to_target[i]);
      // The output column has to have come from somewhere...
      ARROW_DCHECK_NE(candidate_input_idx, start->schema_mgr_->kMissingField());
    }
    if (break_outer) break;

    // The Bloom filter will filter out nulls, which may cause a Right/Full Outer Join
    // to incorrectly output some rows with nulls padding the probe-side rows. This may
    // cause a row with all null keys to be emitted. This is normally not an issue
    // with EQ, but if all comparisons are IS (i.e. all-null is accepted), this could
    // produce incorrect rows.
    bool can_produce_build_side_nulls =
        candidate_as_join->join_type_ == JoinType::RIGHT_OUTER ||
        candidate_as_join->join_type_ == JoinType::FULL_OUTER;

    if (all_comparisons_is || can_produce_build_side_nulls) break;

    // All keys are present, we can update the mapping
    for (int i = 0; i < num_keys; i++) {
      int candidate_input_idx = candidate_output_to_input.get(bloom_to_target[i]);
      bloom_to_target[i] = candidate_input_idx;
    }
    pushdown_target = candidate_as_join;
  }
  return std::make_pair(pushdown_target, std::move(bloom_to_target));
#endif  // ARROW_LITTLE_ENDIAN
}

namespace internal {
void RegisterHashJoinNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("hashjoin", HashJoinNode::Make));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow
