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
#include <mutex>
#include <thread>
#include <unordered_set>

#include "arrow/acero/aggregate_internal.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/util.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

using compute::ExecSpan;
using compute::ExecValue;
using compute::Function;
using compute::FunctionOptions;
using compute::Grouper;
using compute::HashAggregateKernel;
using compute::Kernel;
using compute::KernelContext;
using compute::KernelInitArgs;
using compute::KernelState;
using compute::RowSegmenter;
using compute::ScalarAggregateKernel;
using compute::Segment;

namespace acero {
namespace aggregate {

/*
 * Implementation notes:
 * - windows are defined over segment column values
 * - only finite windows are supported
 * - only sliding windows are supported
 * - only a single segment column is supported
 * - segment values must be unique
 * - segment values must be comparable
 * - buffers enough input ExecBatch to cover a window + at least one more segment value
 * after its end
 * - once a window is 'ready' all window data is stream through the aggregator(s)
 * - outputs one segment id per output ExecBatch
 * - time complexity: O(num_input_rows x window_size)
 * - space complexity: O(window_size)
 */

Result<AggregateNodeArgs<HashAggregateKernel>> WindowGroupByNode::MakeAggregateNodeArgs(
    const std::shared_ptr<Schema>& input_schema, const std::vector<FieldRef>& keys,
    const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggs,
    ExecContext* ctx, const std::optional<WindowAggregateArgs> window_args) {
  // Find input field indices for key fields
  std::vector<int> key_field_ids(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, keys[i].FindOne(*input_schema));
    key_field_ids[i] = match[0];
  }

  // Find input field indices for segment key fields
  std::vector<int> segment_key_field_ids(segment_keys.size());
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto match, segment_keys[i].FindOne(*input_schema));
    segment_key_field_ids[i] = match[0];
  }

  // Check key fields and segment key fields are disjoint
  std::unordered_set<int> key_field_id_set(key_field_ids.begin(), key_field_ids.end());
  for (const auto& segment_key_field_id : segment_key_field_ids) {
    if (key_field_id_set.find(segment_key_field_id) != key_field_id_set.end()) {
      return Status::Invalid("Group-by aggregation with field '",
                             input_schema->field(segment_key_field_id)->name(),
                             "' as both key and segment key");
    }
  }

  // Find input field indices for aggregates
  std::vector<std::vector<int>> agg_src_fieldsets(aggs.size());
  for (size_t i = 0; i < aggs.size(); ++i) {
    const auto& target_fieldset = aggs[i].target;
    for (const auto& target : target_fieldset) {
      ARROW_ASSIGN_OR_RAISE(auto match, target.FindOne(*input_schema));
      agg_src_fieldsets[i].push_back(match[0]);
    }
  }

  // Build vector of aggregate source field data types
  std::vector<std::vector<TypeHolder>> agg_src_types(aggs.size());
  for (size_t i = 0; i < aggs.size(); ++i) {
    for (const auto& agg_src_field_id : agg_src_fieldsets[i]) {
      agg_src_types[i].emplace_back(input_schema->field(agg_src_field_id)->type().get());
    }
  }

  // Build vector of segment key field data types
  std::vector<TypeHolder> segment_key_types(segment_keys.size());
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    auto segment_key_field_id = segment_key_field_ids[i];
    segment_key_types[i] = input_schema->field(segment_key_field_id)->type().get();
  }

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<RowSegmenter> segmenter,
                        RowSegmenter::Make(std::move(segment_key_types),
                                           /*nullable_keys=*/false, ctx));

  // Construct aggregates
  ARROW_ASSIGN_OR_RAISE(auto agg_kernels, GetKernels(ctx, aggs, agg_src_types));

  ARROW_ASSIGN_OR_RAISE(auto agg_states,
                        InitKernels(agg_kernels, ctx, aggs, agg_src_types));

  ARROW_ASSIGN_OR_RAISE(
      FieldVector agg_result_fields,
      ResolveKernels(aggs, agg_kernels, agg_states, ctx, agg_src_types));

  // Build field vector for output schema
  FieldVector output_fields{keys.size() + segment_keys.size() + aggs.size()};

  // First output is segment keys, followed by keys, followed by aggregates themselves
  // This matches the behavior described by Substrait and also tends to be the behavior
  // in SQL engines
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    int segment_key_field_id = segment_key_field_ids[i];
    output_fields[i] = input_schema->field(segment_key_field_id);
  }
  size_t base = segment_keys.size();
  for (size_t i = 0; i < keys.size(); ++i) {
    int key_field_id = key_field_ids[i];
    output_fields[base + i] = input_schema->field(key_field_id);
  }
  base += keys.size();
  for (size_t i = 0; i < aggs.size(); ++i) {
    output_fields[base + i] = agg_result_fields[i]->WithName(aggs[i].name);
  }

  return AggregateNodeArgs<HashAggregateKernel>{schema(std::move(output_fields)),
                                                std::move(key_field_ids),
                                                std::move(segment_key_field_ids),
                                                std::move(segmenter),
                                                std::move(agg_src_fieldsets),
                                                std::move(aggs),
                                                std::move(agg_kernels),
                                                std::move(agg_src_types),
                                                /*states=*/{},
                                                std::move(window_args)};
}

Status WindowGroupByNode::InitStatesVector(KernelContext& kernel_ctx) {
  states_.resize(1);
  ARROW_ASSIGN_OR_RAISE(
      states_[0], InitKernels(agg_kernels_, plan_->query_context()->exec_context(), aggs_,
                              agg_src_types_));

  // Get input schema
  auto input_schema = inputs_[0]->output_schema();

  // Build vector of key field data types
  std::vector<TypeHolder> key_types(key_field_ids_.size());
  for (size_t i = 0; i < key_field_ids_.size(); ++i) {
    auto key_field_id = key_field_ids_[i];
    key_types[i] = input_schema->field(key_field_id)->type().get();
  }

  // Construct grouper
  ARROW_ASSIGN_OR_RAISE(grouper,
                        Grouper::Make(key_types, plan_->query_context()->exec_context()));

  return Status::OK();
}

Result<ExecNode*> WindowGroupByNode::Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                          const ExecNodeOptions& options) {
  RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "WindowedNode"));

  auto input = inputs[0];
  const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
  const auto& keys = aggregate_options.keys;
  const auto& segment_keys = aggregate_options.segment_keys;
  auto aggs = aggregate_options.aggregates;
  bool is_cpu_parallel = plan->query_context()->executor()->GetCapacity() > 1;

  if (keys.size() == 0) {
    return Status::Invalid("Hash window aggregation without keys");
  }
  if (!aggregate_options.window_args) {
    return Status::Invalid("Hash window aggregation without window definition");
  }
  if (segment_keys.size() != 1) {
    return Status::Invalid("Hash window aggregation needs exactly one segment key");
  }
  if (is_cpu_parallel) {
    return Status::NotImplemented(
        "Hash window aggregator in multiple threaded execution");
  }

  const auto& input_schema = input->output_schema();
  auto exec_ctx = plan->query_context()->exec_context();
  ARROW_ASSIGN_OR_RAISE(
      auto args, MakeAggregateNodeArgs(input_schema, keys, segment_keys, aggs, exec_ctx,
                                       aggregate_options.window_args));

  return input->plan()->EmplaceNode<WindowGroupByNode>(
      input, std::move(args.output_schema), std::move(args.grouping_key_field_ids),
      std::move(args.segment_key_field_ids), std::move(args.segmenter),
      std::move(args.kernel_intypes), std::move(args.target_fieldsets),
      std::move(args.aggregates), std::move(args.kernels), std::move(args.window_args));
}

// Extract segment value and queue up batches, then call OutputResults
Status WindowGroupByNode::InputReceived(ExecNode* input, ExecBatch batch) {
  auto scope = TraceInputReceived(batch);
  DCHECK_EQ(input, inputs_[0]);

  // compute segments
  int64_t offset = 0;
  ARROW_ASSIGN_OR_RAISE(auto segment_exec_batch,
                        batch.SelectValues(segment_key_field_ids_));
  ExecSpan segment_batch(segment_exec_batch);

  std::vector<compute::Segment> segments;
  std::vector<Datum> segment_values;

  while (true) {
    // for each input segment
    ARROW_ASSIGN_OR_RAISE(compute::Segment segment,
                          segmenter_->GetNextSegment(segment_batch, offset));
    if (segment.offset >= segment_batch.length) break;  // condition of no-next-segment

    // extract (the single) segment values
    auto exec_batch = batch.Slice(segment.offset, segment.length);
    ARROW_ASSIGN_OR_RAISE(std::vector<Datum> segment_val,
                          ExtractValues(exec_batch, segment_key_field_ids_));

    // store segment boundaries and their value
    segments.push_back(std::move(segment));
    segment_values.push_back(std::move(segment_val.front()));
    offset = segment.offset + segment.length;
  }

  // set first available segment value once
  if (!current_datum.is_value()) {
    current_datum = segment_values.front();
    current_index = std::make_pair(0, 0);
  }

  // buffer data
  window_batches.push_back(std::move(batch));
  window_segment_values.push_back(std::move(segment_values));
  window_segments.push_back(std::move(segments));
  return OutputResult();
}

Status WindowGroupByNode::InputFinished(ExecNode* input, int total_batches) {
  auto scope = TraceFinish();
  EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
  DCHECK_EQ(input, inputs_[0]);
  ARROW_RETURN_NOT_OK(OutputResult(true));
  ARROW_RETURN_NOT_OK(output_->InputFinished(this, total_output_batches_));
  return Status::OK();
}

std::string WindowGroupByNode::ToStringExtra(int indent) const {
  std::stringstream ss;
  const auto input_schema = inputs_[0]->output_schema();
  ss << "keys=[";
  for (size_t i = 0; i < key_field_ids_.size(); i++) {
    if (i > 0) ss << ", ";
    ss << '"' << input_schema->field(key_field_ids_[i])->name() << '"';
  }
  ss << "], ";
  AggregatesToString(&ss, *input_schema, aggs_, agg_src_fieldsets_, indent);
  return ss.str();
}

// find the leftmost index for the window:
// since data is ordered bty segment key we stop at the first one which makes the bound
// comparison true
Result<std::pair<uint32_t, uint32_t>> WindowGroupByNode::FindLeftIndex(const Datum& data,
                                                                       ExecContext* ctx) {
  const Datum trueDatum(true);

  for (size_t i = 0; i < window_segment_values.size(); ++i) {
    for (size_t j = 0; j < window_segment_values[i].size(); j++) {
      Datum& scalar = window_segment_values[i][j];
      ARROW_ASSIGN_OR_RAISE(
          Datum res, compute::CallFunction(left_inclusive, {scalar, data}, nullptr, ctx));
      if (trueDatum == res) {
        return std::make_pair(i, j);
      }
    }
  }
  return Status::Invalid("Cannot find item");
}

// Find the rightmost index for the window:
// since data is ordered bty segment key we return the index before the first one which
// makes the bound comparison true
Result<std::pair<uint32_t, uint32_t>> WindowGroupByNode::FindRightIndex(const Datum& data,
                                                                        ExecContext* ctx,
                                                                        bool flush) {
  const Datum trueDatum(true);

  std::pair<uint32_t, uint32_t> current = std::make_pair(0, 0);
  for (size_t i = 0; i < window_segment_values.size(); ++i) {
    for (size_t j = 0; j < window_segment_values[i].size(); j++) {
      Datum& scalar = window_segment_values[i][j];
      ARROW_ASSIGN_OR_RAISE(
          Datum res,
          compute::CallFunction(right_inclusive, {scalar, data}, nullptr, ctx));
      if (trueDatum == res) {
        return current;
      }
      current = std::make_pair(i, j);
    }
  }
  if (flush) return current;
  return Status::Invalid("Cannot find item");
}

// Push batch to all kernels
Status WindowGroupByNode::Consume(KernelContext ctx, ExecSpan& run_batch) {
  // Create a batch with key columns
  std::vector<ExecValue> keys(key_field_ids_.size());
  for (size_t i = 0; i < key_field_ids_.size(); ++i) {
    keys[i] = run_batch[key_field_ids_[i]];
  }
  ExecSpan key_batch(std::move(keys), run_batch.length);

  // Create a batch with group ids
  ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper->Consume(key_batch));

  for (size_t i = 0; i < agg_kernels_.size(); ++i) {
    arrow::util::tracing::Span span;
    START_COMPUTE_SPAN(span, aggs_[i].function,
                       {{"function.name", aggs_[i].function},
                        {"function.options",
                         aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                        {"function.kind", std::string(kind_name()) + "::Consume"}});
    ctx.SetState(states_[0][i].get());

    std::vector<ExecValue> column_values;
    for (const int field : agg_src_fieldsets_[i]) {
      column_values.push_back(run_batch[field]);
    }
    column_values.emplace_back(*id_batch.array());

    ExecSpan column_batch{std::move(column_values), run_batch.length};
    RETURN_NOT_OK(agg_kernels_[i]->resize(&ctx, grouper->num_groups()));
    RETURN_NOT_OK(agg_kernels_[i]->consume(&ctx, column_batch));
  }
  return Status::OK();
}

// finalize all kernels and add aggregate values to output batch
Status WindowGroupByNode::Finalize(KernelContext ctx, ExecBatch& batch,
                                   std::size_t base) {
  for (size_t i = 0; i < agg_kernels_.size(); ++i) {
    arrow::util::tracing::Span span;
    START_COMPUTE_SPAN(span, aggs_[i].function,
                       {{"function.name", aggs_[i].function},
                        {"function.options",
                         aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                        {"function.kind", std::string(kind_name()) + "::Finalize"}});
    ctx.SetState(states_[0][i].get());
    RETURN_NOT_OK(agg_kernels_[i]->finalize(&ctx, &batch.values[base + i]));
    states_[0][i].reset();
  }
  grouper.reset();
  return Status::OK();
}

// Output as many complete windows as possible
// if flush output the tail of the operation (incomplete windows)
Status WindowGroupByNode::OutputResult(bool flush) {
  ExecContext* exec_ctx = plan()->query_context()->exec_context();
  KernelContext ctx{exec_ctx};
  std::size_t base = segment_key_field_ids_.size() + key_field_ids_.size();
  const Datum trueDatum(true);

  // try to output all possible data
  while (true) {
    RETURN_NOT_OK(InitStatesVector(ctx));

    while (current_done) {
      // move current row pointer
      size_t cur_len = window_segment_values[current_index.first].size();
      size_t total_len = window_segment_values.size();
      if (!flush && (current_index.second + 1 >= cur_len) &&
          (current_index.first + 1 >= total_len)) {
        return Status::OK();
      }
      current_index.second++;
      if (current_index.second >= cur_len) {
        if (flush && (current_index.first + 1 >= total_len)) {
          return Status::OK();
        }
        current_index.second = 0;
        current_index.first++;
      }
      Datum& new_datum = window_segment_values[current_index.first][current_index.second];
      ARROW_ASSIGN_OR_RAISE(
          Datum res,
          compute::CallFunction("equal", {current_datum, new_datum}, nullptr, exec_ctx));
      if (trueDatum != res) {
        current_datum = new_datum;
        current_done = false;
      }
    }

    // compute current bounds
    ARROW_ASSIGN_OR_RAISE(const Datum win_min, compute::Subtract(current_datum, left));
    ARROW_ASSIGN_OR_RAISE(const Datum win_max, compute::Add(current_datum, right));

    // locate bounds indices
    Result<std::pair<uint32_t, uint32_t>> win_min_index =
        FindLeftIndex(win_min, exec_ctx);
    Result<std::pair<uint32_t, uint32_t>> win_max_index =
        FindRightIndex(win_max, exec_ctx, flush);

    // check if we have enough data for the window
    if (!win_min_index.ok() || !win_max_index.ok()) {
      // missing one of the two boundaries, return ok to continue with next input
      return Status::OK();
    }

    uint32_t first = win_min_index->first;
    uint32_t last = win_max_index->first;
    uint32_t second = win_min_index->second;

    // push all window segments into kernels
    while (first <= last) {
      auto segment = window_segments[first][second];
      auto limit =
          first == last ? win_max_index->second : window_segments[first].size() - 1;
      auto limit_segment = window_segments[first][limit];
      auto segment_len = limit_segment.offset + limit_segment.length - segment.offset;

      ExecSpan run_batch =
          ExecSpan(window_batches[first].Slice(segment.offset, segment_len));
      RETURN_NOT_OK(Consume(ctx, run_batch));
      second = 0;
      first++;
    }

    // prepare output batch
    ExecBatch batch{{}, grouper->num_groups()};
    batch.values.resize(agg_kernels_.size() + key_field_ids_.size() +
                        segment_key_field_ids_.size());

    // First, insert segment keys
    Datum& segment_val = window_segment_values[current_index.first][current_index.second];
    std::vector<Datum> vec;
    vec.emplace_back(segment_val);
    PlaceFields(batch, /*base=*/0, vec);

    // Followed by keys
    ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, grouper->GetUniques());
    std::move(out_keys.values.begin(), out_keys.values.end(),
              batch.values.begin() + segment_key_field_ids_.size());

    RETURN_NOT_OK(Finalize(ctx, batch, base));

    // send output
    ARROW_RETURN_NOT_OK(output_->InputReceived(this, std::move(batch)));
    total_output_batches_++;
    current_done = true;

    // check if we can purge something
    if (win_min_index->first > 0 && current_index.first > 0) {
      window_batches.erase(window_batches.begin());
      window_segment_values.erase(window_segment_values.begin());
      window_segments.erase(window_segments.begin());
      current_index.first--;
    }
  }
  return Status::OK();
}

}  // namespace aggregate
}  // namespace acero
}  // namespace arrow
