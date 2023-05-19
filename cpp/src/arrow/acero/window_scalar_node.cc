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
#include <thread>
#include <unordered_set>

#include "arrow/acero/aggregate_internal.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/util.h"
#include "arrow/array/builder_primitive.h"
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
 * - if the window has a left boundary >0 it outputs partial window results at the
 * beginning of the stream
 * - if the window has a right boundary >0 it outputs partial window results at the end of
 * the stream
 * - time complexity: O(num_input_rows x window_size)
 * - space complexity: O(window_size)
 */

Result<AggregateNodeArgs<ScalarAggregateKernel>> WindowScalarNode::MakeAggregateNodeArgs(
    const std::shared_ptr<Schema>& input_schema, const std::vector<FieldRef>& keys,
    const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggs,
    ExecContext* exec_ctx, size_t concurrency,
    const std::optional<WindowAggregateArgs> window_args) {
  // Copy (need to modify options pointer below)
  std::vector<Aggregate> aggregates(aggs);
  std::vector<int> segment_field_ids(segment_keys.size());
  std::vector<TypeHolder> segment_key_types(segment_keys.size());
  for (size_t i = 0; i < segment_keys.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(FieldPath match, segment_keys[i].FindOne(*input_schema));
    if (match.indices().size() > 1) {
      // ARROW-18369: Support nested references as segment ids
      return Status::Invalid("Nested references cannot be used as segment ids");
    }
    segment_field_ids[i] = match[0];
    segment_key_types[i] = input_schema->field(match[0])->type().get();
  }

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<RowSegmenter> segmenter,
                        RowSegmenter::Make(std::move(segment_key_types),
                                           /*nullable_keys=*/false, exec_ctx));

  std::vector<std::vector<TypeHolder>> kernel_intypes(aggregates.size());
  std::vector<const ScalarAggregateKernel*> kernels(aggregates.size());
  std::vector<std::vector<std::unique_ptr<KernelState>>> states(kernels.size());
  FieldVector fields(kernels.size() + segment_keys.size());

  // Output the segment keys first, followed by the aggregates
  for (size_t i = 0; i < segment_keys.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(fields[i], segment_keys[i].GetOne(*input_schema));
  }

  std::vector<std::vector<int>> target_fieldsets(kernels.size());
  std::size_t base = segment_keys.size();
  for (size_t i = 0; i < kernels.size(); ++i) {
    const auto& target_fieldset = aggregates[i].target;
    for (const auto& target : target_fieldset) {
      ARROW_ASSIGN_OR_RAISE(auto match, FieldRef(target).FindOne(*input_schema));
      target_fieldsets[i].push_back(match[0]);
    }

    ARROW_ASSIGN_OR_RAISE(auto function,
                          exec_ctx->func_registry()->GetFunction(aggregates[i].function));

    if (function->kind() != Function::SCALAR_AGGREGATE) {
      if (function->kind() == Function::HASH_AGGREGATE) {
        return Status::Invalid("The provided function (", aggregates[i].function,
                               ") is a hash aggregate function.  Since there are no "
                               "keys to group by, a scalar aggregate function was "
                               "expected (normally these do not start with hash_)");
      }
      return Status::Invalid("The provided function(", aggregates[i].function,
                             ") is not an aggregate function");
    }

    std::vector<TypeHolder> in_types;
    for (const auto& target : target_fieldsets[i]) {
      in_types.emplace_back(input_schema->field(target)->type().get());
    }
    kernel_intypes[i] = in_types;
    ARROW_ASSIGN_OR_RAISE(const Kernel* kernel,
                          function->DispatchExact(kernel_intypes[i]));
    const auto* agg_kernel = static_cast<const ScalarAggregateKernel*>(kernel);

    kernels[i] = agg_kernel;

    if (aggregates[i].options == nullptr) {
      DCHECK(!function->doc().options_required);
      const auto* default_options = function->default_options();
      if (default_options) {
        aggregates[i].options = default_options->Copy();
      }
    }

    KernelContext kernel_ctx{exec_ctx};
    states[i].resize(concurrency);
    RETURN_NOT_OK(Kernel::InitAll(
        &kernel_ctx,
        KernelInitArgs{kernels[i], kernel_intypes[i], aggregates[i].options.get()},
        &states[i]));

    // pick one to resolve the kernel signature
    kernel_ctx.SetState(states[i][0].get());
    ARROW_ASSIGN_OR_RAISE(auto out_type, kernels[i]->signature->out_type().Resolve(
                                             &kernel_ctx, kernel_intypes[i]));

    fields[base + i] = field(aggregates[i].name, out_type.GetSharedPtr());
  }

  return AggregateNodeArgs<ScalarAggregateKernel>{
      schema(std::move(fields)),
      /*grouping_key_field_ids=*/{}, std::move(segment_field_ids),
      std::move(segmenter),          std::move(target_fieldsets),
      std::move(aggregates),         std::move(kernels),
      std::move(kernel_intypes),     {},
      std::move(window_args)};
}

Status WindowScalarNode::InitStatesVector(
    std::vector<std::vector<std::unique_ptr<KernelState>>>& states,
    KernelContext& kernel_ctx) {
  states.resize(kernels_.size());
  for (size_t i = 0; i < kernels_.size(); ++i) {
    states[i].resize(1);
    RETURN_NOT_OK(Kernel::InitAll(
        &kernel_ctx,
        KernelInitArgs{kernels_[i], kernel_intypes_[i], aggs_[i].options.get()},
        &states[i]));
  }
  return Status::OK();
}

Result<ExecNode*> WindowScalarNode::Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                         const ExecNodeOptions& options) {
  RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "WindowScalarNode"));

  const auto& aggregate_options = checked_cast<const AggregateNodeOptions&>(options);
  auto aggregates = aggregate_options.aggregates;
  const auto& keys = aggregate_options.keys;
  const auto& segment_keys = aggregate_options.segment_keys;
  const auto concurrency = plan->query_context()->max_concurrency();
  // We can't use concurrency == 1 because that include I/O concurrency
  const bool is_cpu_parallel = plan->query_context()->executor()->GetCapacity() > 1;

  if (keys.size() > 0) {
    return Status::Invalid("Scalar aggregation with some key");
  }
  if (!aggregate_options.window_args) {
    return Status::Invalid("Scalar window aggregation without window definition");
  }
  if (segment_keys.size() != 1) {
    return Status::Invalid("Scalar window aggregation needs exactly one segment key");
  }
  if (is_cpu_parallel) {
    return Status::NotImplemented(
        "Scalar window aggregator in multiple threaded execution");
  }

  const auto& input_schema = inputs[0]->output_schema();
  auto exec_ctx = plan->query_context()->exec_context();

  ARROW_ASSIGN_OR_RAISE(
      auto args,
      MakeAggregateNodeArgs(input_schema, keys, segment_keys, aggregates, exec_ctx,
                            concurrency, aggregate_options.window_args));

  return plan->EmplaceNode<WindowScalarNode>(
      plan, std::move(inputs), std::move(args.output_schema), std::move(args.segmenter),
      std::move(args.segment_key_field_ids), std::move(args.target_fieldsets),
      std::move(args.aggregates), std::move(args.kernels), std::move(args.kernel_intypes),
      std::move(args.window_args));
}

// Extract segment value and queue up batches, then call OutputResults
Status WindowScalarNode::InputReceived(ExecNode* input, ExecBatch batch) {
  auto scope = TraceInputReceived(batch);
  DCHECK_EQ(input, inputs_[0]);

  // compute segments
  int64_t offset = 0;
  ARROW_ASSIGN_OR_RAISE(auto segment_exec_batch, batch.SelectValues(segment_field_ids_));
  ExecSpan segment_batch(segment_exec_batch);

  std::vector<compute::Segment> segments;
  std::vector<Datum> segment_values;

  while (true) {
    // for each input segment
    ARROW_ASSIGN_OR_RAISE(compute::Segment segment,
                          segmenter_->GetNextSegment(segment_batch, offset));
    if (segment.offset >= segment_batch.length) break;  // condition of no-next-segment

    // Scalar window aggregation wants a single row per segment value
    if (segment.length != 1 || (segment.extends && segment.offset > 0)) {
      return Status::Invalid(
          "Window aggregation segments must contain a single row per segment value");
    }

    // extract (the single) segment values
    auto exec_batch = batch.Slice(segment.offset, segment.length);
    ARROW_ASSIGN_OR_RAISE(std::vector<Datum> segment_val,
                          ExtractValues(exec_batch, segment_field_ids_));

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

Status WindowScalarNode::InputFinished(ExecNode* input, int total_batches) {
  auto scope = TraceFinish();
  EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
  DCHECK_EQ(input, inputs_[0]);
  ARROW_RETURN_NOT_OK(OutputResult(true));
  ARROW_RETURN_NOT_OK(output_->InputFinished(this, total_output_batches_));
  return Status::OK();
}

std::string WindowScalarNode::ToStringExtra(int indent) const {
  std::stringstream ss;
  const auto input_schema = inputs_[0]->output_schema();
  AggregatesToString(&ss, *input_schema, aggs_, target_fieldsets_);
  return ss.str();
}

// find the leftmost index for the window:
// since data is ordered bty segment key we stop at the first one which makes the bound
// comparison true
Result<std::pair<uint32_t, uint32_t>> WindowScalarNode::FindLeftIndex(const Datum& data,
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
Result<std::pair<uint32_t, uint32_t>> WindowScalarNode::FindRightIndex(const Datum& data,
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
Status WindowScalarNode::Consume(
    KernelContext ctx, std::vector<std::vector<std::unique_ptr<KernelState>>>& states_,
    ExecSpan& run_batch) {
  for (size_t i = 0; i < kernels_.size(); ++i) {
    arrow::util::tracing::Span span;
    START_COMPUTE_SPAN(span, aggs_[i].function,
                       {{"function.name", aggs_[i].function},
                        {"function.options",
                         aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                        {"function.kind", std::string(kind_name()) + "::Consume"}});
    ctx.SetState(states_[i][0].get());

    std::vector<ExecValue> column_values;
    for (const int field : target_fieldsets_[i]) {
      column_values.push_back(run_batch.values[field]);
    }
    ExecSpan column_batch{std::move(column_values), run_batch.length};
    RETURN_NOT_OK(kernels_[i]->consume(&ctx, column_batch));
  }
  return Status::OK();
}

// finalize all kernels and add aggregate values to output batch
Status WindowScalarNode::Finalize(
    KernelContext ctx, std::vector<std::vector<std::unique_ptr<KernelState>>>& states_,
    ExecBatch& batch, std::size_t base) {
  for (size_t i = 0; i < kernels_.size(); ++i) {
    arrow::util::tracing::Span span;
    START_COMPUTE_SPAN(span, aggs_[i].function,
                       {{"function.name", aggs_[i].function},
                        {"function.options",
                         aggs_[i].options ? aggs_[i].options->ToString() : "<NULLPTR>"},
                        {"function.kind", std::string(kind_name()) + "::Finalize"}});
    ctx.SetState(states_[i][0].get());
    RETURN_NOT_OK(kernels_[i]->finalize(&ctx, &batch.values[base + i]));
    states_[i][0].reset();
  }
  return Status::OK();
}

// Output as many complete windows as possible
// if flush output the tail of the operation (incomplete windows)
Status WindowScalarNode::OutputResult(bool flush) {
  ExecContext* exec_ctx = plan()->query_context()->exec_context();
  KernelContext ctx{exec_ctx};
  std::vector<std::vector<std::unique_ptr<KernelState>>> states_;
  std::size_t base = segment_field_ids_.size();

  // try to output all possible data
  while (true) {
    if (current_done) {
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
      current_datum = window_segment_values[current_index.first][current_index.second];
      current_done = false;
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

    // prepare output batch
    ExecBatch batch{{}, 1};
    batch.values.resize(kernels_.size() + segment_field_ids_.size());

    // First, insert segment keys
    Datum& segment_val = window_segment_values[current_index.first][current_index.second];
    std::vector<Datum> vec;
    vec.emplace_back(segment_val);
    PlaceFields(batch, /*base=*/0, vec);

    // run each kernel

    RETURN_NOT_OK(InitStatesVector(states_, ctx));

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
      RETURN_NOT_OK(Consume(ctx, states_, run_batch));
      second = 0;
      first++;
    }

    RETURN_NOT_OK(Finalize(ctx, states_, batch, base));

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
