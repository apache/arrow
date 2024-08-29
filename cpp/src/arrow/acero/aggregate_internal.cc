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

#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_set>

#include "arrow/acero/aggregate_internal.h"
#include "arrow/acero/aggregate_node.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

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

std::vector<TypeHolder> ExtendWithGroupIdType(const std::vector<TypeHolder>& in_types) {
  std::vector<TypeHolder> aggr_in_types;
  aggr_in_types.reserve(in_types.size() + 1);
  aggr_in_types = in_types;
  aggr_in_types.emplace_back(uint32());
  return aggr_in_types;
}

Result<const HashAggregateKernel*> GetKernel(ExecContext* ctx, const Aggregate& aggregate,
                                             const std::vector<TypeHolder>& in_types) {
  const auto aggr_in_types = ExtendWithGroupIdType(in_types);
  ARROW_ASSIGN_OR_RAISE(auto function,
                        ctx->func_registry()->GetFunction(aggregate.function));
  if (function->kind() != Function::HASH_AGGREGATE) {
    if (function->kind() == Function::SCALAR_AGGREGATE) {
      return Status::Invalid("The provided function (", aggregate.function,
                             ") is a scalar aggregate function.  Since there are "
                             "keys to group by, a hash aggregate function was "
                             "expected (normally these start with hash_)");
    }
    return Status::Invalid("The provided function(", aggregate.function,
                           ") is not an aggregate function");
  }
  ARROW_ASSIGN_OR_RAISE(const Kernel* kernel, function->DispatchExact(aggr_in_types));
  return static_cast<const HashAggregateKernel*>(kernel);
}

Result<std::unique_ptr<KernelState>> InitKernel(const HashAggregateKernel* kernel,
                                                ExecContext* ctx,
                                                const Aggregate& aggregate,
                                                const std::vector<TypeHolder>& in_types) {
  const auto aggr_in_types = ExtendWithGroupIdType(in_types);

  KernelContext kernel_ctx{ctx};
  const auto* options =
      arrow::internal::checked_cast<const FunctionOptions*>(aggregate.options.get());
  if (options == nullptr) {
    // use known default options for the named function if possible
    auto maybe_function = ctx->func_registry()->GetFunction(aggregate.function);
    if (maybe_function.ok()) {
      options = maybe_function.ValueOrDie()->default_options();
    }
  }

  ARROW_ASSIGN_OR_RAISE(
      auto state,
      kernel->init(&kernel_ctx, KernelInitArgs{kernel, aggr_in_types, options}));
  return std::move(state);
}

Result<std::vector<const HashAggregateKernel*>> GetKernels(
    ExecContext* ctx, const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  if (aggregates.size() != in_types.size()) {
    return Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           in_types.size(), " arguments were provided.");
  }

  std::vector<const HashAggregateKernel*> kernels(in_types.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(kernels[i], GetKernel(ctx, aggregates[i], in_types[i]));
  }
  return kernels;
}

Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<Aggregate>& aggregates,
    const std::vector<std::vector<TypeHolder>>& in_types) {
  std::vector<std::unique_ptr<KernelState>> states(kernels.size());
  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(states[i],
                          InitKernel(kernels[i], ctx, aggregates[i], in_types[i]));
  }
  return std::move(states);
}

Result<FieldVector> ResolveKernels(
    const std::vector<Aggregate>& aggregates,
    const std::vector<const HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<KernelState>>& states, ExecContext* ctx,
    const std::vector<std::vector<TypeHolder>>& types) {
  FieldVector fields(types.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(states[i].get());

    const auto aggr_in_types = ExtendWithGroupIdType(types[i]);
    ARROW_ASSIGN_OR_RAISE(
        auto type, kernels[i]->signature->out_type().Resolve(&kernel_ctx, aggr_in_types));
    fields[i] = field(aggregates[i].function, type.GetSharedPtr());
  }
  return fields;
}

void AggregatesToString(std::stringstream* ss, const Schema& input_schema,
                        const std::vector<Aggregate>& aggs,
                        const std::vector<std::vector<int>>& target_fieldsets,
                        int indent) {
  *ss << "aggregates=[" << std::endl;
  for (size_t i = 0; i < aggs.size(); i++) {
    for (int j = 0; j < indent; ++j) *ss << "  ";
    *ss << '\t' << aggs[i].function << '(';
    const auto& target = target_fieldsets[i];
    if (target.size() == 0) {
      *ss << "*";
    } else {
      *ss << input_schema.field(target[0])->name();
      for (size_t k = 1; k < target.size(); k++) {
        *ss << ", " << input_schema.field(target[k])->name();
      }
    }
    if (aggs[i].options) {
      *ss << ", " << aggs[i].options->ToString();
    }
    *ss << ")," << std::endl;
  }
  for (int j = 0; j < indent; ++j) *ss << "  ";
  *ss << ']';
}

Status ExtractSegmenterValues(std::vector<Datum>* values_ptr,
                              const ExecBatch& input_batch,
                              const std::vector<int>& field_ids) {
  DCHECK_GT(input_batch.length, 0);
  std::vector<Datum>& values = *values_ptr;
  int64_t row = input_batch.length - 1;
  values.clear();
  values.resize(field_ids.size());
  for (size_t i = 0; i < field_ids.size(); i++) {
    const Datum& value = input_batch.values[field_ids[i]];
    if (value.is_scalar()) {
      values[i] = value;
    } else if (value.is_array()) {
      ARROW_ASSIGN_OR_RAISE(auto scalar, value.make_array()->GetScalar(row));
      values[i] = scalar;
    } else {
      DCHECK(false);
    }
  }
  return Status::OK();
}

void PlaceFields(ExecBatch& batch, size_t base, std::vector<Datum>& values) {
  DCHECK_LE(base + values.size(), batch.values.size());
  for (size_t i = 0; i < values.size(); i++) {
    batch.values[base + i] = values[i];
  }
}

Result<std::shared_ptr<Schema>> MakeOutputSchema(
    const std::shared_ptr<Schema>& input_schema, const std::vector<FieldRef>& keys,
    const std::vector<FieldRef>& segment_keys, const std::vector<Aggregate>& aggregates,
    ExecContext* exec_ctx) {
  if (keys.empty()) {
    ARROW_ASSIGN_OR_RAISE(auto args,
                          ScalarAggregateNode::MakeAggregateNodeArgs(
                              input_schema, keys, segment_keys, aggregates, exec_ctx,
                              /*concurrency=*/1, /*is_cpu_parallel=*/false));
    return std::move(args.output_schema);
  } else {
    ARROW_ASSIGN_OR_RAISE(auto args, GroupByNode::MakeAggregateNodeArgs(
                                         input_schema, keys, segment_keys, aggregates,
                                         exec_ctx, /*is_cpu_parallel=*/false));
    return std::move(args.output_schema);
  }
}

Result<std::vector<Datum>> ExtractValues(const ExecBatch& input_batch,
                                         const std::vector<int>& field_ids) {
  DCHECK_GT(input_batch.length, 0);
  std::vector<Datum> values;
  int64_t row = input_batch.length - 1;
  values.clear();
  values.resize(field_ids.size());
  for (size_t i = 0; i < field_ids.size(); i++) {
    const Datum& value = input_batch.values[field_ids[i]];
    if (value.is_scalar()) {
      values[i] = value;
    } else if (value.is_array()) {
      ARROW_ASSIGN_OR_RAISE(auto scalar, value.make_array()->GetScalar(row));
      values[i] = scalar;
    } else {
      DCHECK(false);
    }
  }
  return std::move(values);
}

}  // namespace aggregate

namespace internal {

void RegisterAggregateNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory(
      "aggregate",
      [](ExecPlan* plan, std::vector<ExecNode*> inputs,
         const ExecNodeOptions& options) -> Result<ExecNode*> {
        const auto& aggregate_options =
            checked_cast<const AggregateNodeOptions&>(options);

        if (aggregate_options.keys.empty()) {
          return aggregate::ScalarAggregateNode::Make(plan, std::move(inputs), options);
        }
        return aggregate::GroupByNode::Make(plan, std::move(inputs), options);
      }));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow
