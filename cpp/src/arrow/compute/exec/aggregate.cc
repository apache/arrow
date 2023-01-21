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

#include "arrow/compute/exec/aggregate.h"

#include <mutex>
#include <thread>
#include <unordered_map>

#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"

namespace arrow {

using internal::ToChars;

namespace compute {
namespace internal {

namespace {

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

}  // namespace

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

Result<Datum> GroupBy(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
                      const std::vector<Aggregate>& aggregates, bool use_threads,
                      ExecContext* ctx) {
  auto task_group =
      use_threads
          ? arrow::internal::TaskGroup::MakeThreaded(arrow::internal::GetCpuThreadPool())
          : arrow::internal::TaskGroup::MakeSerial();

  std::vector<const HashAggregateKernel*> kernels;
  std::vector<std::vector<std::unique_ptr<KernelState>>> states;
  FieldVector out_fields;

  using arrow::compute::detail::ExecSpanIterator;
  ExecSpanIterator argument_iterator;

  ExecBatch args_batch;
  Result<int64_t> inferred_length = ExecBatch::InferLength(arguments);
  if (!inferred_length.ok()) {
    inferred_length = ExecBatch::InferLength(keys);
  }
  ARROW_ASSIGN_OR_RAISE(const int64_t length, std::move(inferred_length));
  if (!aggregates.empty()) {
    ARROW_ASSIGN_OR_RAISE(args_batch, ExecBatch::Make(arguments, length));

    // Construct and initialize HashAggregateKernels
    std::vector<std::vector<TypeHolder>> aggs_argument_types;
    aggs_argument_types.reserve(aggregates.size());
    size_t i = 0;
    for (const auto& aggregate : aggregates) {
      auto& agg_types = aggs_argument_types.emplace_back();
      const size_t num_needed = aggregate.target.size();
      for (size_t j = 0; j < num_needed && i < arguments.size(); j++, i++) {
        agg_types.emplace_back(arguments[i].type());
      }
      if (agg_types.size() != num_needed) {
        return Status::Invalid("Not enough arguments specified to aggregate functions.");
      }
    }
    DCHECK_EQ(aggs_argument_types.size(), aggregates.size());
    if (i != arguments.size()) {
      return Status::Invalid("Aggregate functions expect exactly ", i, " arguments, but ",
                             arguments.size(), " were specified.");
    }

    ARROW_ASSIGN_OR_RAISE(kernels, GetKernels(ctx, aggregates, aggs_argument_types));

    states.resize(task_group->parallelism());
    for (auto& state : states) {
      ARROW_ASSIGN_OR_RAISE(state,
                            InitKernels(kernels, ctx, aggregates, aggs_argument_types));
    }

    ARROW_ASSIGN_OR_RAISE(out_fields, ResolveKernels(aggregates, kernels, states[0], ctx,
                                                     aggs_argument_types));

    RETURN_NOT_OK(argument_iterator.Init(args_batch, ctx->exec_chunksize()));
  }

  // Construct Groupers
  ARROW_ASSIGN_OR_RAISE(ExecBatch keys_batch, ExecBatch::Make(keys, length));
  auto key_types = keys_batch.GetTypes();

  std::vector<std::unique_ptr<Grouper>> groupers(task_group->parallelism());
  for (auto& grouper : groupers) {
    ARROW_ASSIGN_OR_RAISE(grouper, Grouper::Make(key_types, ctx));
  }

  std::mutex mutex;
  std::unordered_map<std::thread::id, size_t> thread_ids;

  int i = 0;
  for (const TypeHolder& key_type : key_types) {
    out_fields.push_back(field("key_" + ToChars(i++), key_type.GetSharedPtr()));
  }

  ExecSpanIterator key_iterator;
  RETURN_NOT_OK(key_iterator.Init(keys_batch, ctx->exec_chunksize()));

  // start "streaming" execution
  ExecSpan key_batch, argument_batch;
  while ((arguments.empty() || argument_iterator.Next(&argument_batch)) &&
         key_iterator.Next(&key_batch)) {
    if (arguments.empty()) {
      // A value-less argument_batch should still have a valid length
      argument_batch.length = key_batch.length;
    }
    if (key_batch.length == 0) continue;

    task_group->Append([&, key_batch, argument_batch] {
      size_t thread_index;
      {
        std::unique_lock<std::mutex> lock(mutex);
        auto it = thread_ids.emplace(std::this_thread::get_id(), thread_ids.size()).first;
        thread_index = it->second;
        DCHECK_LT(static_cast<int>(thread_index), task_group->parallelism());
      }

      auto grouper = groupers[thread_index].get();

      // compute a batch of group ids
      ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper->Consume(key_batch));

      // consume group ids with HashAggregateKernels
      for (size_t k = 0, arg_idx = 0; k < kernels.size(); ++k) {
        const auto* kernel = kernels[k];
        KernelContext batch_ctx{ctx};
        batch_ctx.SetState(states[thread_index][k].get());

        const size_t kernel_num_args = kernel->signature->in_types().size();
        DCHECK_GT(kernel_num_args, 0);

        std::vector<ExecValue> kernel_args;
        for (size_t i = 0; i + 1 < kernel_num_args; i++, arg_idx++) {
          kernel_args.push_back(argument_batch[arg_idx]);
        }
        kernel_args.emplace_back(*id_batch.array());

        ExecSpan kernel_batch(std::move(kernel_args), argument_batch.length);
        RETURN_NOT_OK(kernel->resize(&batch_ctx, grouper->num_groups()));
        RETURN_NOT_OK(kernel->consume(&batch_ctx, kernel_batch));
      }

      return Status::OK();
    });
  }

  RETURN_NOT_OK(task_group->Finish());

  // Merge if necessary
  for (size_t thread_index = 1; thread_index < thread_ids.size(); ++thread_index) {
    ARROW_ASSIGN_OR_RAISE(ExecBatch other_keys, groupers[thread_index]->GetUniques());
    ARROW_ASSIGN_OR_RAISE(Datum transposition,
                          groupers[0]->Consume(ExecSpan(other_keys)));
    groupers[thread_index].reset();

    for (size_t idx = 0; idx < kernels.size(); ++idx) {
      KernelContext batch_ctx{ctx};
      batch_ctx.SetState(states[0][idx].get());

      RETURN_NOT_OK(kernels[idx]->resize(&batch_ctx, groupers[0]->num_groups()));
      RETURN_NOT_OK(kernels[idx]->merge(&batch_ctx, std::move(*states[thread_index][idx]),
                                        *transposition.array()));
      states[thread_index][idx].reset();
    }
  }

  // Finalize output
  ArrayDataVector out_data(kernels.size() + keys.size());
  auto it = out_data.begin();

  for (size_t idx = 0; idx < kernels.size(); ++idx) {
    KernelContext batch_ctx{ctx};
    batch_ctx.SetState(states[0][idx].get());
    Datum out;
    RETURN_NOT_OK(kernels[idx]->finalize(&batch_ctx, &out));
    *it++ = out.array();
  }

  ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, groupers[0]->GetUniques());
  for (const auto& key : out_keys.values) {
    *it++ = key.array();
  }

  const int64_t out_length = out_data[0]->length;
  return ArrayData::Make(struct_(std::move(out_fields)), out_length,
                         {/*null_bitmap=*/nullptr}, std::move(out_data),
                         /*null_count=*/0);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
