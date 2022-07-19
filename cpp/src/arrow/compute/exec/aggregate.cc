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

#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace compute {
namespace internal {

Result<std::vector<const HashAggregateKernel*>> GetKernels(
    ExecContext* ctx, const std::vector<Aggregate>& aggregates,
    const std::vector<TypeHolder>& in_types) {
  if (aggregates.size() != in_types.size()) {
    return Status::Invalid(aggregates.size(), " aggregate functions were specified but ",
                           in_types.size(), " arguments were provided.");
  }

  std::vector<const HashAggregateKernel*> kernels(in_types.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto function,
                          ctx->func_registry()->GetFunction(aggregates[i].function));
    ARROW_ASSIGN_OR_RAISE(const Kernel* kernel,
                          function->DispatchExact({in_types[i], uint32()}));
    kernels[i] = static_cast<const HashAggregateKernel*>(kernel);
  }
  return kernels;
}

Result<std::vector<std::unique_ptr<KernelState>>> InitKernels(
    const std::vector<const HashAggregateKernel*>& kernels, ExecContext* ctx,
    const std::vector<Aggregate>& aggregates, const std::vector<TypeHolder>& in_types) {
  std::vector<std::unique_ptr<KernelState>> states(kernels.size());

  for (size_t i = 0; i < aggregates.size(); ++i) {
    const FunctionOptions* options =
        arrow::internal::checked_cast<const FunctionOptions*>(
            aggregates[i].options.get());

    if (options == nullptr) {
      // use known default options for the named function if possible
      auto maybe_function = ctx->func_registry()->GetFunction(aggregates[i].function);
      if (maybe_function.ok()) {
        options = maybe_function.ValueOrDie()->default_options();
      }
    }

    KernelContext kernel_ctx{ctx};
    ARROW_ASSIGN_OR_RAISE(states[i],
                          kernels[i]->init(&kernel_ctx, KernelInitArgs{kernels[i],
                                                                       {
                                                                           in_types[i],
                                                                           uint32(),
                                                                       },
                                                                       options}));
  }

  return std::move(states);
}

Result<FieldVector> ResolveKernels(
    const std::vector<Aggregate>& aggregates,
    const std::vector<const HashAggregateKernel*>& kernels,
    const std::vector<std::unique_ptr<KernelState>>& states, ExecContext* ctx,
    const std::vector<TypeHolder>& types) {
  FieldVector fields(types.size());

  for (size_t i = 0; i < kernels.size(); ++i) {
    KernelContext kernel_ctx{ctx};
    kernel_ctx.SetState(states[i].get());

    ARROW_ASSIGN_OR_RAISE(auto type, kernels[i]->signature->out_type().Resolve(
                                         &kernel_ctx, {types[i], uint32()}));
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
  if (!arguments.empty()) {
    ARROW_ASSIGN_OR_RAISE(args_batch, ExecBatch::Make(arguments));

    // Construct and initialize HashAggregateKernels
    auto argument_types = args_batch.GetTypes();

    ARROW_ASSIGN_OR_RAISE(kernels, GetKernels(ctx, aggregates, argument_types));

    states.resize(task_group->parallelism());
    for (auto& state : states) {
      ARROW_ASSIGN_OR_RAISE(state, InitKernels(kernels, ctx, aggregates, argument_types));
    }

    ARROW_ASSIGN_OR_RAISE(
        out_fields, ResolveKernels(aggregates, kernels, states[0], ctx, argument_types));

    RETURN_NOT_OK(argument_iterator.Init(args_batch, ctx->exec_chunksize()));
  }

  // Construct Groupers
  ARROW_ASSIGN_OR_RAISE(ExecBatch keys_batch, ExecBatch::Make(keys));
  auto key_types = keys_batch.GetTypes();

  std::vector<std::unique_ptr<Grouper>> groupers(task_group->parallelism());
  for (auto& grouper : groupers) {
    ARROW_ASSIGN_OR_RAISE(grouper, Grouper::Make(key_types, ctx));
  }

  std::mutex mutex;
  std::unordered_map<std::thread::id, size_t> thread_ids;

  int i = 0;
  for (const TypeHolder& key_type : key_types) {
    out_fields.push_back(field("key_" + std::to_string(i++), key_type.GetSharedPtr()));
  }

  ExecSpanIterator key_iterator;
  RETURN_NOT_OK(key_iterator.Init(keys_batch, ctx->exec_chunksize()));

  // start "streaming" execution
  ExecSpan key_batch, argument_batch;
  while ((arguments.empty() || argument_iterator.Next(&argument_batch)) &&
         key_iterator.Next(&key_batch)) {
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
      for (size_t i = 0; i < kernels.size(); ++i) {
        KernelContext batch_ctx{ctx};
        batch_ctx.SetState(states[thread_index][i].get());
        ExecSpan kernel_batch({argument_batch[i], *id_batch.array()},
                              argument_batch.length);
        RETURN_NOT_OK(kernels[i]->resize(&batch_ctx, grouper->num_groups()));
        RETURN_NOT_OK(kernels[i]->consume(&batch_ctx, kernel_batch));
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
  ArrayDataVector out_data(arguments.size() + keys.size());
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

  int64_t length = out_data[0]->length;
  return ArrayData::Make(struct_(std::move(out_fields)), length,
                         {/*null_bitmap=*/nullptr}, std::move(out_data),
                         /*null_count=*/0);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
