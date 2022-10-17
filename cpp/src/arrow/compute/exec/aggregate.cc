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
#include <sstream>
#include <thread>
#include <unordered_map>

#include "arrow/array/concatenate.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace compute {
namespace internal {

using arrow::compute::detail::ExecSpanIterator;

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

namespace {

template <typename T>
inline std::string ToString(const std::vector<T>& v) {
  std::stringstream s;
  s << '[';
  for (size_t i = 0; i < v.size(); i++) {
    if (i != 0) s << ',';
    s << v[i];
  }
  s << ']';
  return s.str();
}

int64_t FindLength(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
                   const std::vector<Datum>& segment_keys) {
  int64_t length = -1;
  for (const auto& datums : {arguments, keys, segment_keys}) {
    for (const auto& datum : datums) {
      if (datum.is_scalar()) {
        // do nothing
      } else if (datum.is_array() || datum.is_chunked_array()) {
        int64_t datum_length =
            datum.is_array() ? datum.array()->length : datum.chunked_array()->length();
        if (length == -1) {
          length = datum_length;
        } else if (length != datum_length) {
          return -1;
        }
      } else {
        ARROW_DCHECK(false);
      }
    }
  }
  return length;
}

class GroupByProcess {
 public:
  struct BatchInfo {
    ExecBatch args_batch;
    std::vector<TypeHolder> argument_types;
    ExecBatch keys_batch;
    std::vector<TypeHolder> key_types;
    ExecBatch segment_keys_batch;
    std::vector<TypeHolder> segment_key_types;

    static Result<BatchInfo> Make(const std::vector<Datum>& arguments,
                                  const std::vector<Datum>& keys,
                                  const std::vector<Datum>& segment_keys) {
      int64_t batch_length = FindLength(arguments, keys, segment_keys);

      ARROW_ASSIGN_OR_RAISE(auto args_batch, ExecBatch::Make(arguments, batch_length));
      auto argument_types = args_batch.GetTypes();

      ARROW_ASSIGN_OR_RAISE(auto keys_batch, ExecBatch::Make(keys, batch_length));
      auto key_types = keys_batch.GetTypes();

      ARROW_ASSIGN_OR_RAISE(auto segment_keys_batch,
                            ExecBatch::Make(segment_keys, batch_length));
      auto segment_key_types = segment_keys_batch.GetTypes();

      return BatchInfo{std::move(args_batch),         std::move(argument_types),
                       std::move(keys_batch),         std::move(key_types),
                       std::move(segment_keys_batch), std::move(segment_key_types)};
    }

    BatchInfo Slice(int64_t offset, int64_t length) const {
      return BatchInfo{args_batch.Slice(offset, length),         argument_types,
                       keys_batch.Slice(offset, length),         key_types,
                       segment_keys_batch.Slice(offset, length), segment_key_types};
    }
  };

  struct StateInfo {
    GroupByProcess& process;
    std::shared_ptr<arrow::internal::TaskGroup> task_group;
    std::vector<std::unique_ptr<Grouper>> groupers;
    std::vector<const HashAggregateKernel*> kernels;
    std::vector<std::vector<std::unique_ptr<KernelState>>> states;
    FieldVector out_fields;
    ExecSpanIterator argument_iterator;
    ExecSpanIterator key_iterator;
    ScalarVector segment_keys;

    explicit StateInfo(GroupByProcess& process) : process(process) {}

    Status Init() {
      const std::vector<TypeHolder>& argument_types = process.argument_types;
      const std::vector<TypeHolder>& key_types = process.key_types;
      const std::vector<Aggregate>& aggregates = process.aggregates;
      ExecContext* ctx = process.ctx;
      const FieldVector& key_fields = process.key_fields;

      task_group = process.use_threads ? arrow::internal::TaskGroup::MakeThreaded(
                                             arrow::internal::GetCpuThreadPool())
                                       : arrow::internal::TaskGroup::MakeSerial();

      groupers.resize(task_group->parallelism());
      for (auto& grouper : groupers) {
        ARROW_ASSIGN_OR_RAISE(grouper, Grouper::Make(key_types, ctx));
      }

      if (!argument_types.empty()) {
        // Construct and initialize HashAggregateKernels
        ARROW_ASSIGN_OR_RAISE(kernels, GetKernels(ctx, aggregates, argument_types));

        states.resize(task_group->parallelism());
        for (auto& state : states) {
          ARROW_ASSIGN_OR_RAISE(state,
                                InitKernels(kernels, ctx, aggregates, argument_types));
        }

        ARROW_ASSIGN_OR_RAISE(out_fields, ResolveKernels(aggregates, kernels, states[0],
                                                         ctx, argument_types));
      } else {
        out_fields = {};
      }
      out_fields.insert(out_fields.end(), key_fields.begin(), key_fields.end());

      return Status::OK();
    }

    Status Consume(const BatchInfo& batch_info) {
      const std::vector<TypeHolder>& argument_types = process.argument_types;
      ExecContext* ctx = process.ctx;

      const ExecBatch& args_batch = batch_info.args_batch;
      const ExecBatch& keys_batch = batch_info.keys_batch;
      const ExecBatch& segment_keys_batch = batch_info.segment_keys_batch;

      if (segment_keys_batch.length == 0) {
        return Status::OK();
      }
      segment_keys = {};
      for (auto value : segment_keys_batch.values) {
        if (value.is_scalar()) {
          segment_keys.push_back(value.scalar());
        } else if (value.is_array()) {
          ARROW_ASSIGN_OR_RAISE(auto scalar, value.make_array()->GetScalar(0));
          segment_keys.push_back(scalar);
        } else if (value.is_chunked_array()) {
          ARROW_ASSIGN_OR_RAISE(auto scalar, value.chunked_array()->GetScalar(0));
          segment_keys.push_back(scalar);
        } else {
          return Status::Invalid("consuming an invalid segment key type ", *value.type());
        }
      }

      if (!argument_types.empty()) {
        ARROW_RETURN_NOT_OK(argument_iterator.Init(args_batch, ctx->exec_chunksize()));
      }
      ARROW_RETURN_NOT_OK(key_iterator.Init(keys_batch, ctx->exec_chunksize()));

      std::mutex mutex;
      std::unordered_map<std::thread::id, size_t> thread_ids;

      // start "streaming" execution
      ExecSpan key_batch, argument_batch;
      while ((argument_types.empty() || argument_iterator.Next(&argument_batch)) &&
             key_iterator.Next(&key_batch)) {
        if (key_batch.length == 0) continue;

        task_group->Append([&, key_batch, argument_batch] {
          size_t thread_index;
          {
            std::unique_lock<std::mutex> lock(mutex);
            auto it =
                thread_ids.emplace(std::this_thread::get_id(), thread_ids.size()).first;
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
            ARROW_RETURN_NOT_OK(kernels[i]->resize(&batch_ctx, grouper->num_groups()));
            ARROW_RETURN_NOT_OK(kernels[i]->consume(&batch_ctx, kernel_batch));
          }

          return Status::OK();
        });
      }

      ARROW_RETURN_NOT_OK(task_group->Finish());
      return Status::OK();
    }

    Status Merge() {
      ExecContext* ctx = process.ctx;
      size_t num_threads = static_cast<size_t>(task_group->parallelism());
      for (size_t thread_index = 1; thread_index < num_threads; ++thread_index) {
        ARROW_ASSIGN_OR_RAISE(ExecBatch other_keys, groupers[thread_index]->GetUniques());
        ARROW_ASSIGN_OR_RAISE(Datum transposition,
                              groupers[0]->Consume(ExecSpan(other_keys)));
        groupers[thread_index].reset();

        for (size_t idx = 0; idx < kernels.size(); ++idx) {
          KernelContext batch_ctx{ctx};
          batch_ctx.SetState(states[0][idx].get());

          ARROW_RETURN_NOT_OK(
              kernels[idx]->resize(&batch_ctx, groupers[0]->num_groups()));
          ARROW_RETURN_NOT_OK(kernels[idx]->merge(
              &batch_ctx, std::move(*states[thread_index][idx]), *transposition.array()));
          states[thread_index][idx].reset();
        }
      }
      return Status::OK();
    }

    Result<Datum> Finalize() {
      const std::vector<TypeHolder>& argument_types = process.argument_types;
      const std::vector<TypeHolder>& key_types = process.key_types;
      const std::vector<TypeHolder>& segment_key_types = process.segment_key_types;
      ExecContext* ctx = process.ctx;

      ArrayDataVector out_data(argument_types.size() + key_types.size() +
                               segment_key_types.size());
      auto it = out_data.begin();

      for (size_t idx = 0; idx < kernels.size(); ++idx) {
        KernelContext batch_ctx{ctx};
        batch_ctx.SetState(states[0][idx].get());
        Datum out;
        ARROW_RETURN_NOT_OK(kernels[idx]->finalize(&batch_ctx, &out));
        *it++ = out.array();
      }

      ARROW_ASSIGN_OR_RAISE(ExecBatch out_keys, groupers[0]->GetUniques());
      for (const auto& key : out_keys.values) {
        *it++ = key.array();
      }

      int64_t length = out_data[0]->length;
      for (const auto& key : segment_keys) {
        ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(*key, length));
        *it++ = array->data();
      }

      return ArrayData::Make(struct_(std::move(out_fields)), length,
                             {/*null_bitmap=*/nullptr}, std::move(out_data),
                             /*null_count=*/0);
    }
  };

  ARROW_DISALLOW_COPY_AND_ASSIGN(GroupByProcess);

  GroupByProcess(std::vector<TypeHolder> argument_types,
                 std::vector<TypeHolder> key_types,
                 std::vector<TypeHolder> segment_key_types,
                 const std::vector<Aggregate>& aggregates,
                 std::unique_ptr<GroupingSegmenter> segmenter, bool use_threads,
                 ExecContext* ctx)
      : argument_types(argument_types),
        key_types(key_types),
        segment_key_types(segment_key_types),
        aggregates(aggregates),
        segmenter(std::move(segmenter)),
        use_threads(use_threads),
        ctx(ctx),
        key_fields(),
        state_info(*this) {
    int i = 0;
    for (auto types : {key_types, segment_key_types}) {
      for (const TypeHolder& type : types) {
        key_fields.push_back(field("key_" + std::to_string(i++), type.GetSharedPtr()));
      }
    }
  }

  static Result<std::unique_ptr<GroupByProcess>> Make(
      const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
      const std::vector<Datum>& segment_keys, const std::vector<Aggregate>& aggregates,
      bool use_threads, ExecContext* ctx) {
    ARROW_ASSIGN_OR_RAISE(auto batch_info,
                          BatchInfo::Make(arguments, keys, segment_keys));
    std::vector<TypeHolder> segment_key_types_dup = batch_info.segment_key_types;
    ARROW_ASSIGN_OR_RAISE(auto segmenter,
                          GroupingSegmenter::Make(std::move(segment_key_types_dup), ctx));
    return std::make_unique<GroupByProcess>(
        std::move(batch_info.argument_types), std::move(batch_info.key_types),
        std::move(batch_info.segment_key_types), aggregates, std::move(segmenter),
        use_threads, ctx);
  }

  Status CheckTypes(const std::vector<TypeHolder>& expected_types,
                    const std::vector<TypeHolder>& actual_types,
                    const std::string& types_kind) {
    if (expected_types != actual_types) {
      return Status::Invalid("expected ", types_kind, " ", ToString(expected_types),
                             " but got ", ToString(actual_types));
    }
    return Status::OK();
  }

  Status CheckTypes(const BatchInfo& batch_info) {
    ARROW_RETURN_NOT_OK(
        CheckTypes(argument_types, batch_info.argument_types, "argument types"));
    ARROW_RETURN_NOT_OK(CheckTypes(key_types, batch_info.key_types, "key types"));
    ARROW_RETURN_NOT_OK(
        CheckTypes(segment_key_types, batch_info.segment_key_types, "segment key types"));
    return Status::OK();
  }

  Result<Datum> Run(const BatchInfo& batch_info) {
    ARROW_RETURN_NOT_OK(CheckTypes(batch_info));
    ARROW_RETURN_NOT_OK(state_info.Init());

    // Consume batch
    ARROW_RETURN_NOT_OK(state_info.Consume(batch_info));

    // Merge if necessary
    ARROW_RETURN_NOT_OK(state_info.Merge());

    // Finalize output
    return state_info.Finalize();
  }

  Status Run(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
             const std::vector<Datum>& segment_keys, GroupByCallback callback) {
    ARROW_ASSIGN_OR_RAISE(auto batch_info,
                          BatchInfo::Make(arguments, keys, segment_keys));
    ARROW_RETURN_NOT_OK(CheckTypes(batch_info));

    if (segment_keys.size() == 0) {
      // an optimized code-path - the code works correctly without it
      ARROW_ASSIGN_OR_RAISE(auto datum, Run(std::move(batch_info)));
      return callback(datum);
    }
    int64_t offset = 0;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(
          auto segment, segmenter->GetNextSegment(batch_info.segment_keys_batch, offset));
      if (segment.offset >= batch_info.segment_keys_batch.length) break;
      BatchInfo segment_batch_info = batch_info.Slice(segment.offset, segment.length);
      ARROW_ASSIGN_OR_RAISE(auto datum, Run(segment_batch_info));
      ARROW_RETURN_NOT_OK(callback(datum));
      offset = segment.offset + segment.length;
    }
    return Status::OK();
  }

  Result<Datum> Run(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
                    const std::vector<Datum>& segment_keys) {
    ArrayVector arrays;
    ARROW_RETURN_NOT_OK(Run(arguments, keys, segment_keys, [&arrays](const Datum& datum) {
      arrays.push_back(datum.make_array());
      return Status::OK();
    }));
    if (arrays.size() == 1) {
      return arrays[0];
    } else {
      return ChunkedArray::Make(arrays);
    }
  }

 private:
  const std::vector<TypeHolder> argument_types;
  const std::vector<TypeHolder> key_types;
  const std::vector<TypeHolder> segment_key_types;
  const std::vector<Aggregate>& aggregates;
  std::unique_ptr<GroupingSegmenter> segmenter;
  bool use_threads;
  ExecContext* ctx;
  FieldVector key_fields;
  StateInfo state_info;
};

}  // namespace

Result<Datum> GroupBy(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
                      const std::vector<Datum>& segment_keys,
                      const std::vector<Aggregate>& aggregates, bool use_threads,
                      ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(auto gbp, GroupByProcess::Make(arguments, keys, segment_keys,
                                                       aggregates, use_threads, ctx));
  return gbp->Run(arguments, keys, segment_keys);
}

Status GroupBy(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
               const std::vector<Datum>& segment_keys,
               const std::vector<Aggregate>& aggregates, GroupByCallback callback,
               bool use_threads, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(auto gbp, GroupByProcess::Make(arguments, keys, segment_keys,
                                                       aggregates, use_threads, ctx));
  return gbp->Run(arguments, keys, segment_keys, callback);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
