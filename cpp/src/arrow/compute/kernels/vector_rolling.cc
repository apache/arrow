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

#include <functional>
#include <set>
#include <type_traits>
#include <utility>
#include "arrow/array/array_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/result.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {
namespace {
struct RollingOptionsWrapper : public OptionsWrapper<RollingOptions> {
  explicit RollingOptionsWrapper(RollingOptions options)
      : OptionsWrapper(std::move(options)) {}

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    auto options = checked_cast<const RollingOptions*>(args.options);
    if (!options) {
      return Status::Invalid(
          "Attempted to initialize KernelState from null FunctionOptions");
    }

    if (options->min_periods > options->window_length) {
      return Status::Invalid("min_periods must be less than or equal to window_length");
    }

    if (options->window_length <= 0) {
      return Status::Invalid("window_length must be greater than 0");
    }

    if (options->min_periods <= 0) {
      return Status::Invalid("min_periods must be greater than 0");
    }

    return std::make_unique<RollingOptionsWrapper>(*options);
  }
};

template <typename ArgType>
struct SumWindow {
  using OutType = ArgType;
  using ArgValue = typename GetViewType<ArgType>::T;
  using OutValue = typename GetOutputType<OutType>::T;
  KernelContext* ctx;
  ArgValue sum = 0;
  explicit SumWindow(KernelContext* ctx) : ctx(ctx) {}

  void Append(ArgValue value, Status* st) {
    sum = Add::Call<ArgValue>(ctx, sum, value, st);
  }
  void Remove(ArgValue value, Status* st) {
    sum = Subtract::Call<ArgValue>(ctx, sum, value, st);
  }
  OutValue GetValue(Status* st) const { return sum; }
};

template <typename ArgType>
struct SumCheckedWindow {
  using OutType = ArgType;
  using ArgValue = typename GetViewType<ArgType>::T;
  using OutValue = typename GetOutputType<OutType>::T;
  KernelContext* ctx;
  ArgValue sum = 0;
  explicit SumCheckedWindow(KernelContext* ctx) : ctx(ctx) {}

  void Append(ArgValue value, Status* st) {
    sum = AddChecked::Call<ArgValue>(ctx, sum, value, st);
  }
  void Remove(ArgValue value, Status* st) {
    sum = SubtractChecked::Call<ArgValue>(ctx, sum, value, st);
  }
  OutValue GetValue(Status* st) const { return sum; }
};

template <typename ArgType>
struct ProdWindow {
  using OutType = ArgType;
  using ArgValue = typename GetViewType<ArgType>::T;
  using OutValue = typename GetOutputType<OutType>::T;
  KernelContext* ctx;
  ArgValue prod = 1;
  explicit ProdWindow(KernelContext* ctx) : ctx(ctx) {}

  void Append(ArgValue value, Status* st) {
    prod = Multiply::Call<ArgValue, ArgValue, ArgValue>(ctx, prod, value, st);
  }
  void Remove(ArgValue value, Status* st) {
    prod = Divide::Call<ArgValue, ArgValue, ArgValue>(ctx, prod, value, st);
  }
  OutValue GetValue(Status* st) const { return prod; }
};

template <typename ArgType>
struct ProdCheckedWindow {
  using OutType = ArgType;
  using ArgValue = typename GetViewType<ArgType>::T;
  using OutValue = typename GetOutputType<OutType>::T;
  KernelContext* ctx;
  ArgValue prod = 1;
  explicit ProdCheckedWindow(KernelContext* ctx) : ctx(ctx) {}

  void Append(ArgValue value, Status* st) {
    prod = MultiplyChecked::Call<ArgValue, ArgValue, ArgValue>(ctx, prod, value, st);
  }
  void Remove(ArgValue value, Status* st) {
    prod = DivideChecked::Call<ArgValue, ArgValue, ArgValue>(ctx, prod, value, st);
  }
  OutValue GetValue(Status* st) const { return prod; }
};

template <typename ArgType>
struct MinWindow {
  using OutType = ArgType;
  using ArgValue = typename GetViewType<ArgType>::T;
  using OutValue = typename GetOutputType<OutType>::T;
  KernelContext* ctx;
  std::multiset<ArgValue> values;
  explicit MinWindow(KernelContext* ctx) : ctx(ctx) {}
  void Append(ArgValue value, Status* st) { values.insert(value); }
  void Remove(ArgValue value, Status* st) {
    auto it = values.find(value);
    DCHECK_NE(it, values.end());
    values.erase(it);
  }
  OutValue GetValue(Status* st) const { return *values.begin(); }
};

template <typename ArgType>
struct MaxWindow {
  using OutType = ArgType;
  using ArgValue = typename GetViewType<ArgType>::T;
  using OutValue = typename GetOutputType<OutType>::T;
  KernelContext* ctx;
  std::multiset<ArgValue, std::greater<ArgValue>> values;
  explicit MaxWindow(KernelContext* ctx) : ctx(ctx) {}
  void Append(ArgValue value, Status* st) { values.insert(value); }
  void Remove(ArgValue value, Status* st) {
    auto it = values.find(value);
    DCHECK_NE(it, values.end());
    values.erase(it);
  }
  OutValue GetValue(Status* st) const { return *values.begin(); }
};

template <typename ArgType>
struct MeanWindow {
  using OutType = DoubleType;
  using ArgValue = typename GetViewType<ArgType>::T;
  using OutValue = typename GetOutputType<OutType>::T;
  KernelContext* ctx;
  double sum = 0;
  int64_t count = 0;
  explicit MeanWindow(KernelContext* ctx) : ctx(ctx) {}
  void Append(ArgValue value, Status* st) {
    sum += value;
    count++;
  }
  void Remove(ArgValue value, Status* st) {
    sum -= value;
    count--;
  }
  OutValue GetValue(Status* st) const {
    DCHECK_GE(count, 0);
    return sum / count;
  }
};

template <typename ArgType, typename Window>
struct WindowAccumulator {
  using OutType = typename Window::OutType;
  using OutValue = typename GetOutputType<OutType>::T;
  using ArgValue = typename GetViewType<ArgType>::T;

  KernelContext* ctx;
  const RollingOptions& options;
  Window window;
  NumericBuilder<OutType> out_builder;
  int64_t input_length;
  WindowAccumulator(KernelContext* ctx, const RollingOptions& options,
                    int64_t input_length)
      : ctx(ctx),
        options(options),
        window(ctx),
        out_builder(ctx->memory_pool()),
        input_length(input_length) {}

  Status AccumulateIgnoreNulls(const ArraySpan& input) {
    RETURN_NOT_OK(out_builder.Reserve(input_length));
    Status st = Status::OK();

    // Use two pointers to deal with rolling window
    ArraySpan left_span = input;
    ArraySpan right_span = input;
    auto produce_value_start = std::min(options.min_periods - 1, input_length);
    auto remove_last_start = std::min(options.window_length, input_length);

    out_builder.UnsafeAppendNulls(produce_value_start);
    int64_t window_valid_count = 0;
    // 0 - produce_value_start: only append
    right_span.offset = 0;
    right_span.length = produce_value_start;
    VisitArrayValuesInline<ArgType>(
        right_span,
        [&](ArgValue value) {
          window.Append(value, &st);
          window_valid_count++;
        },
        [] {});
    RETURN_NOT_OK(st);

    // produce_value_start - remove_last_start: append and produce value
    right_span.offset = produce_value_start;
    right_span.length = remove_last_start - produce_value_start;
    VisitArrayValuesInline<ArgType>(
        right_span,
        [&](ArgValue value) {
          window.Append(value, &st);
          window_valid_count++;
          if (window_valid_count >= options.min_periods) {
            out_builder.UnsafeAppend(window.GetValue(&st));
          } else {
            out_builder.UnsafeAppendNull();
          }
        },
        [&] {
          if (window_valid_count >= options.min_periods) {
            out_builder.UnsafeAppend(window.GetValue(&st));
          } else {
            out_builder.UnsafeAppendNull();
          }
        });
    RETURN_NOT_OK(st);

    // remove_last_start - input.length: remove, append and produce value
    right_span.offset = remove_last_start;
    right_span.length = input_length - remove_last_start;
    left_span.length = input_length - remove_last_start;
    VisitTwoArrayValuesInlineAllCases<ArgType, ArgType>(
        left_span, right_span,
        [&](ArgValue left_value, ArgValue right_value) {
          window.Remove(left_value, &st);
          window.Append(right_value, &st);
          if (window_valid_count >= options.min_periods) {
            out_builder.UnsafeAppend(window.GetValue(&st));
          } else {
            out_builder.UnsafeAppendNull();
          }
        },
        [&](ArgValue right_value) {
          window.Append(right_value, &st);
          window_valid_count++;
          if (window_valid_count >= options.min_periods) {
            out_builder.UnsafeAppend(window.GetValue(&st));
          } else {
            out_builder.UnsafeAppendNull();
          }
        },
        [&](ArgValue left_value) {
          window.Remove(left_value, &st);
          window_valid_count--;
          if (window_valid_count >= options.min_periods) {
            out_builder.UnsafeAppend(window.GetValue(&st));
          } else {
            out_builder.UnsafeAppendNull();
          }
        },
        [&] {
          if (window_valid_count >= options.min_periods) {
            out_builder.UnsafeAppend(window.GetValue(&st));
          } else {
            out_builder.UnsafeAppendNull();
          }
        });
    return st;
  }

  Status AccumulateNoIgnoreNulls(const ArraySpan& input) {
    RETURN_NOT_OK(out_builder.Reserve(input_length));

    Status st = Status::OK();

    // Use two pointers to deal with rolling window
    ArraySpan left_span = input;
    ArraySpan right_span = input;
    auto produce_value_start = std::min(options.min_periods - 1, input.length);
    auto remove_last_start = std::min(options.window_length, input.length);

    out_builder.UnsafeAppendNulls(produce_value_start);
    int64_t window_null_count = 0;
    // 0 - produce_value_start, only append
    right_span.offset = 0;
    right_span.length = produce_value_start;
    VisitArrayValuesInline<ArgType>(
        right_span, [&](ArgValue value) { window.Append(value, &st); },
        [&] { window_null_count++; });
    RETURN_NOT_OK(st);

    // produce_value_start - remove_last_start, append and produce value
    right_span.offset = produce_value_start;
    right_span.length = remove_last_start - produce_value_start;
    int64_t window_size = produce_value_start;
    VisitArrayValuesInline<ArgType>(
        right_span,
        [&](ArgValue value) {
          window_size++;
          window.Append(value, &st);
          if (window_null_count > 0 || window_size < options.min_periods) {
            out_builder.UnsafeAppendNull();
          } else {
            out_builder.UnsafeAppend(window.GetValue(&st));
          }
        },
        [&] {
          window_size++;
          window_null_count++;
          out_builder.UnsafeAppendNull();
        });
    RETURN_NOT_OK(st);

    // remove_last_start - input.length, remove, append and produce value
    right_span.offset = remove_last_start;
    right_span.length = input.length - remove_last_start;
    left_span.length = input_length - remove_last_start;
    VisitTwoArrayValuesInlineAllCases<ArgType, ArgType>(
        left_span, right_span,
        [&](ArgValue left_value, ArgValue right_value) {
          window.Remove(left_value, &st);
          window.Append(right_value, &st);
          if (window_null_count > 0) {
            out_builder.UnsafeAppendNull();
          } else {
            out_builder.UnsafeAppend(window.GetValue(&st));
          }
        },
        [&](ArgValue right_value) {
          window_null_count--;
          window.Append(right_value, &st);
          if (window_null_count > 0) {
            out_builder.UnsafeAppendNull();
          } else {
            out_builder.UnsafeAppend(window.GetValue(&st));
          }
        },
        [&](ArgValue left_value) {
          window.Remove(left_value, &st);
          window_null_count++;
          out_builder.UnsafeAppendNull();
        },
        [&] {
          if (window_null_count > 0) {
            out_builder.UnsafeAppendNull();
          } else {
            out_builder.UnsafeAppend(window.GetValue(&st));
          }
        });
    return st;
  }

  Status Accumulate(const ArraySpan& input, std::shared_ptr<ArrayData>* output) {
    if (options.ignore_nulls) {
      RETURN_NOT_OK(AccumulateIgnoreNulls(input));
    } else {
      RETURN_NOT_OK(AccumulateNoIgnoreNulls(input));
    }
    return out_builder.FinishInternal(output);
  }
};

template <typename ArgType, typename Window>
struct RollingKernel {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = RollingOptionsWrapper::Get(ctx);
    WindowAccumulator<ArgType, Window> accumulator(ctx, options, batch.length);
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(accumulator.Accumulate(batch[0].array, &result));
    out->value = std::move(result);
    return Status::OK();
  }
};

template <typename ArgType, typename Window>
struct ChunkedWindowAccumulator {
  using OutType = typename Window::OutType;
  using OutValue = typename GetOutputType<OutType>::T;
  using ArgValue = typename GetViewType<ArgType>::T;

  KernelContext* ctx;
  const RollingOptions& options;
  Window window;
  NumericBuilder<OutType> out_builder;
  int64_t input_length;
  ChunkedWindowAccumulator(KernelContext* ctx, const RollingOptions& options,
                           int64_t input_length)
      : ctx(ctx),
        options(options),
        window(ctx),
        out_builder(ctx->memory_pool()),
        input_length(input_length) {}

  Status AccumulateIgnoreNulls(const ChunkedArray& input) {
    RETURN_NOT_OK(out_builder.Reserve(input_length));
    Status st = Status::OK();

    // Use two pointers to deal with rolling window
    int left_chunk_index = 0;
    int64_t left_position = 0;
    ArrayIterator<ArgType> left_iterator(*input.chunk(0)->data());
    int right_chunk_index = 0;
    int64_t right_position = 0;
    ArrayIterator<ArgType> right_iterator(*input.chunk(0)->data());

    auto check_left_boundary = [&]() {
      while (left_position == input.chunk(left_chunk_index)->length()) {
        left_chunk_index++;
        left_position = 0;
        left_iterator = ArrayIterator<ArgType>(*input.chunk(left_chunk_index)->data());
      }
    };
    auto check_right_boundary = [&]() {
      while (right_position == input.chunk(right_chunk_index)->length()) {
        right_chunk_index++;
        right_position = 0;
        right_iterator = ArrayIterator<ArgType>(*input.chunk(right_chunk_index)->data());
      }
    };

    int64_t produce_value_start = std::min(options.min_periods - 1, input_length);
    int64_t remove_last_start = std::min(options.window_length, input_length);

    out_builder.UnsafeAppendNulls(produce_value_start);
    int64_t window_valid_count = 0;

    // 0 - produce_value_start: only append
    for (int64_t i = 0; i < produce_value_start; ++i) {
      check_right_boundary();
      if (input.chunk(right_chunk_index)->IsValid(right_position)) {
        window.Append(right_iterator(), &st);
        window_valid_count++;
      } else {
        right_iterator();
      }
      right_position++;
    }
    RETURN_NOT_OK(st);

    // produce_value_start - remove_last_start: remove, append and produce value
    for (int64_t i = produce_value_start; i < remove_last_start; ++i) {
      check_right_boundary();
      if (input.chunk(right_chunk_index)->IsValid(right_position)) {
        window.Append(right_iterator(), &st);
        window_valid_count++;
        if (window_valid_count >= options.min_periods) {
          out_builder.UnsafeAppend(window.GetValue(&st));
        } else {
          out_builder.UnsafeAppendNull();
        }
      } else {
        right_iterator();
        if (window_valid_count >= options.min_periods) {
          out_builder.UnsafeAppend(window.GetValue(&st));
        } else {
          out_builder.UnsafeAppendNull();
        }
      }
      right_position++;
    }
    RETURN_NOT_OK(st);

    // remove_last_start - input_length: remove, append and produce value
    for (int64_t i = remove_last_start; i < input_length; ++i) {
      check_left_boundary();
      check_right_boundary();
      bool left_valid = input.chunk(left_chunk_index)->IsValid(left_position);
      bool right_valid = input.chunk(right_chunk_index)->IsValid(right_position);
      if (left_valid && right_valid) {
        window.Remove(left_iterator(), &st);
        window.Append(right_iterator(), &st);
        if (window_valid_count >= options.min_periods) {
          out_builder.UnsafeAppend(window.GetValue(&st));
        } else {
          out_builder.UnsafeAppendNull();
        }
      } else if (left_valid) {
        window.Remove(left_iterator(), &st);
        right_iterator();
        window_valid_count--;
        if (window_valid_count >= options.min_periods) {
          out_builder.UnsafeAppend(window.GetValue(&st));
        } else {
          out_builder.UnsafeAppendNull();
        }
      } else if (right_valid) {
        left_iterator();
        window.Append(right_iterator(), &st);
        window_valid_count++;
        if (window_valid_count >= options.min_periods) {
          out_builder.UnsafeAppend(window.GetValue(&st));
        } else {
          out_builder.UnsafeAppendNull();
        }
      } else {
        left_iterator();
        right_iterator();
        if (window_valid_count >= options.min_periods) {
          out_builder.UnsafeAppend(window.GetValue(&st));
        } else {
          out_builder.UnsafeAppendNull();
        }
      }
      left_position++;
      right_position++;
    }
    return st;
  }

  Status AccumulateNoIgnoreNulls(const ChunkedArray& input) {
    RETURN_NOT_OK(out_builder.Reserve(input_length));

    Status st = Status::OK();

    // Use two pointers to deal with rolling window
    int left_chunk_index = 0;
    int64_t left_position = 0;
    ArrayIterator<ArgType> left_iterator(*input.chunk(0)->data());
    int right_chunk_index = 0;
    int64_t right_position = 0;
    ArrayIterator<ArgType> right_iterator(*input.chunk(0)->data());

    auto check_left_boundary = [&]() {
      while (left_position == input.chunk(left_chunk_index)->length()) {
        left_chunk_index++;
        left_position = 0;
        left_iterator = ArrayIterator<ArgType>(*input.chunk(left_chunk_index)->data());
      }
    };
    auto check_right_boundary = [&]() {
      while (right_position == input.chunk(right_chunk_index)->length()) {
        right_chunk_index++;
        right_position = 0;
        right_iterator = ArrayIterator<ArgType>(*input.chunk(right_chunk_index)->data());
      }
    };

    int64_t produce_value_start = std::min(options.min_periods - 1, input_length);
    int64_t remove_last_start = std::min(options.window_length, input_length);

    out_builder.UnsafeAppendNulls(produce_value_start);
    int64_t window_null_count = 0;

    // 0 - produce_value_start: only append
    for (int64_t i = 0; i < produce_value_start; ++i) {
      check_right_boundary();
      if (input.chunk(right_chunk_index)->IsValid(right_position)) {
        window.Append(right_iterator(), &st);
      } else {
        right_iterator();
        window_null_count++;
      }
      right_position++;
    }
    RETURN_NOT_OK(st);

    // produce_value_start - remove_last_start: remove, append and produce value
    for (int64_t i = produce_value_start; i < remove_last_start; ++i) {
      check_right_boundary();
      if (input.chunk(right_chunk_index)->IsValid(right_position)) {
        window.Append(right_iterator(), &st);
        if (window_null_count > 0) {
          out_builder.UnsafeAppendNull();
        } else {
          out_builder.UnsafeAppend(window.GetValue(&st));
        }
      } else {
        right_iterator();
        window_null_count++;
        out_builder.UnsafeAppendNull();
      }
      right_position++;
    }
    RETURN_NOT_OK(st);

    // remove_last_start - input_length: remove, append and produce value
    for (int64_t i = remove_last_start; i < input_length; ++i) {
      check_left_boundary();
      check_right_boundary();
      bool left_valid = input.chunk(left_chunk_index)->IsValid(left_position);
      bool right_valid = input.chunk(right_chunk_index)->IsValid(right_position);
      if (left_valid && right_valid) {
        window.Remove(left_iterator(), &st);
        window.Append(right_iterator(), &st);
        if (window_null_count > 0) {
          out_builder.UnsafeAppendNull();
        } else {
          out_builder.UnsafeAppend(window.GetValue(&st));
        }
      } else if (left_valid) {
        window.Remove(left_iterator(), &st);
        right_iterator();
        window_null_count++;
        out_builder.UnsafeAppendNull();
      } else if (right_valid) {
        left_iterator();
        window.Append(right_iterator(), &st);
        window_null_count--;
        if (window_null_count > 0) {
          out_builder.UnsafeAppendNull();
        } else {
          out_builder.UnsafeAppend(window.GetValue(&st));
        }
      } else {
        left_iterator();
        right_iterator();
        if (window_null_count > 0) {
          out_builder.UnsafeAppendNull();
        } else {
          out_builder.UnsafeAppend(window.GetValue(&st));
        }
      }
      left_position++;
      right_position++;
    }
    return st;
  }

  Status Accumulate(const ChunkedArray& input, std::shared_ptr<ArrayData>* output) {
    if (options.ignore_nulls) {
      RETURN_NOT_OK(AccumulateIgnoreNulls(input));
    } else {
      RETURN_NOT_OK(AccumulateNoIgnoreNulls(input));
    }
    return out_builder.FinishInternal(output);
  }
};

template <typename ArgType, typename Window>
struct RollingKernelChunked {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = RollingOptionsWrapper::Get(ctx);
    const ChunkedArray& chunked_input = *batch[0].chunked_array();
    ChunkedWindowAccumulator<ArgType, Window> accumulator(ctx, options,
                                                          chunked_input.length());
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(accumulator.Accumulate(chunked_input, &result));
    out->value = std::move(result);
    return Status::OK();
  }
};

template <template <typename ArgType> typename Window>
struct RollingKernelFactory {
  VectorKernel kernel;

  RollingKernelFactory() {
    kernel.can_execute_chunkwise = false;
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::NO_PREALLOCATE;
    kernel.init = RollingOptionsWrapper::Init;
  }
  template <typename T>
  enable_if_number<T, Status> Visit(const T& ty) {
    using OutType = typename Window<T>::OutType;
    kernel.signature = KernelSignature::Make(
        {ty.id()}, OutputType(TypeTraits<OutType>::type_singleton()));
    kernel.exec = RollingKernel<T, Window<T>>::Exec;
    kernel.exec_chunked = RollingKernelChunked<T, Window<T>>::Exec;
    return Status::OK();
  }

  Status Visit(const DataType& ty) {
    return Status::NotImplemented("No rolling kernel implemented for ", ty);
  }

  Result<VectorKernel> Make(const std::shared_ptr<DataType>& ty) {
    RETURN_NOT_OK(VisitTypeInline(*ty, this));
    return kernel;
  }
};

template <template <typename ArgValue> typename Window>
void MakeVectorRollingFunction(FunctionRegistry* registry, const std::string& func_name,
                               const FunctionDoc doc) {
  RollingKernelFactory<Window> factory;
  auto func = std::make_shared<VectorFunction>(func_name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto kernel = factory.Make(ty).ValueOrDie();
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

const FunctionDoc rolling_sum_doc{
    "Compute the rolling sum over a fixed length sliding window",
    "`values` must be numeric. Return an array/chunked array which\n"
    "contains the rolling sum of the values in the window. Result will wrap\n"
    "around on integer overflow. Use function \"rolling_sum_checked\" if\n"
    "you want overflow to return an error.",
    {"array"},
    "RollingOptions"};
const FunctionDoc rolling_sum_checked_doc{
    "Compute the rolling sum over a fixed length sliding window",
    "`values` must be numeric. Return an array/chunked array which\n"
    "contains the rolling sum of the values in the window. This function\n"
    "returns an error on overflow. For a variant that doesn't fail on\n"
    "overflow, use function \"rolling_sum\".",
    {"array"},
    "RollingOptions"};
const FunctionDoc rolling_prod_doc{
    "Compute the rolling product over a fixed length sliding window",
    "`values` must be numeric. Return an array/chunked array which\n"
    "contains the rolling product of the values in the window. Result will\n"
    "wrap around on integer overflow. Use function \"rolling_prod_checked\"\n"
    "if you want overflow to return an error.",
    {"array"},
    "RollingOptions"};
const FunctionDoc rolling_prod_checked_doc{
    "Compute the rolling product over a fixed length sliding window",
    "`values` must be numeric. Return an array/chunked array which\n"
    "contains the rolling product of the values in the window. This\n"
    "function returns an error on overflow. For a variant that doesn't\n"
    "fail on overflow, use function \"rolling_prod\".",
    {"array"},
    "RollingOptions"};
const FunctionDoc rolling_min_doc{
    "Compute the rolling minimum over a fixed length sliding window",
    "`values` must be numeric. Return an array/chunked array which\n"
    "contains the rolling minimum of the values in the window.",
    {"array"},
    "RollingOptions"};
const FunctionDoc rolling_max_doc{
    "Compute the rolling maximum over a fixed length sliding window",
    "`values` must be numeric. Return an array/chunked array which\n"
    "contains the rolling maximum of the values in the window.",
    {"array"},
    "RollingOptions"};
const FunctionDoc rolling_mean_doc{
    "Compute the rolling mean over a fixed length sliding window",
    "`values` must be numeric. Return an array/chunked array which\n"
    "contains the rolling mean of the values in the window.",
    {"array"},
    "RollingOptions"};

}  // namespace

void RegisterVectorRolling(FunctionRegistry* registry) {
  MakeVectorRollingFunction<SumWindow>(registry, "rolling_sum", rolling_sum_doc);
  MakeVectorRollingFunction<SumCheckedWindow>(registry, "rolling_sum_checked",
                                              rolling_sum_checked_doc);
  MakeVectorRollingFunction<ProdWindow>(registry, "rolling_prod", rolling_prod_doc);
  MakeVectorRollingFunction<ProdCheckedWindow>(registry, "rolling_prod_checked",
                                               rolling_prod_checked_doc);
  MakeVectorRollingFunction<MinWindow>(registry, "rolling_min", rolling_min_doc);
  MakeVectorRollingFunction<MaxWindow>(registry, "rolling_max", rolling_max_doc);
  MakeVectorRollingFunction<MeanWindow>(registry, "rolling_mean", rolling_mean_doc);
}

}  // namespace arrow::compute::internal
