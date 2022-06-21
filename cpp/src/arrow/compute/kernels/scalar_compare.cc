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

#include <algorithm>
#include <cmath>
#include <limits>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/optional.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using util::string_view;

namespace compute {
namespace internal {

namespace {

struct Equal {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr T Call(KernelContext*, const Arg0& left, const Arg1& right, Status*) {
    static_assert(std::is_same<T, bool>::value && std::is_same<Arg0, Arg1>::value, "");
    return left == right;
  }
};

struct NotEqual {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr T Call(KernelContext*, const Arg0& left, const Arg1& right, Status*) {
    static_assert(std::is_same<T, bool>::value && std::is_same<Arg0, Arg1>::value, "");
    return left != right;
  }
};

struct Greater {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr T Call(KernelContext*, const Arg0& left, const Arg1& right, Status*) {
    static_assert(std::is_same<T, bool>::value && std::is_same<Arg0, Arg1>::value, "");
    return left > right;
  }
};

struct GreaterEqual {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr T Call(KernelContext*, const Arg0& left, const Arg1& right, Status*) {
    static_assert(std::is_same<T, bool>::value && std::is_same<Arg0, Arg1>::value, "");
    return left >= right;
  }
};

struct Minimum {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_value<T> Call(Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<Arg0, Arg1>::value, "");
    return std::fmin(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer_value<T> Call(Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<Arg0, Arg1>::value, "");
    return std::min(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<Arg0, Arg1>::value, "");
    return std::min(left, right);
  }

  static string_view Call(string_view left, string_view right) {
    return std::min(left, right);
  }

  template <typename T>
  static constexpr enable_if_t<std::is_same<float, T>::value, T> antiextreme() {
    return std::nanf("");
  }

  template <typename T>
  static constexpr enable_if_t<std::is_same<double, T>::value, T> antiextreme() {
    return std::nan("");
  }

  template <typename T>
  static constexpr enable_if_integer_value<T> antiextreme() {
    return std::numeric_limits<T>::max();
  }

  template <typename T>
  static constexpr enable_if_decimal_value<T> antiextreme() {
    return T::GetMaxSentinel();
  }
};

struct Maximum {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_value<T> Call(Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<Arg0, Arg1>::value, "");
    return std::fmax(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer_value<T> Call(Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<Arg0, Arg1>::value, "");
    return std::max(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<Arg0, Arg1>::value, "");
    return std::max(left, right);
  }

  static string_view Call(string_view left, string_view right) {
    return std::max(left, right);
  }

  template <typename T>
  static constexpr enable_if_t<std::is_same<float, T>::value, T> antiextreme() {
    return std::nanf("");
  }

  template <typename T>
  static constexpr enable_if_t<std::is_same<double, T>::value, T> antiextreme() {
    return std::nan("");
  }

  template <typename T>
  static constexpr enable_if_integer_value<T> antiextreme() {
    return std::numeric_limits<T>::min();
  }

  template <typename T>
  static constexpr enable_if_decimal_value<T> antiextreme() {
    return T::GetMinSentinel();
  }
};

// Implement Less, LessEqual by flipping arguments to Greater, GreaterEqual

template <typename OutType, typename ArgType, typename Op>
struct CompareTimestamps
    : public applicator::ScalarBinaryEqualTypes<OutType, ArgType, Op> {
  using Base = applicator::ScalarBinaryEqualTypes<OutType, ArgType, Op>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& lhs = checked_cast<const TimestampType&>(*batch[0].type());
    const auto& rhs = checked_cast<const TimestampType&>(*batch[1].type());
    if (lhs.timezone().empty() ^ rhs.timezone().empty()) {
      return Status::Invalid(
          "Cannot compare timestamp with timezone to timestamp without timezone, got: ",
          lhs, " and ", rhs);
    }
    return Base::Exec(ctx, batch, out);
  }
};

template <typename Op>
void AddIntegerCompare(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  auto exec =
      GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(*ty);
  DCHECK_OK(func->AddKernel({ty, ty}, boolean(), std::move(exec)));
}

template <typename InType, typename Op>
void AddGenericCompare(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  DCHECK_OK(
      func->AddKernel({ty, ty}, boolean(),
                      applicator::ScalarBinaryEqualTypes<BooleanType, InType, Op>::Exec));
}

struct CompareFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));
    if (HasDecimal(*values)) {
      RETURN_NOT_OK(CastBinaryDecimalArgs(DecimalPromotion::kAdd, values));
    }

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);
    ReplaceNullWithOtherType(values);

    if (auto type = CommonNumeric(*values)) {
      ReplaceTypes(type, values);
    } else if (auto type = CommonTemporal(values->data(), values->size())) {
      ReplaceTypes(type, values);
    } else if (auto type = CommonBinary(values->data(), values->size())) {
      ReplaceTypes(type, values);
    }

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

struct VarArgsCompareFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);

    if (auto type = CommonNumeric(*values)) {
      ReplaceTypes(type, values);
    } else if (auto type = CommonTemporal(values->data(), values->size())) {
      ReplaceTypes(type, values);
    }

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

template <typename Op>
std::shared_ptr<ScalarFunction> MakeCompareFunction(std::string name, FunctionDoc doc) {
  auto func = std::make_shared<CompareFunction>(name, Arity::Binary(), std::move(doc));

  DCHECK_OK(func->AddKernel(
      {boolean(), boolean()}, boolean(),
      applicator::ScalarBinary<BooleanType, BooleanType, BooleanType, Op>::Exec));

  for (const std::shared_ptr<DataType>& ty : IntTypes()) {
    AddIntegerCompare<Op>(ty, func.get());
  }
  AddIntegerCompare<Op>(date32(), func.get());
  AddIntegerCompare<Op>(date64(), func.get());

  AddGenericCompare<FloatType, Op>(float32(), func.get());
  AddGenericCompare<DoubleType, Op>(float64(), func.get());

  // Add timestamp kernels
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(),
                              CompareTimestamps<BooleanType, TimestampType, Op>::Exec));
  }

  // Duration
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::DurationTypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(
            int64());
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(), std::move(exec)));
  }

  // Time32 and Time64
  for (auto unit : {TimeUnit::SECOND, TimeUnit::MILLI}) {
    InputType in_type(match::Time32TypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(
            int32());
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(), std::move(exec)));
  }
  for (auto unit : {TimeUnit::MICRO, TimeUnit::NANO}) {
    InputType in_type(match::Time64TypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(
            int64());
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(), std::move(exec)));
  }

  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    auto exec =
        GenerateVarBinaryBase<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(*ty);
    DCHECK_OK(func->AddKernel({ty, ty}, boolean(), std::move(exec)));
  }

  for (const auto id : {Type::DECIMAL128, Type::DECIMAL256}) {
    auto exec = GenerateDecimal<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(id);
    DCHECK_OK(
        func->AddKernel({InputType(id), InputType(id)}, boolean(), std::move(exec)));
  }

  {
    auto exec =
        applicator::ScalarBinaryEqualTypes<BooleanType, FixedSizeBinaryType, Op>::Exec;
    auto ty = InputType(Type::FIXED_SIZE_BINARY);
    DCHECK_OK(func->AddKernel({ty, ty}, boolean(), std::move(exec)));
  }

  return func;
}

struct FlippedData : public KernelState {
  ArrayKernelExec unflipped_exec;
  explicit FlippedData(ArrayKernelExec unflipped_exec) : unflipped_exec(unflipped_exec) {}
};

Status FlippedBinaryExec(KernelContext* ctx, const ExecSpan& span, ExecResult* out) {
  const auto kernel = static_cast<const ScalarKernel*>(ctx->kernel());
  DCHECK(kernel);
  const auto kernel_data = static_cast<const FlippedData*>(kernel->data.get());

  ExecSpan flipped_span = span;
  std::swap(flipped_span.values[0], flipped_span.values[1]);
  return kernel_data->unflipped_exec(ctx, flipped_span, out);
}

std::shared_ptr<ScalarFunction> MakeFlippedFunction(std::string name,
                                                    const ScalarFunction& func,
                                                    FunctionDoc doc) {
  auto flipped_func =
      std::make_shared<CompareFunction>(name, Arity::Binary(), std::move(doc));
  for (const ScalarKernel* kernel : func.kernels()) {
    ScalarKernel flipped_kernel = *kernel;
    flipped_kernel.data = std::make_shared<FlippedData>(kernel->exec);
    flipped_kernel.exec = FlippedBinaryExec;
    DCHECK_OK(flipped_func->AddKernel(std::move(flipped_kernel)));
  }
  return flipped_func;
}

using MinMaxState = OptionsWrapper<ElementWiseAggregateOptions>;

// Implement a variadic scalar min/max kernel.
template <typename OutType, typename Op>
struct ScalarMinMax {
  using OutValue = typename GetOutputType<OutType>::T;

  static void ExecScalar(const ExecSpan& batch,
                         const ElementWiseAggregateOptions& options, Scalar* out) {
    // All arguments are scalar
    OutValue value{};
    bool valid = false;
    for (const ExecValue& arg : batch.values) {
      // Ignore non-scalar arguments so we can use it in the mixed-scalar-and-array case
      if (!arg.is_scalar()) continue;
      const Scalar& scalar = *arg.scalar;
      if (!scalar.is_valid) {
        if (options.skip_nulls) continue;
        out->is_valid = false;
        return;
      }
      if (!valid) {
        value = UnboxScalar<OutType>::Unbox(scalar);
        valid = true;
      } else {
        value = Op::template Call<OutValue, OutValue, OutValue>(
            value, UnboxScalar<OutType>::Unbox(scalar));
      }
    }
    out->is_valid = valid;
    if (valid) {
      BoxScalar<OutType>::Box(value, out);
    }
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ElementWiseAggregateOptions& options = MinMaxState::Get(ctx);
    const auto descrs = batch.GetDescriptors();
    const size_t scalar_count = static_cast<size_t>(
        std::count_if(batch.values.begin(), batch.values.end(),
                      [](const ExecValue& v) { return v.is_scalar(); }));
    if (scalar_count == batch.values.size()) {
      ExecScalar(batch, options, out->scalar().get());
      return Status::OK();
    }

    ArrayData* output = out->array_data().get();

    // At least one array, two or more arguments
    std::vector<const ArraySpan*> arrays;
    for (const auto& value : batch.values) {
      if (!value.is_array()) continue;
      arrays.push_back(&value.array);
    }

    bool initialize_output = true;
    if (scalar_count > 0) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> temp_scalar,
                            MakeScalar(out->type()->Copy(), 0));
      ExecScalar(batch, options, temp_scalar.get());
      if (temp_scalar->is_valid) {
        const auto value = UnboxScalar<OutType>::Unbox(*temp_scalar);
        initialize_output = false;
        OutValue* out = output->GetMutableValues<OutValue>(1);
        std::fill(out, out + batch.length, value);
      } else if (!options.skip_nulls) {
        // Abort early
        ARROW_ASSIGN_OR_RAISE(auto array, MakeArrayFromScalar(*temp_scalar, batch.length,
                                                              ctx->memory_pool()));
        out->value = std::move(array->data());
        return Status::OK();
      }
    }

    if (initialize_output) {
      OutValue* out = output->GetMutableValues<OutValue>(1);
      std::fill(out, out + batch.length, Op::template antiextreme<OutValue>());
    }

    // Precompute the validity buffer
    if (options.skip_nulls && initialize_output) {
      // OR together the validity buffers of all arrays
      if (std::all_of(arrays.begin(), arrays.end(),
                      [](const ArraySpan* arr) { return arr->MayHaveNulls(); })) {
        for (const ArraySpan* arr : arrays) {
          if (!arr->MayHaveNulls()) continue;
          if (!output->buffers[0]) {
            ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(batch.length));
            ::arrow::internal::CopyBitmap(arr->buffers[0].data, arr->offset, batch.length,
                                          output->buffers[0]->mutable_data(),
                                          /*dest_offset=*/0);
          } else {
            ::arrow::internal::BitmapOr(output->buffers[0]->data(), /*left_offset=*/0,
                                        arr->buffers[0].data, arr->offset, batch.length,
                                        /*out_offset=*/0,
                                        output->buffers[0]->mutable_data());
          }
        }
      }
    } else if (!options.skip_nulls) {
      // AND together the validity buffers of all arrays
      for (const ArraySpan* arr : arrays) {
        if (!arr->MayHaveNulls()) continue;
        if (!output->buffers[0]) {
          ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(batch.length));
          ::arrow::internal::CopyBitmap(arr->buffers[0].data, arr->offset, batch.length,
                                        output->buffers[0]->mutable_data(),
                                        /*dest_offset=*/0);
        } else {
          ::arrow::internal::BitmapAnd(output->buffers[0]->data(), /*left_offset=*/0,
                                       arr->buffers[0].data, arr->offset, batch.length,
                                       /*out_offset=*/0,
                                       output->buffers[0]->mutable_data());
        }
      }
    }

    for (const ArraySpan* array : arrays) {
      // TODO(wesm): this got to be a mess in ARROW-16576, clean up
      ArraySpan out_span(*output);
      OutputArrayWriter<OutType> writer(&out_span);
      ArrayIterator<OutType> out_it(out_span);
      int64_t index = 0;
      VisitArrayValuesInline<OutType>(
          *array,
          [&](OutValue value) {
            auto u = out_it();
            if (!output->buffers[0] ||
                bit_util::GetBit(output->buffers[0]->data(), index)) {
              writer.Write(Op::template Call<OutValue, OutValue, OutValue>(u, value));
            } else {
              writer.Write(value);
            }
            index++;
          },
          [&]() {
            // RHS is null, preserve the LHS
            writer.values++;
            index++;
            out_it();
          });
    }
    output->null_count = output->buffers[0] ? -1 : 0;
    return Status::OK();
  }
};

template <typename Op>
Status ExecBinaryMinMaxScalar(KernelContext* ctx,
                              const ElementWiseAggregateOptions& options,
                              const ExecSpan& batch, ExecResult* out) {
  if (batch.values.empty()) {
    return Status::OK();
  }
  auto output = checked_cast<BaseBinaryScalar*>(out->scalar().get());
  if (!options.skip_nulls) {
    // any nulls in the input will produce a null output
    for (const ExecValue& value : batch.values) {
      if (!value.scalar->is_valid) {
        output->is_valid = false;
        return Status::OK();
      }
    }
  }
  const auto& first_scalar = *batch.values.front().scalar;
  string_view result = checked_cast<const BaseBinaryScalar&>(first_scalar).view();
  bool valid = first_scalar.is_valid;
  for (int i = 1; i < batch.num_values(); i++) {
    const Scalar& scalar = *batch[i].scalar;
    if (!scalar.is_valid) {
      DCHECK(options.skip_nulls);
      continue;
    } else {
      string_view value = checked_cast<const BaseBinaryScalar&>(scalar).view();
      result = !valid ? value : Op::Call(result, value);
      valid = true;
    }
  }
  if (valid) {
    ARROW_ASSIGN_OR_RAISE(output->value, ctx->Allocate(result.size()));
    std::copy(result.begin(), result.end(), output->value->mutable_data());
    output->is_valid = true;
  } else {
    output->is_valid = false;
  }
  return Status::OK();
}

template <typename Type, typename Op>
struct BinaryScalarMinMax {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ElementWiseAggregateOptions& options = MinMaxState::Get(ctx);
    if (batch.is_all_scalar()) {
      return ExecBinaryMinMaxScalar<Op>(ctx, options, batch, out);
    }
    return ExecContainingArrays(ctx, options, batch, out);
  }

  static Status ExecContainingArrays(KernelContext* ctx,
                                     const ElementWiseAggregateOptions& options,
                                     const ExecSpan& batch, ExecResult* out) {
    // Presize data to avoid reallocations, using an estimation of final size.
    int64_t estimated_final_size = EstimateOutputSize(batch);
    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    RETURN_NOT_OK(builder.ReserveData(estimated_final_size));

    for (int64_t row = 0; row < batch.length; row++) {
      util::optional<string_view> result;
      auto visit_value = [&](string_view value) {
        result = !result ? value : Op::Call(*result, value);
      };

      for (int col = 0; col < batch.num_values(); col++) {
        if (batch[col].is_scalar()) {
          const Scalar& scalar = *batch[col].scalar;
          if (scalar.is_valid) {
            visit_value(UnboxScalar<Type>::Unbox(scalar));
          } else if (!options.skip_nulls) {
            result = util::nullopt;
            break;
          }
        } else {
          const ArraySpan& array = batch[col].array;
          if (!array.MayHaveNulls() ||
              bit_util::GetBit(array.buffers[0].data, array.offset + row)) {
            const auto offsets = array.GetValues<offset_type>(1);
            const auto data = array.GetValues<uint8_t>(2, /*absolute_offset=*/0);
            const int64_t length = offsets[row + 1] - offsets[row];
            visit_value(
                string_view(reinterpret_cast<const char*>(data + offsets[row]), length));
          } else if (!options.skip_nulls) {
            result = util::nullopt;
            break;
          }
        }
      }

      if (result) {
        RETURN_NOT_OK(builder.Append(*result));
      } else {
        builder.UnsafeAppendNull();
      }
    }

    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder.Finish(&string_array));
    out->value = std::move(string_array->data());
    out->array_data()->type = batch[0].type()->Copy();
    DCHECK_EQ(batch.length, out->array_data()->length);
    return Status::OK();
  }

  // Compute an estimation for the length of the output batch.
  static int64_t EstimateOutputSize(const ExecSpan& batch) {
    int64_t estimated_final_size = 0;
    for (const ExecValue& value : batch.values) {
      if (value.is_scalar()) {
        const auto& scalar = checked_cast<const BaseBinaryScalar&>(*value.scalar);
        if (scalar.is_valid) {
          estimated_final_size = std::max(estimated_final_size, scalar.value->size());
        }
      } else {
        const ArraySpan& array = value.array;
        const auto offsets = array.GetValues<offset_type>(1);
        int64_t estimated_current_size = offsets[array.length] - offsets[0];
        estimated_final_size = std::max(estimated_final_size, estimated_current_size);
      }
    }
    return estimated_final_size;
  }
};

template <typename Op>
struct FixedSizeBinaryScalarMinMax {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ElementWiseAggregateOptions& options = MinMaxState::Get(ctx);
    if (batch.is_all_scalar()) {
      return ExecBinaryMinMaxScalar<Op>(ctx, options, batch, out);
    }
    return ExecContainingArrays(ctx, options, batch, out);
  }

  static Status ExecContainingArrays(KernelContext* ctx,
                                     const ElementWiseAggregateOptions& options,
                                     const ExecSpan& batch, ExecResult* out) {
    const DataType* batch_type = batch[0].type();
    const auto binary_type = checked_cast<const FixedSizeBinaryType*>(batch_type);
    int32_t byte_width = binary_type->byte_width();
    // Presize data to avoid reallocations.
    int64_t estimated_final_size = batch.length * byte_width;
    FixedSizeBinaryBuilder builder(batch_type->Copy());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    RETURN_NOT_OK(builder.ReserveData(estimated_final_size));

    std::vector<string_view> valid_cols(batch.num_values());
    for (int64_t row = 0; row < batch.length; row++) {
      string_view result;
      auto visit_value = [&](string_view value) {
        result = result.empty() ? value : Op::Call(result, value);
      };

      for (int col = 0; col < batch.num_values(); col++) {
        if (batch[col].is_scalar()) {
          const Scalar& scalar = *batch[col].scalar;
          if (scalar.is_valid) {
            visit_value(UnboxScalar<FixedSizeBinaryType>::Unbox(scalar));
          } else if (!options.skip_nulls) {
            result = string_view();
            break;
          }
        } else {
          const ArraySpan& array = batch[col].array;
          if (!array.MayHaveNulls() ||
              bit_util::GetBit(array.buffers[0].data, array.offset + row)) {
            const auto data = array.GetValues<uint8_t>(1, /*absolute_offset=*/0);
            visit_value(string_view(
                reinterpret_cast<const char*>(data) + row * byte_width, byte_width));
          } else if (!options.skip_nulls) {
            result = string_view();
            break;
          }
        }
      }

      if (result.empty()) {
        builder.UnsafeAppendNull();
      } else {
        builder.UnsafeAppend(result);
      }
    }

    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder.Finish(&string_array));
    out->value = std::move(string_array->data());
    out->array_data()->type = batch[0].type()->Copy();
    DCHECK_EQ(batch.length, out->array_data()->length);
    return Status::OK();
  }
};

Result<ValueDescr> ResolveMinOrMaxOutputType(KernelContext*,
                                             const std::vector<ValueDescr>& args) {
  if (args.empty()) {
    return null();
  }
  auto first_type = args[0].type;
  for (size_t i = 1; i < args.size(); ++i) {
    auto type = args[i].type;
    if (*type != *first_type) {
      return Status::NotImplemented(
          "Different input types not supported for {min, max}_element_wise");
    }
  }
  return ValueDescr(first_type, GetBroadcastShape(args));
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeScalarMinMax(std::string name, FunctionDoc doc) {
  static auto default_element_wise_aggregate_options =
      ElementWiseAggregateOptions::Defaults();

  auto func = std::make_shared<VarArgsCompareFunction>(
      name, Arity::VarArgs(), std::move(doc), &default_element_wise_aggregate_options);
  for (const auto& ty : NumericTypes()) {
    auto exec = GeneratePhysicalNumeric<ScalarMinMax, Op>(ty);
    ScalarKernel kernel{KernelSignature::Make({ty}, ty, /*is_varargs=*/true), exec,
                        MinMaxState::Init};
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  for (const auto& ty : TemporalTypes()) {
    auto exec = GeneratePhysicalNumeric<ScalarMinMax, Op>(ty);
    ScalarKernel kernel{KernelSignature::Make({ty}, ty, /*is_varargs=*/true), exec,
                        MinMaxState::Init};
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  for (const auto& ty : BaseBinaryTypes()) {
    auto exec =
        GenerateTypeAgnosticVarBinaryBase<BinaryScalarMinMax, ArrayKernelExec, Op>(ty);
    ScalarKernel kernel{KernelSignature::Make({ty}, ty, /*is_varargs=*/true), exec,
                        MinMaxState::Init};
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  for (const auto id : {Type::DECIMAL128, Type::DECIMAL256}) {
    auto exec = GenerateDecimalToDecimal<ScalarMinMax, Op>(id);
    OutputType out_type(ResolveMinOrMaxOutputType);
    ScalarKernel kernel{KernelSignature::Make({InputType{id}}, out_type,
                                              /*is_varargs=*/true),
                        exec, MinMaxState::Init};
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  {
    const auto id = Type::FIXED_SIZE_BINARY;
    auto exec = FixedSizeBinaryScalarMinMax<Op>::Exec;
    OutputType out_type(ResolveMinOrMaxOutputType);
    ScalarKernel kernel{KernelSignature::Make({InputType{id}}, out_type,
                                              /*is_varargs=*/true),
                        exec, MinMaxState::Init};
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::NO_PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  return func;
}

const FunctionDoc equal_doc{"Compare values for equality (x == y)",
                            ("A null on either side emits a null comparison result."),
                            {"x", "y"}};

const FunctionDoc not_equal_doc{"Compare values for inequality (x != y)",
                                ("A null on either side emits a null comparison result."),
                                {"x", "y"}};

const FunctionDoc greater_doc{"Compare values for ordered inequality (x > y)",
                              ("A null on either side emits a null comparison result."),
                              {"x", "y"}};

const FunctionDoc greater_equal_doc{
    "Compare values for ordered inequality (x >= y)",
    ("A null on either side emits a null comparison result."),
    {"x", "y"}};

const FunctionDoc less_doc{"Compare values for ordered inequality (x < y)",
                           ("A null on either side emits a null comparison result."),
                           {"x", "y"}};

const FunctionDoc less_equal_doc{
    "Compare values for ordered inequality (x <= y)",
    ("A null on either side emits a null comparison result."),
    {"x", "y"}};

const FunctionDoc min_element_wise_doc{
    "Find the element-wise minimum value",
    ("Nulls are ignored (by default) or propagated.\n"
     "NaN is preferred over null, but not over any valid value."),
    {"*args"},
    "ElementWiseAggregateOptions"};

const FunctionDoc max_element_wise_doc{
    "Find the element-wise maximum value",
    ("Nulls are ignored (by default) or propagated.\n"
     "NaN is preferred over null, but not over any valid value."),
    {"*args"},
    "ElementWiseAggregateOptions"};

}  // namespace

void RegisterScalarComparison(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(MakeCompareFunction<Equal>("equal", equal_doc)));
  DCHECK_OK(
      registry->AddFunction(MakeCompareFunction<NotEqual>("not_equal", not_equal_doc)));

  auto greater = MakeCompareFunction<Greater>("greater", greater_doc);
  auto greater_equal =
      MakeCompareFunction<GreaterEqual>("greater_equal", greater_equal_doc);

  auto less = MakeFlippedFunction("less", *greater, less_doc);
  auto less_equal = MakeFlippedFunction("less_equal", *greater_equal, less_equal_doc);
  DCHECK_OK(registry->AddFunction(std::move(less)));
  DCHECK_OK(registry->AddFunction(std::move(less_equal)));
  DCHECK_OK(registry->AddFunction(std::move(greater)));
  DCHECK_OK(registry->AddFunction(std::move(greater_equal)));

  // ----------------------------------------------------------------------
  // Variadic element-wise functions

  auto min_element_wise =
      MakeScalarMinMax<Minimum>("min_element_wise", min_element_wise_doc);
  DCHECK_OK(registry->AddFunction(std::move(min_element_wise)));

  auto max_element_wise =
      MakeScalarMinMax<Maximum>("max_element_wise", max_element_wise_doc);
  DCHECK_OK(registry->AddFunction(std::move(max_element_wise)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
