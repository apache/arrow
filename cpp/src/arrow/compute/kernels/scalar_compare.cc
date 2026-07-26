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
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <optional>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/logging_internal.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using std::string_view;

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
};

// Implement Less, LessEqual by flipping arguments to Greater, GreaterEqual

template <typename Type, typename Op>
struct ComparePrimitiveArrayArray {
  using T = typename Type::c_type;
  static void Exec(const void* left_values_void, const void* right_values_void,
                   int64_t length, void* out_bitmap_void) {
    const T* left_values = reinterpret_cast<const T*>(left_values_void);
    const T* right_values = reinterpret_cast<const T*>(right_values_void);
    uint8_t* out_bitmap = reinterpret_cast<uint8_t*>(out_bitmap_void);
    static constexpr int kBatchSize = 32;
    int64_t num_batches = length / kBatchSize;
    uint32_t temp_output[kBatchSize];
    for (int64_t j = 0; j < num_batches; ++j) {
      for (int i = 0; i < kBatchSize; ++i) {
        temp_output[i] = Op::template Call<bool, T, T>(nullptr, *left_values++,
                                                       *right_values++, nullptr);
      }
      bit_util::PackBits<kBatchSize>(temp_output, out_bitmap);
      out_bitmap += kBatchSize / 8;
    }
    int64_t bit_index = 0;
    for (int64_t j = kBatchSize * num_batches; j < length; ++j) {
      bit_util::SetBitTo(out_bitmap, bit_index++,
                         Op::template Call<bool, T, T>(nullptr, *left_values++,
                                                       *right_values++, nullptr));
    }
  }
};

template <typename Type, typename Op>
struct ComparePrimitiveArrayScalar {
  using T = typename Type::c_type;
  static void Exec(const void* left_values_void, const void* right_value_void,
                   int64_t length, void* out_bitmap_void) {
    const T* left_values = reinterpret_cast<const T*>(left_values_void);
    const T right_value = *reinterpret_cast<const T*>(right_value_void);
    uint8_t* out_bitmap = reinterpret_cast<uint8_t*>(out_bitmap_void);
    static constexpr int kBatchSize = 32;
    int64_t num_batches = length / kBatchSize;
    uint32_t temp_output[kBatchSize];
    for (int64_t j = 0; j < num_batches; ++j) {
      for (int i = 0; i < kBatchSize; ++i) {
        temp_output[i] =
            Op::template Call<bool, T, T>(nullptr, *left_values++, right_value, nullptr);
      }
      bit_util::PackBits<kBatchSize>(temp_output, out_bitmap);
      out_bitmap += kBatchSize / 8;
    }
    int64_t bit_index = 0;
    for (int64_t j = kBatchSize * num_batches; j < length; ++j) {
      bit_util::SetBitTo(
          out_bitmap, bit_index++,
          Op::template Call<bool, T, T>(nullptr, *left_values++, right_value, nullptr));
    }
  }
};

template <typename Type, typename Op>
struct ComparePrimitiveScalarArray {
  using T = typename Type::c_type;
  static void Exec(const void* left_value_void, const void* right_values_void,
                   int64_t length, void* out_bitmap_void) {
    const T left_value = *reinterpret_cast<const T*>(left_value_void);
    const T* right_values = reinterpret_cast<const T*>(right_values_void);
    uint8_t* out_bitmap = reinterpret_cast<uint8_t*>(out_bitmap_void);
    static constexpr int kBatchSize = 32;
    int64_t num_batches = length / kBatchSize;
    uint32_t temp_output[kBatchSize];
    for (int64_t j = 0; j < num_batches; ++j) {
      for (int i = 0; i < kBatchSize; ++i) {
        temp_output[i] =
            Op::template Call<bool, T, T>(nullptr, left_value, *right_values++, nullptr);
      }
      bit_util::PackBits<kBatchSize>(temp_output, out_bitmap);
      out_bitmap += kBatchSize / 8;
    }
    int64_t bit_index = 0;
    for (int64_t j = kBatchSize * num_batches; j < length; ++j) {
      bit_util::SetBitTo(
          out_bitmap, bit_index++,
          Op::template Call<bool, T, T>(nullptr, left_value, *right_values++, nullptr));
    }
  }
};

using BinaryKernel = void (*)(const void*, const void*, int64_t, void*);

struct CompareData : public KernelState {
  BinaryKernel func_aa;
  BinaryKernel func_sa;
  BinaryKernel func_as;
  CompareData(BinaryKernel func_aa, BinaryKernel func_sa, BinaryKernel func_as)
      : func_aa(func_aa), func_sa(func_sa), func_as(func_as) {}
};

template <typename Type>
struct CompareKernel {
  using T = typename Type::c_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto kernel = static_cast<const ScalarKernel*>(ctx->kernel());
    DCHECK(kernel);
    const auto kernel_data = checked_cast<const CompareData*>(kernel->data.get());

    ArraySpan* out_arr = out->array_span_mutable();

    // TODO: implement path for offset not multiple of 8
    const bool out_is_byte_aligned = out_arr->offset % 8 == 0;

    std::shared_ptr<Buffer> out_buffer_tmp;
    uint8_t* out_buffer;
    if (out_is_byte_aligned) {
      out_buffer = out_arr->buffers[1].data + out_arr->offset / 8;
    } else {
      ARROW_ASSIGN_OR_RAISE(out_buffer_tmp, ctx->AllocateBitmap(batch.length));
      out_buffer = out_buffer_tmp->mutable_data();
    }
    if (batch[0].is_array() && batch[1].is_array()) {
      kernel_data->func_aa(batch[0].array.GetValues<T>(1), batch[1].array.GetValues<T>(1),
                           batch.length, out_buffer);
    } else if (batch[1].is_scalar()) {
      T value = UnboxScalar<Type>::Unbox(*batch[1].scalar);
      kernel_data->func_as(batch[0].array.GetValues<T>(1), &value, batch.length,
                           out_buffer);
    } else {
      T value = UnboxScalar<Type>::Unbox(*batch[0].scalar);
      kernel_data->func_sa(&value, batch[1].array.GetValues<T>(1), batch.length,
                           out_buffer);
    }
    if (!out_is_byte_aligned) {
      ::arrow::internal::CopyBitmap(out_buffer, /*offset=*/0, batch.length,
                                    out_arr->buffers[1].data, out_arr->offset);
    }
    return Status::OK();
  }
};

template <typename Op>
struct CompareTimestamps {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& lhs = checked_cast<const TimestampType&>(*batch[0].type());
    const auto& rhs = checked_cast<const TimestampType&>(*batch[1].type());
    if (lhs.timezone().empty() ^ rhs.timezone().empty()) {
      return Status::Invalid(
          "Cannot compare timestamp with timezone to timestamp without timezone, got: ",
          lhs, " and ", rhs);
    }
    return CompareKernel<Int64Type>::Exec(ctx, batch, out);
  }
};

template <typename Op>
ScalarKernel GetCompareKernel(InputType ty, Type::type compare_type,
                              ArrayKernelExec exec) {
  ScalarKernel kernel;
  kernel.signature = KernelSignature::Make({ty, ty}, boolean());
  BinaryKernel func_aa =
      GeneratePhysicalNumericGeneric<BinaryKernel, ComparePrimitiveArrayArray, Op>(
          compare_type);
  BinaryKernel func_sa =
      GeneratePhysicalNumericGeneric<BinaryKernel, ComparePrimitiveScalarArray, Op>(
          compare_type);
  BinaryKernel func_as =
      GeneratePhysicalNumericGeneric<BinaryKernel, ComparePrimitiveArrayScalar, Op>(
          compare_type);
  kernel.data = std::make_shared<CompareData>(func_aa, func_sa, func_as);
  kernel.exec = exec;
  return kernel;
}

template <typename Op>
void AddPrimitiveCompare(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  ArrayKernelExec exec = GeneratePhysicalNumeric<CompareKernel>(ty);
  ScalarKernel kernel = GetCompareKernel<Op>(ty, ty->id(), exec);
  DCHECK_OK(func->AddKernel(kernel));
}

struct CompareFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));
    if (HasDecimal(*types)) {
      RETURN_NOT_OK(CastBinaryDecimalArgs(DecimalPromotion::kAdd, types));
    }

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    EnsureDictionaryDecoded(types);
    ReplaceNullWithOtherType(types);

    if (auto type = CommonNumeric(*types)) {
      ReplaceTypes(type, types);
    } else if (auto type = CommonTemporal(types->data(), types->size())) {
      ReplaceTypes(type, types);
    } else if (auto type = CommonBinary(types->data(), types->size())) {
      ReplaceTypes(type, types);
    }

    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

struct VarArgsCompareFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    EnsureDictionaryDecoded(types);

    if (auto type = CommonNumeric(*types)) {
      ReplaceTypes(type, types);
    } else if (auto type = CommonTemporal(types->data(), types->size())) {
      ReplaceTypes(type, types);
    }

    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

template <typename Op>
std::shared_ptr<ScalarFunction> MakeCompareFunction(std::string name, FunctionDoc doc) {
  auto func = std::make_shared<CompareFunction>(name, Arity::Binary(), std::move(doc));

  DCHECK_OK(func->AddKernel(
      {boolean(), boolean()}, boolean(),
      applicator::ScalarBinary<BooleanType, BooleanType, BooleanType, Op>::Exec));

  for (const std::shared_ptr<DataType>& ty : NumericTypes()) {
    AddPrimitiveCompare<Op>(ty, func.get());
  }
  AddPrimitiveCompare<Op>(date32(), func.get());
  AddPrimitiveCompare<Op>(date64(), func.get());

  // Add timestamp kernels
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    ScalarKernel kernel =
        GetCompareKernel<Op>(in_type, Type::INT64, CompareTimestamps<Op>::Exec);
    DCHECK_OK(func->AddKernel(kernel));
  }

  // Duration
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::DurationTypeUnit(unit));
    ArrayKernelExec exec = GeneratePhysicalNumeric<CompareKernel>(int64());
    DCHECK_OK(func->AddKernel(GetCompareKernel<Op>(in_type, Type::INT64, exec)));
  }

  // Time32 and Time64
  for (auto unit : {TimeUnit::SECOND, TimeUnit::MILLI}) {
    InputType in_type(match::Time32TypeUnit(unit));
    ArrayKernelExec exec = GeneratePhysicalNumeric<CompareKernel>(int32());
    DCHECK_OK(func->AddKernel(GetCompareKernel<Op>(in_type, Type::INT32, exec)));
  }
  for (auto unit : {TimeUnit::MICRO, TimeUnit::NANO}) {
    InputType in_type(match::Time64TypeUnit(unit));
    ArrayKernelExec exec = GeneratePhysicalNumeric<CompareKernel>(int64());
    DCHECK_OK(func->AddKernel(GetCompareKernel<Op>(in_type, Type::INT64, exec)));
  }

  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    auto exec =
        GenerateVarBinaryBase<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(*ty);
    DCHECK_OK(func->AddKernel({ty, ty}, boolean(), std::move(exec)));
  }
  for (const auto& ty : BinaryViewTypes()) {
    auto exec =
        GenerateVarBinaryViewBase<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(
            *ty);
    DCHECK_OK(func->AddKernel({ty, ty}, boolean(), std::move(exec)));
  }

  for (const auto id : {Type::DECIMAL128, Type::DECIMAL256}) {
    auto exec = GenerateDecimal<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(id);
    DCHECK_OK(func->AddKernel({InputType(id), InputType(id)}, boolean(), std::move(exec),
                              /*init=*/nullptr, DecimalsHaveSameScale()));
  }

  {
    auto exec =
        applicator::ScalarBinaryEqualTypes<BooleanType, FixedSizeBinaryType, Op>::Exec;
    auto ty = InputType(Type::FIXED_SIZE_BINARY);
    DCHECK_OK(func->AddKernel({ty, ty}, boolean(), std::move(exec)));
  }

  return func;
}

struct FlippedData : public CompareData {
  ArrayKernelExec unflipped_exec;
  explicit FlippedData(ArrayKernelExec unflipped_exec, BinaryKernel func_aa = nullptr,
                       BinaryKernel func_sa = nullptr, BinaryKernel func_as = nullptr)
      : CompareData{func_aa, func_sa, func_as}, unflipped_exec(unflipped_exec) {}
};

Status FlippedCompare(KernelContext* ctx, const ExecSpan& span, ExecResult* out) {
  const auto kernel = static_cast<const ScalarKernel*>(ctx->kernel());
  const auto kernel_data = checked_cast<const FlippedData*>(kernel->data.get());
  ExecSpan flipped_span = span;
  std::swap(flipped_span.values[0], flipped_span.values[1]);
  return kernel_data->unflipped_exec(ctx, flipped_span, out);
}

std::shared_ptr<ScalarFunction> MakeFlippedCompare(std::string name,
                                                   const ScalarFunction& func,
                                                   FunctionDoc doc) {
  auto flipped_func =
      std::make_shared<CompareFunction>(name, Arity::Binary(), std::move(doc));
  for (const ScalarKernel* kernel : func.kernels()) {
    ScalarKernel flipped_kernel = *kernel;
    if (kernel->data) {
      auto compare_data = checked_cast<const CompareData*>(kernel->data.get());
      flipped_kernel.data =
          std::make_shared<FlippedData>(kernel->exec, compare_data->func_aa,
                                        compare_data->func_sa, compare_data->func_as);
    } else {
      flipped_kernel.data = std::make_shared<FlippedData>(kernel->exec);
    }
    flipped_kernel.exec = FlippedCompare;
    DCHECK_OK(flipped_func->AddKernel(std::move(flipped_kernel)));
  }
  return flipped_func;
}

using MinMaxState = OptionsWrapper<ElementWiseAggregateOptions>;

// Implement a variadic scalar min/max kernel.
template <typename OutType, typename Op>
struct ScalarMinMax {
  using OutValue = typename GetOutputType<OutType>::T;

  static Result<std::shared_ptr<Scalar>> ExecScalar(
      const ExecSpan& batch, const ElementWiseAggregateOptions& options,
      std::shared_ptr<DataType> type) {
    // All arguments are scalar
    OutValue value{};
    bool valid = false;
    for (const ExecValue& arg : batch.values) {
      // Ignore non-scalar arguments so we can use it in the mixed-scalar-and-array case
      if (!arg.is_scalar()) continue;
      const Scalar& scalar = *arg.scalar;
      if (!scalar.is_valid) {
        if (options.skip_nulls) continue;
        valid = false;
        break;
      }
      if (!valid) {
        value = UnboxScalar<OutType>::Unbox(scalar);
        valid = true;
      } else {
        value = Op::template Call<OutValue, OutValue, OutValue>(
            value, UnboxScalar<OutType>::Unbox(scalar));
      }
    }
    if (valid) {
      return MakeScalar(std::move(type), std::move(value));
    } else {
      return MakeNullScalar(std::move(type));
    }
  }

  // Fold left/right into out_values/out_valid a word at a time. Each validity
  // word is read once to compute out validity (OR/AND) and pick the value per
  // lane (left, right, or Op); bitmap visitors expose neither input's validity
  // word, forcing a second validity read
  static int64_t CombineWordwise(const OutValue* left, const uint8_t* left_valid,
                                 int64_t left_offset, const OutValue* right,
                                 const uint8_t* right_valid, int64_t right_offset,
                                 bool skip_nulls, int64_t length, OutValue* out_values,
                                 uint8_t* out_valid) {
    auto left_reader = ::arrow::internal::BitmapUInt64Reader(
        left_valid, left_valid ? left_offset : 0, left_valid ? length : 0);
    auto right_reader = ::arrow::internal::BitmapUInt64Reader(
        right_valid, right_valid ? right_offset : 0, right_valid ? length : 0);

    int64_t null_count = 0;
    int64_t i = 0;
    for (int64_t words = length / 64; words > 0; --words) {
      const uint64_t left_word = left_valid ? left_reader.NextWord() : ~uint64_t(0);
      const uint64_t right_word = right_valid ? right_reader.NextWord() : ~uint64_t(0);
      const uint64_t out_word =
          skip_nulls ? (left_word | right_word) : (left_word & right_word);
      // out_valid is allocated at bit offset 0, so store the word directly
      const uint64_t out_word_le = bit_util::ToLittleEndian(out_word);
      std::memcpy(out_valid + i / 8, &out_word_le, sizeof(out_word_le));
      null_count += 64 - std::popcount(out_word);
      if (out_word == 0) {
        // All null: the values are never read, so skip them
        i += 64;
      } else if (left_word == ~uint64_t(0) && right_word == ~uint64_t(0)) {
        // All valid: no per-lane validity check
        for (int j = 0; j < 64; ++j, ++i) {
          out_values[i] =
              Op::template Call<OutValue, OutValue, OutValue>(left[i], right[i]);
        }
      } else if (!skip_nulls) {
        for (int j = 0; j < 64; ++j, ++i) {
          out_values[i] =
              Op::template Call<OutValue, OutValue, OutValue>(left[i], right[i]);
        }
      } else {
        for (int j = 0; j < 64; ++j, ++i) {
          out_values[i] =
              CombineOne((left_word >> j) & 1, (right_word >> j) & 1, left[i], right[i]);
        }
      }
    }
    for (int bit = 0; i < length; ++i, ++bit) {
      const bool left_bit = !left_valid || bit_util::GetBit(left_valid, left_offset + i);
      const bool right_bit =
          !right_valid || bit_util::GetBit(right_valid, right_offset + i);
      const bool out_bit = skip_nulls ? (left_bit || right_bit) : (left_bit && right_bit);
      bit_util::SetBitTo(out_valid, i, out_bit);
      null_count += !out_bit;
      if (out_bit) out_values[i] = CombineOne(left_bit, right_bit, left[i], right[i]);
    }
    return null_count;
  }

  // Seed the accumulator from the first two arrays in a single pass
  static Status CombineArrays(KernelContext* ctx, const ArraySpan& lhs,
                              const ArraySpan& rhs, bool skip_nulls, ArrayData* output) {
    const int64_t length = output->length;
    const OutValue* left = lhs.GetValues<OutValue>(1);
    const OutValue* right = rhs.GetValues<OutValue>(1);
    OutValue* out_values = output->GetMutableValues<OutValue>(1);
    const uint8_t* left_valid = lhs.MayHaveNulls() ? lhs.buffers[0].data : nullptr;
    const uint8_t* right_valid = rhs.MayHaveNulls() ? rhs.buffers[0].data : nullptr;

    if (!left_valid && !right_valid) {
      for (int64_t i = 0; i < length; ++i) {
        out_values[i] =
            Op::template Call<OutValue, OutValue, OutValue>(left[i], right[i]);
      }
      output->buffers[0] = nullptr;
      output->null_count = 0;
      return Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(length));
    // CombineWordwise skips all-null words, so pre-zero the values to keep those
    // slots initialized for later reads
    std::memset(out_values, 0, static_cast<size_t>(length) * sizeof(OutValue));
    output->null_count = CombineWordwise(left, left_valid, lhs.offset, right, right_valid,
                                         rhs.offset, skip_nulls, length, out_values,
                                         output->buffers[0]->mutable_data());
    if (output->null_count == 0) {
      output->buffers[0] = nullptr;
    }
    return Status::OK();
  }

  // Seed the accumulator from a single array (result == that array)
  static Status CopyArrayToOutput(KernelContext* ctx, const ArraySpan& arr,
                                  ArrayData* output) {
    const int64_t length = output->length;
    const OutValue* arr_values = arr.GetValues<OutValue>(1);
    OutValue* out_values = output->GetMutableValues<OutValue>(1);
    std::copy(arr_values, arr_values + length, out_values);
    if (arr.MayHaveNulls()) {
      ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(length));
      ::arrow::internal::CopyBitmap(arr.buffers[0].data, arr.offset, length,
                                    output->buffers[0]->mutable_data(),
                                    /*dest_offset=*/0);
      output->null_count = arr.null_count;
    } else {
      output->buffers[0] = nullptr;
      output->null_count = 0;
    }
    return Status::OK();
  }

  // Fold `arr` into the accumulator already in `output` (buffers[0] null =>
  // all valid), in place, leaving a result that can be folded again
  static Status FoldArrayIntoOutput(KernelContext* ctx, const ArraySpan& arr,
                                    bool skip_nulls, ArrayData* output) {
    const int64_t length = output->length;
    OutValue* acc_values = output->GetMutableValues<OutValue>(1);
    const OutValue* arr_values = arr.GetValues<OutValue>(1);
    // Keep the current validity buffer alive while CombineWordwise writes a new one
    std::shared_ptr<Buffer> acc_valid_buf = output->buffers[0];
    const uint8_t* acc_valid = acc_valid_buf ? acc_valid_buf->data() : nullptr;
    const uint8_t* arr_valid = arr.MayHaveNulls() ? arr.buffers[0].data : nullptr;

    if (!acc_valid && !arr_valid) {
      for (int64_t i = 0; i < length; ++i) {
        acc_values[i] =
            Op::template Call<OutValue, OutValue, OutValue>(acc_values[i], arr_values[i]);
      }
      output->null_count = 0;
      return Status::OK();
    }

    // Accumulator all valid with skip_nulls: the result stays all valid, so no
    // output bitmap is needed. Where the array is null the accumulator already
    // holds the right value
    if (!acc_valid && skip_nulls) {
      auto reader = ::arrow::internal::BitmapUInt64Reader(arr_valid, arr.offset, length);
      int64_t i = 0;
      for (int64_t words = length / 64; words > 0; --words) {
        const uint64_t word = reader.NextWord();
        if (word == ~uint64_t(0)) {
          for (int j = 0; j < 64; ++j, ++i) {
            acc_values[i] = Op::template Call<OutValue, OutValue, OutValue>(
                acc_values[i], arr_values[i]);
          }
        } else if (word == 0) {
          i += 64;
        } else {
          for (int j = 0; j < 64; ++j, ++i) {
            const OutValue combined = Op::template Call<OutValue, OutValue, OutValue>(
                acc_values[i], arr_values[i]);
            acc_values[i] = ((word >> j) & 1) ? combined : acc_values[i];
          }
        }
      }
      for (; i < length; ++i) {
        if (bit_util::GetBit(arr_valid, arr.offset + i)) {
          acc_values[i] = Op::template Call<OutValue, OutValue, OutValue>(acc_values[i],
                                                                          arr_values[i]);
        }
      }
      output->null_count = 0;
      return Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(length));
    // The accumulator's validity is always stored at bit offset 0
    output->null_count = CombineWordwise(
        acc_values, acc_valid, /*left_offset=*/0, arr_values, arr_valid, arr.offset,
        skip_nulls, length, acc_values, output->buffers[0]->mutable_data());
    if (output->null_count == 0) {
      output->buffers[0] = nullptr;
    }
    return Status::OK();
  }

  // Op of both sides when both valid, else the valid side (left if neither, unused)
  static OutValue CombineOne(uint64_t lhs_valid, uint64_t rhs_valid, OutValue left_value,
                             OutValue right_value) {
    if (lhs_valid && rhs_valid) {
      return Op::template Call<OutValue, OutValue, OutValue>(left_value, right_value);
    }
    return rhs_valid ? right_value : left_value;
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ElementWiseAggregateOptions& options = MinMaxState::Get(ctx);
    const bool skip_nulls = options.skip_nulls;
    ArrayData* output = out->array_data().get();
    const int64_t length = output->length;

    // Fold the scalar arguments into one seed, then fold each array into the
    // output a word at a time
    const size_t scalar_count = static_cast<size_t>(
        std::count_if(batch.values.begin(), batch.values.end(),
                      [](const ExecValue& v) { return v.is_scalar(); }));

    std::vector<const ArraySpan*> arrays;
    arrays.reserve(batch.values.size());
    for (const auto& value : batch.values) {
      if (value.is_array()) arrays.push_back(&value.array);
    }

    std::optional<OutValue> seed;
    if (scalar_count > 0) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> temp_scalar,
                            ExecScalar(batch, options, out->type()->GetSharedPtr()));
      if (temp_scalar->is_valid) {
        seed = UnboxScalar<OutType>::Unbox(*temp_scalar);
      } else if (!skip_nulls) {
        // A null scalar with skip_nulls=false makes every output slot null
        ARROW_ASSIGN_OR_RAISE(
            auto array, MakeArrayFromScalar(*temp_scalar, length, ctx->memory_pool()));
        out->value = std::move(array->data());
        return Status::OK();
      }
    }

    OutValue* out_values = output->GetMutableValues<OutValue>(1);

    if (seed.has_value()) {
      std::fill(out_values, out_values + length, *seed);
      output->buffers[0] = nullptr;
      output->null_count = 0;
      for (const ArraySpan* arr : arrays) {
        RETURN_NOT_OK(FoldArrayIntoOutput(ctx, *arr, skip_nulls, output));
      }
      return Status::OK();
    }

    if (arrays.empty()) {
      // Every argument was a null scalar (skip_nulls): the result is all null
      if (length > 0) {
        ARROW_ASSIGN_OR_RAISE(output->buffers[0], ctx->AllocateBitmap(length));
      }
      output->null_count = length;
      return Status::OK();
    }
    if (arrays.size() == 1) {
      return CopyArrayToOutput(ctx, *arrays[0], output);
    }
    RETURN_NOT_OK(CombineArrays(ctx, *arrays[0], *arrays[1], skip_nulls, output));
    for (size_t k = 2; k < arrays.size(); ++k) {
      RETURN_NOT_OK(FoldArrayIntoOutput(ctx, *arrays[k], skip_nulls, output));
    }
    return Status::OK();
  }
};

template <typename Type, typename Op>
struct BinaryScalarMinMax {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ElementWiseAggregateOptions& options = MinMaxState::Get(ctx);
    // Presize data to avoid reallocations, using an estimation of final size.
    int64_t estimated_final_size = EstimateOutputSize(batch);
    BuilderType builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    RETURN_NOT_OK(builder.ReserveData(estimated_final_size));

    for (int64_t row = 0; row < batch.length; row++) {
      std::optional<string_view> result;
      auto visit_value = [&](string_view value) {
        result = !result ? value : Op::Call(*result, value);
      };

      for (int col = 0; col < batch.num_values(); col++) {
        if (batch[col].is_scalar()) {
          const Scalar& scalar = *batch[col].scalar;
          if (scalar.is_valid) {
            visit_value(UnboxScalar<Type>::Unbox(scalar));
          } else if (!options.skip_nulls) {
            result = std::nullopt;
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
            result = std::nullopt;
            break;
          }
        }
      }

      RETURN_NOT_OK(builder.AppendOrNull(result));
    }

    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder.Finish(&string_array));
    out->value = std::move(string_array->data());
    out->array_data()->type = batch[0].type()->GetSharedPtr();
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
    const DataType* batch_type = batch[0].type();
    const auto binary_type = checked_cast<const FixedSizeBinaryType*>(batch_type);
    int32_t byte_width = binary_type->byte_width();
    // Presize data to avoid reallocations.
    int64_t estimated_final_size = batch.length * byte_width;
    FixedSizeBinaryBuilder builder(batch_type->GetSharedPtr());
    RETURN_NOT_OK(builder.Reserve(batch.length));
    RETURN_NOT_OK(builder.ReserveData(estimated_final_size));

    std::vector<string_view> valid_cols(batch.num_values());
    for (int64_t row = 0; row < batch.length; row++) {
      string_view result;
      auto visit_value = [&](string_view value) {
        result = result.empty() ? value : Op::Call(result, value);
      };

      int num_valid_values = 0;
      for (int col = 0; col < batch.num_values(); col++) {
        if (batch[col].is_scalar()) {
          const Scalar& scalar = *batch[col].scalar;
          if (scalar.is_valid) {
            visit_value(UnboxScalar<FixedSizeBinaryType>::Unbox(scalar));
            num_valid_values += 1;
          } else if (!options.skip_nulls) {
            // If we encounter a null, exit the loop and mark num_row_values to
            // be 0 so we append a null
            num_valid_values = 0;
            break;
          }
        } else {
          const ArraySpan& array = batch[col].array;
          if (!array.MayHaveNulls() ||
              bit_util::GetBit(array.buffers[0].data, array.offset + row)) {
            const auto data = array.GetValues<uint8_t>(1, /*absolute_offset=*/0);
            visit_value(string_view(
                reinterpret_cast<const char*>(data) + row * byte_width, byte_width));
            num_valid_values += 1;
          } else if (!options.skip_nulls) {
            // If we encounter a null, exit the loop and mark num_row_values to
            // be 0 so we append a null
            num_valid_values = 0;
            break;
          }
        }
      }

      if (num_valid_values == 0) {
        builder.UnsafeAppendNull();
      } else {
        builder.UnsafeAppend(result);
      }
    }

    std::shared_ptr<Array> string_array;
    RETURN_NOT_OK(builder.Finish(&string_array));
    out->value = std::move(string_array->data());
    out->array_data()->type = batch[0].type()->GetSharedPtr();
    DCHECK_EQ(batch.length, out->array_data()->length);
    return Status::OK();
  }
};

Result<TypeHolder> ResolveMinOrMaxOutputType(KernelContext*,
                                             const std::vector<TypeHolder>& types) {
  if (types.empty()) {
    return null();
  }
  auto first_type = types[0].type;
  for (size_t i = 1; i < types.size(); ++i) {
    auto type = types[i].type;
    if (*type != *first_type) {
      return Status::NotImplemented(
          "Different input types not supported for {min, max}_element_wise");
    }
  }
  return first_type;
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
  for (const auto& ty : DurationTypes()) {
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

  auto less = MakeFlippedCompare("less", *greater, less_doc);
  auto less_equal = MakeFlippedCompare("less_equal", *greater_equal, less_equal_doc);
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
