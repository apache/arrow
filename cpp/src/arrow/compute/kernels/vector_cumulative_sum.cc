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

#include "arrow/array/array_base.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/result.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {


struct Add {
  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                                    Status*) {
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_unsigned_integer_value<T> Call(KernelContext*, Arg0 left,
                                                            Arg1 right, Status*) {
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static constexpr enable_if_signed_integer_value<T> Call(KernelContext*, Arg0 left,
                                                          Arg1 right, Status*) {
    return arrow::internal::SafeSignedAdd(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + right;
  }
};

struct AddChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                         Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(AddWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_value<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal_value<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + right;
  }
};
std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
  return KernelSignature::Make({InputType::Array(get_id.id)}, OutputType(FirstType));
}

}  // namespace

template <typename Type>
struct CumulativeSum {
  using CType = typename TypeTraits<Type>::CType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;

  CType Sum(ExecContext* ctx, std::shared_ptr<Array>& input, ArrayData* output,
            CType start, bool skip_nulls) {
    CType sum = start;
    CType* data = checked_cast<CType*>(input->data()->buffers[1]->data());
    CType* out_values = checked_cast<CType*>(output->buffers[1]->mutable_data());
    ArithmeticOptions options;
    bool set_null = false;
    for (size_t i = input->offset(); i < input->length(); ++i) {
      if (set_null) {
        out_values[i] = NULL;
      } else if (input->IsNull(i) && !skip_nulls) {
        out_values[i] = NULL;
        set_null = true;
      } else {
        // Datum value_datum(data[i]);
        // Datum sum_datum(sum);
        // auto result = Add(value_datum, sum_datum, options, ctx);
        // sum = result_scalar.value;

        sum = Add::Call(data[i], sum);
        out_values[i] = sum;
      }
    }

    return sum;
  }

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = OptionsWrapper<CumulativeSumOptions>::Get(ctx);
    std::shared_ptr<Scalar>& start_scalar = options.start;
    bool skip_nulls = options.skip_nulls;

    CType start = 0;
    if (start_scalar) {
      if (start_scalar->is_valid()) {
        if (start_scalar->type()->id() != TypeTraits<Type>::type_singleton()->id()) {
          return Status::Invalid(
              "Types of array values and starting value do not match.");
        }
        start = UnboxScalar<Type>::Unbox(*start_scalar);
      }
    }

    switch (batch[0].kind()) {
      case Datum::ARRAY: {
        std::shared_ptr<Array> input = batch[0].make_array();
        ArrayData* output = out->array().get();

        output->length = input->data()->length;
        *output->type = std::move(input->type());
        uint8_t* out_bitmap = output->buffers[0]->mutable_data();
        int64_t out_offset = input->offset();

        if (input->data()->MayHaveNulls()) {
          arrow::internal::CopyBitmap(input->null_bitmap_data(), input->offset(),
                                      input->length(), out_bitmap, out_offset);
          output->null_count = input->null_count();
        } else {
          bit_util::SetBitsTo(out_bitmap, out_offset, input->length(), true);
          output->null_count = 0;
        }

        Sum(ctx->exec_context(), input, output, start, skip_nulls);
        return Status::OK();
      }
      case Datum::CHUNKED_ARRAY: {
        const auto& input = batch[0].chunked_array();

        ArrayVector out_chunks;
        for (const auto& chunk : input->chunks()) {
          auto out_chunk = std::make_shared<ArrayData>(
              chunk->type(), chunk->length(), chunk->null_count(), chunk->offset());

          uint8_t* out_chunk_bitmap = out_chunk->buffers[0]->mutable_data();
          if (chunk->data()->MayHaveNulls()) {
            arrow::internal::CopyBitmap(chunk->null_bitmap_data(), chunk->offset(),
                                        chunk->length(), out_chunk_bitmap,
                                        out_chunk->offset());
            out_chunk->null_count = chunk->null_count();
          } else {
            bit_util::SetBitsTo(out_chunk_bitmap, out_chunk->offset(), chunk->length(),
                                true);
            out_chunk->null_count = 0;
          }

          CType last_value =
              Sum(ctx->exec_context(), chunk, out_chunk, start, skip_nulls);
          start = last_value;
          out_chunks.push_back(MakeArray(std::move(out_chunk)));
        }

        *out->chunked_array() = ChunkedArray(out_chunks, input->type());
        return Status::OK();
      }
      default:
        return Status::NotImplemented(
            "Unsupported input type for function 'cumulative_sum': ",
            batch[0].ToString());
    }
  }
};

const FunctionDoc cumulative_sum_doc(
    "Compute the cumulative sum over an array of numbers",
    ("`values` must be an array of numeric type values.\n"
     "`start` is a single value of the same type.\n"
     "Return an array which is the cumulative sum computed over `values.`\n"
     "Null entries remain in place but are not used in calucating sum.\n"
     "`start` is an optional starting sum of computation."),
    {"values", "start"});

void RegisterVectorCumulativeSum(FunctionRegistry* registry) {
  auto cumulative_sum = std::make_shared<VectorFunction>(
      "cumulative_sum", Arity::Binary(), &cumulative_sum_doc);

  std::vector<std::shared_ptr<DataType>> types;
  types.insert(types.end(), IntTypes().begin(), IntTypes().end());
  types.insert(types.end(), FloatingPointTypes().begin(), IntTypes().end());
  types.insert(types.end(), TemporalTypes().begin(), TemporalTypes().end());
  types.push_back(duration(TimeUnit::SECOND));
  types.push_back(duration(TimeUnit::MILLI));
  types.push_back(duration(TimeUnit::MICRO));
  types.push_back(duration(TimeUnit::NANO));
  types.push_back(month_interval());

  for (auto ty : types) {
    VectorKernel kernel;
    kernel.can_execute_chunkwise = false;
    kernel.null_handling = NullHandling::type::INTERSECTION;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    kernel.signature = GetSignature(ty->id());
    kernel.exec = std::move(GenerateTypeAgnosticPrimitive<CumulativeSum>(ty));
    kernel.init = OptionsWrapper<CumulativeSumOptions>::Init;
    DCHECK_OK(cumulative_sum->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(cumulative_sum)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
