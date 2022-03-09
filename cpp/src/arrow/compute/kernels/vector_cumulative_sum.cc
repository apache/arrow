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
#include "arrow/compute/kernels/common.h"
#include "arrow/result.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename Type>
struct CumulativeSum {
  using CType = TypeTraits<Type>::CType;
  using ScalarType = TypeTraits<Type>::ScalarType;

  CType Sum(ExecContext* ctx, std::shared_ptr<Array>& input, ArrayData* output,
            CType start) {
    CType sum = start;
    CType* data = checked_cast<CType*>(input->data()->buffers[1]->data());
    CType* out_values = checked_cast<CType*>(output->buffers[1]->mutable_data());
    ArithmeticOptions options;
    for (size_t i = input->offset; i < input->length; ++i) {
      if (input->IsValid(i)) {
        Datum value_datum(data[i]);
        Datum sum_datum(sum);
        auto result = Add(value_datum, sum_datum, options, ctx);
        ScalarType result_scalar = result.ValueOrDie().scalar_as();
        sum = result_scalar.value;
        out_values[i] = sum;
      }
    }

    return sum;
  }

  Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = OptionsWrapper<CumulativeSumOptions<CType>>::Get(ctx);
    CType start = checked_cast<const ScalarType&>(options.start).value;

    switch (batch[0].kind()) {
      case Datum::ARRAY:
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

        Sum(ctx->exec_context(), input, output, start);
        return Status::OK();
      case Datum::CHUNKED_ARRAY:
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

          CType last_value = Sum(ctx->exec_context(), chunk, out_chunk, start);
          start = last_value;
          out_chunks.push_back(MakeArray(std::move(out_chunk)));
        }

        *out->chunked_array() = ChunkedArray(out_chunks, input->type());
        return Status::OK();
      default:
        return Status::NotImplemented(
            "Unsupported input type for function 'cumulative_sum': ",
            batch[0].ToString());
    }
  }

  static std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
    return KernelSignature::Make({InputType::Array(get_id.id)}, OutputType(FirstType));
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

  auto add_kernel = [&](detail::GetTypeId get_id, ArrayKernelExec exec) {
    VectorKernel kernel;
    kernel.can_execute_chunkwise = true;
    kernel.null_handling = NullHandling::type::INTERSECTION;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    kernel.signature = CumulativeSum<NumberType>::GetSignature(get_id.id);
    kernel.exec = std::move(exec);
    kernel.init = OptionsWrapper<CumulativeSumOptions>::Init;
    DCHECK_OK(cumulative_sum->AddKernel(std::move(kernel)));
  };

  for (auto ty : NumericTypes()) {
    add_kernel(ty, GenerateTypeAgnosticPrimitive<CumulativeSum>(ty));
  }

  for (auto ty : TemporalTypes()) {
    add_kernel(ty, GenerateTypeAgnosticPrimitive<CumulativeSum>(ty));
  }

  add_kernel(Type::DURATION,
             GenerateTypeAgnosticPrimitive<CumulativeSum>(Type::DURATION));
  add_kernel(Type::INTERVAL_MONTHS,
             GenerateTypeAgnosticPrimitive<CumulativeSum>(Type::INTERVAL_MONTHS));

  DCHECK_OK(registry->AddFunction(std::move(cumulative_sum)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
