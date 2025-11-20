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
// under the License.#include "arrow/compute/api_vector.h"

#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"

namespace arrow::compute::internal {

namespace {

// ----------------------------------------------------------------------
// InversePermutation

const FunctionDoc inverse_permutation_doc(
    "Return the inverse permutation of the given indices",
    "For the `i`-th `index` in `indices`, the `index`-th output is `i`", {"indices"});

const InversePermutationOptions* GetDefaultInversePermutationOptions() {
  static const auto kDefaultInversePermutationOptions =
      InversePermutationOptions::Defaults();
  return &kDefaultInversePermutationOptions;
}

using InversePermutationState = OptionsWrapper<InversePermutationOptions>;

/// Resolve the output type of inverse_permutation. The output type is specified in the
/// options, and if null, set it to the input type. The output type must be signed
/// integer.
Result<TypeHolder> ResolveInversePermutationOutputType(
    KernelContext* ctx, const std::vector<TypeHolder>& input_types) {
  DCHECK_EQ(input_types.size(), 1);
  DCHECK_NE(input_types[0], nullptr);

  std::shared_ptr<DataType> output_type = InversePermutationState::Get(ctx).output_type;
  if (!output_type) {
    output_type = input_types[0].owned_type;
  }
  if (!is_signed_integer(output_type->id())) {
    return Status::TypeError(
        "Output type of inverse_permutation must be signed integer, got " +
        output_type->ToString());
  }

  return TypeHolder(std::move(output_type));
}

template <typename ExecType>
struct InversePermutationImpl {
  using ThisType = InversePermutationImpl<ExecType>;
  using IndexType = typename ExecType::IndexType;
  using IndexCType = typename IndexType::c_type;
  using ShapeType = typename ExecType::ShapeType;

  static Result<std::shared_ptr<ArrayData>> Exec(
      KernelContext* ctx, const ShapeType& indices, int64_t input_length,
      const std::shared_ptr<DataType>& input_type) {
    const auto& options = InversePermutationState::Get(ctx);

    // Apply default options semantics.
    int64_t output_length = options.max_index < 0 ? input_length : options.max_index + 1;
    std::shared_ptr<DataType> output_type = options.output_type;
    if (!output_type) {
      output_type = input_type;
    }

    ThisType impl(ctx, indices, input_length, output_length);
    RETURN_NOT_OK(VisitTypeInline(*output_type, &impl));

    return ArrayData::Make(std::move(output_type), output_length,
                           {std::move(impl.validity_buf_), std::move(impl.data_buf_)});
  }

  template <typename Type>
  enable_if_t<is_integer_type<Type>::value, Status> Visit(const Type& output_type) {
    using OutputCType = typename Type::c_type;

    RETURN_NOT_OK(CheckInput(output_type));

    // Dispatch the execution based on whether there are likely many nulls in the output.
    // - If many nulls (i.e. the output is "sparse"), preallocate an all-false validity
    // buffer and a zero-initialized data buffer (just to avoid exposing previous memory
    // contents - even if it is shadowed by the validity bit). The subsequent processing
    // will fill the valid values only.
    // - Otherwise (i.e. the output is "dense"), the validity buffer is lazily allocated
    // and initialized all-true in the subsequent processing only when needed. The data
    // buffer is preallocated and filled with "impossible" values (that is, input_length -
    // note that the range of inverse_permutation is [0, input_length)) for the subsequent
    // processing to detect validity.
    if (LikelyManyNulls()) {
      RETURN_NOT_OK(AllocateValidityBufAndFill(false));
      RETURN_NOT_OK(AllocateDataBufAndZero(output_type));
      return Execute<Type, true>();
    } else {
      RETURN_NOT_OK(
          AllocateDataBufAndFill(output_type, static_cast<OutputCType>(input_length_)));
      return Execute<Type, false>();
    }
  }

  Status Visit(const DataType& output_type) {
    DCHECK(false) << "Shouldn't reach here";
    return Status::Invalid("Shouldn't reach here");
  }

 private:
  KernelContext* ctx_;
  const ShapeType& indices_;
  const int64_t input_length_;
  const int64_t output_length_;

  std::shared_ptr<Buffer> validity_buf_;
  std::shared_ptr<Buffer> data_buf_;

  InversePermutationImpl(KernelContext* ctx, const ShapeType& indices,
                         int64_t input_length, int64_t output_length)
      : ctx_(ctx),
        indices_(indices),
        input_length_(input_length),
        output_length_(output_length) {}

  template <typename Type>
  Status CheckInput(const Type& output_type) {
    using OutputCType = typename Type::c_type;

    if (static_cast<int64_t>(std::numeric_limits<OutputCType>::max()) < input_length_) {
      return Status::Invalid(
          "Output type " + output_type.ToString() +
          " of inverse_permutation is insufficient to store indices of length " +
          std::to_string(input_length_));
    }

    return Status::OK();
  }

  bool LikelyManyNulls() { return output_length_ > 2 * input_length_; }

  Status AllocateValidityBufAndFill(bool valid) {
    DCHECK_EQ(validity_buf_, nullptr);

    ARROW_ASSIGN_OR_RAISE(validity_buf_, ctx_->Allocate(output_length_));
    auto validity = validity_buf_->mutable_data_as<uint8_t>();
    std::memset(validity, valid ? 0xff : 0, validity_buf_->capacity());

    return Status::OK();
  }

  Status AllocateDataBuf(const DataType& output_type) {
    DCHECK_EQ(data_buf_, nullptr);

    ARROW_ASSIGN_OR_RAISE(data_buf_,
                          ctx_->Allocate(output_length_ * output_type.byte_width()));

    return Status::OK();
  }

  Status AllocateDataBufAndZero(const DataType& output_type) {
    RETURN_NOT_OK(AllocateDataBuf(output_type));

    uint8_t* data = data_buf_->mutable_data();
    std::memset(data, 0, output_length_ * output_type.byte_width());

    return Status::OK();
  }

  template <typename Type, typename OutputCType = typename Type::c_type>
  Status AllocateDataBufAndFill(const Type& output_type, OutputCType value) {
    RETURN_NOT_OK(AllocateDataBuf(output_type));

    OutputCType* data = data_buf_->mutable_data_as<OutputCType>();
    for (int64_t i = 0; i < output_length_; ++i) {
      data[i] = value;
    }

    return Status::OK();
  }

  template <typename Type, bool likely_many_nulls>
  Status Execute() {
    using OutputCType = typename Type::c_type;

    uint8_t* validity = nullptr;
    if constexpr (likely_many_nulls) {
      DCHECK_NE(validity_buf_, nullptr);
      validity = validity_buf_->mutable_data_as<uint8_t>();
    } else {
      DCHECK_EQ(validity_buf_, nullptr);
    }
    DCHECK_NE(data_buf_, nullptr);
    OutputCType* data = data_buf_->mutable_data_as<OutputCType>();
    int64_t inverse = 0;
    RETURN_NOT_OK(ExecType::VisitIndices(
        indices_,
        [&](IndexCType index) {
          if (ARROW_PREDICT_FALSE(index < 0 ||
                                  static_cast<int64_t>(index) >= output_length_)) {
            return Status::IndexError("Index out of bounds: ", std::to_string(index));
          }
          data[index] = static_cast<OutputCType>(inverse);
          // If many nulls, set validity to true for valid values.
          if constexpr (likely_many_nulls) {
            bit_util::SetBitTo(validity, index, true);
          }
          ++inverse;
          return Status::OK();
        },
        [&]() {
          ++inverse;
          return Status::OK();
        }));

    // If not many nulls, run another pass iterating over the data to set the validity
    // to false if the value is "impossible". The validity buffer is on demand allocated
    // and initialized all-true when the first "impossible" value is seen.
    if constexpr (!likely_many_nulls) {
      for (int64_t i = 0; i < output_length_; ++i) {
        if (data[i] == static_cast<OutputCType>(input_length_)) {
          if (ARROW_PREDICT_FALSE(!validity_buf_)) {
            RETURN_NOT_OK(AllocateValidityBufAndFill(true));
            validity = validity_buf_->mutable_data_as<uint8_t>();
          }
          bit_util::SetBitTo(validity, i, false);
        }
      }
    }

    return Status::OK();
  }
};

template <typename Ignored, typename Type>
struct InversePermutation {
  using ThisType = InversePermutation<Ignored, Type>;
  using IndexType = Type;
  using ShapeType = ArraySpan;

  template <typename ValidFunc, typename NullFunc>
  static Status VisitIndices(const ArraySpan& span, ValidFunc&& valid_func,
                             NullFunc&& null_func) {
    return VisitArraySpanInline<IndexType>(span, std::forward<ValidFunc>(valid_func),
                                           std::forward<NullFunc>(null_func));
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    DCHECK_EQ(span.num_values(), 1);
    DCHECK(span[0].is_array());
    const auto& indices = span[0].array;
    ARROW_ASSIGN_OR_RAISE(
        result->value, InversePermutationImpl<ThisType>::Exec(
                           ctx, indices, indices.length, indices.type->GetSharedPtr()));
    return Status::OK();
  }
};

template <typename Ignored, typename Type>
struct InversePermutationChunked {
  using ThisType = InversePermutationChunked<Ignored, Type>;
  using IndexType = Type;
  using ShapeType = std::shared_ptr<ChunkedArray>;

  template <typename ValidFunc, typename NullFunc>
  static Status VisitIndices(const std::shared_ptr<ChunkedArray>& chunked_array,
                             ValidFunc&& valid_func, NullFunc&& null_func) {
    for (const auto& chunk : chunked_array->chunks()) {
      ArraySpan span(*chunk->data());
      RETURN_NOT_OK(VisitArraySpanInline<IndexType>(
          span, std::forward<ValidFunc>(valid_func), std::forward<NullFunc>(null_func)));
    }
    return Status::OK();
  }

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* result) {
    DCHECK_EQ(batch.num_values(), 1);
    DCHECK(batch[0].is_chunked_array());
    const auto& indices = batch[0].chunked_array();
    ARROW_ASSIGN_OR_RAISE(auto inverse_permutation,
                          InversePermutationImpl<ThisType>::Exec(
                              ctx, indices, indices->length(), indices->type()));
    *result =
        Datum(std::make_shared<ChunkedArray>(MakeArray(std::move(inverse_permutation))));
    return Status::OK();
  }
};

void RegisterVectorInversePermutation(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("inverse_permutation", Arity::Unary(),
                                                   inverse_permutation_doc,
                                                   GetDefaultInversePermutationOptions());

  auto add_kernel = [&function](Type::type type_id) {
    VectorKernel kernel;
    kernel.signature =
        KernelSignature::Make({InputType(match::SameTypeId(type_id))},
                              OutputType(ResolveInversePermutationOutputType));
    kernel.init = InversePermutationState::Init;
    kernel.exec = GenerateInteger<InversePermutation, void, ArrayKernelExec>(type_id);
    kernel.exec_chunked =
        GenerateInteger<InversePermutationChunked, void, VectorKernel::ChunkedExec>(
            type_id);
    kernel.can_execute_chunkwise = false;
    kernel.output_chunked = false;
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  };
  for (const auto& t : SignedIntTypes()) {
    add_kernel(t->id());
  }

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

// ----------------------------------------------------------------------
// Scatter

const FunctionDoc scatter_doc(
    "Scatter the values into specified positions according to the indices",
    "Place the `i`-th value at the position specified by the `i`-th index",
    {"values", "indices"});

const ScatterOptions* GetDefaultScatterOptions() {
  static const auto kDefaultScatterOptions = ScatterOptions::Defaults();
  return &kDefaultScatterOptions;
}

class ScatterMetaFunction : public MetaFunction {
 public:
  ScatterMetaFunction()
      : MetaFunction("scatter", Arity::Binary(), scatter_doc,
                     GetDefaultScatterOptions()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    DCHECK_EQ(args.size(), 2);
    const auto& values = args[0];
    const auto& indices = args[1];
    // Though the way how scatter is currently implemented may support record batch or
    // table, we don't want to promise that yet.
    if (!values.is_arraylike()) {
      return Status::NotImplemented("Scatter does not support " +
                                    ToString(values.kind()) + " values");
    }
    if (!indices.is_arraylike()) {
      return Status::NotImplemented("Scatter does not support " +
                                    ToString(values.kind()) + " indices");
    }
    auto* scatter_options = checked_cast<const ScatterOptions*>(options);
    if (values.length() != indices.length()) {
      return Status::Invalid(
          "Input and indices of scatter must have the same length, got " +
          std::to_string(values.length()) + " and " + std::to_string(indices.length()));
    }
    if (!is_signed_integer(indices.type()->id())) {
      return Status::TypeError("Indices of scatter must be of signed integer type, got ",
                               indices.type()->ToString());
    }
    // Internally invoke Take(values, InversePermutation(indices)) to implement scatter.
    // For example, with
    //   values = [a, b, c, d, e, f, g]
    //   indices = [null, 0, 3, 2, 4, 1, 1]
    // the InversePermutation(indices) is
    // [1, 6, 3, 2, 4, null, null]  if max_index = 6.
    // and Take(values, InversePermutation(indices)) is
    // [b, g, d, c, e, null, null]  if max_index = 6.
    InversePermutationOptions inverse_permutation_options{
        scatter_options->max_index,
        // Use the smallest possible uint type to store inverse permutation.
        InferSmallestInversePermutationType(values.length())};
    ARROW_ASSIGN_OR_RAISE(auto inverse_permutation,
                          CallFunction("inverse_permutation", {indices},
                                       &inverse_permutation_options, ctx));
    TakeOptions take_options{/*boundcheck=*/false};
    return CallFunction("take", {values, inverse_permutation}, &take_options, ctx);
  }

 private:
  static std::shared_ptr<DataType> InferSmallestInversePermutationType(
      int64_t input_length) {
    DCHECK_GE(input_length, 0);
    if (input_length <= std::numeric_limits<int8_t>::max()) {
      return int8();
    } else if (input_length <= std::numeric_limits<int16_t>::max()) {
      return int16();
    } else if (input_length <= std::numeric_limits<int32_t>::max()) {
      return int32();
    } else {
      return int64();
    }
  }
};

void RegisterVectorScatter(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<ScatterMetaFunction>()));
}

}  // namespace

// ----------------------------------------------------------------------

void RegisterVectorSwizzle(FunctionRegistry* registry) {
  RegisterVectorInversePermutation(registry);
  RegisterVectorScatter(registry);
}

}  // namespace arrow::compute::internal
