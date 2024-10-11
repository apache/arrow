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
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow::compute::internal {

namespace {

// ----------------------------------------------------------------------
// ReverseIndices

const FunctionDoc reverse_indices_doc(
    "Return the reverse indices of the given indices",
    "For the `i`-th `index` in `indices`, the `index`-th output is `i`", {"indices"});

const ReverseIndicesOptions* GetDefaultReverseIndicesOptions() {
  static const auto kDefaultReverseIndicesOptions = ReverseIndicesOptions::Defaults();
  return &kDefaultReverseIndicesOptions;
}

using ReverseIndicesState = OptionsWrapper<ReverseIndicesOptions>;

/// Resolve the output type of reverse_indices. The output type is specified in the
/// options, and if null, set it to the input type. The output type must be integer.
Result<TypeHolder> ResolveReverseIndicesOutputType(
    KernelContext* ctx, const std::vector<TypeHolder>& input_types) {
  DCHECK_EQ(input_types.size(), 1);
  DCHECK_NE(input_types[0], nullptr);

  std::shared_ptr<DataType> output_type = ReverseIndicesState::Get(ctx).output_type;
  if (!output_type) {
    output_type = input_types[0].owned_type;
  }
  if (!is_integer(output_type->id())) {
    return Status::Invalid("Output type of reverse_indices must be integer, got " +
                           output_type->ToString());
  }

  return TypeHolder(std::move(output_type));
}

template <typename ExecType>
struct ReverseIndicesImpl {
  using ThisType = ReverseIndicesImpl<ExecType>;
  using IndexType = typename ExecType::IndexType;
  using IndexCType = typename IndexType::c_type;
  using ShapeType = typename ExecType::ShapeType;

  static Result<std::shared_ptr<ArrayData>> Exec(
      KernelContext* ctx, const ShapeType& indices, int64_t input_length,
      const std::shared_ptr<DataType>& input_type) {
    const auto& options = ReverseIndicesState::Get(ctx);

    // Apply default options semantics.
    int64_t output_length = options.output_length;
    if (output_length < 0) {
      output_length = input_length;
    }
    std::shared_ptr<DataType> output_type = options.output_type;
    if (!output_type) {
      output_type = input_type;
    }

    ThisType impl(ctx, indices, input_length, output_length);
    RETURN_NOT_OK(VisitTypeInline(*output_type, &impl));

    return ArrayData::Make(std::move(output_type), output_length,
                           {std::move(impl.validity_buf), std::move(impl.data_buf)});
  }

 private:
  KernelContext* ctx;
  const ShapeType& indices;
  const int64_t input_length;
  const int64_t output_length;

  std::shared_ptr<Buffer> validity_buf = nullptr;
  std::shared_ptr<Buffer> data_buf = nullptr;

 private:
  ReverseIndicesImpl(KernelContext* ctx, const ShapeType& indices, int64_t input_length,
                     int64_t output_length)
      : ctx(ctx),
        indices(indices),
        input_length(input_length),
        output_length(output_length) {}

  Status Visit(const DataType& output_type) {
    DCHECK(false) << "Shouldn't reach here";
    return Status::Invalid("Shouldn't reach here");
  }

  template <typename Type>
  enable_if_t<is_integer_type<Type>::value, Status> Visit(const Type& output_type) {
    using OutputCType = typename Type::c_type;

    RETURN_NOT_OK(CheckInput(output_type));

    // Dispatch the execution based on wether there are likely many nulls in the output.
    // - If many nulls (i.e. the output is "sparse"), preallocate an all-false validity
    // buffer and an uninitialized data buffer. The subsequent processing will fill the
    // valid values only.
    // - Otherwise (i.e. the output is "dense"), the validity buffer is lazily allocated
    // and initialized all-true in the subsequent processing only when needed. The data
    // buffer is preallocated and filled with all "impossible" values (that is,
    // input_length - note that the range of reverse_indices is [0, input_length)) for the
    // subsequent processing to detect validity.
    bool likely_many_nulls = LikelyManyNulls();
    if (likely_many_nulls) {
      RETURN_NOT_OK(AllocateValidityBufAndFill(false));
      RETURN_NOT_OK(AllocateDataBuf(output_type));
      return Execute<Type, true>();
    } else {
      RETURN_NOT_OK(
          AllocateDataBufAndFill(output_type, static_cast<OutputCType>(input_length)));
      return Execute<Type, false>();
    }
  }

  template <typename Type>
  Status CheckInput(const Type& output_type) {
    using OutputCType = typename Type::c_type;

    if constexpr (!std::is_same_v<OutputCType, uint64_t>) {
      if (static_cast<int64_t>(std::numeric_limits<OutputCType>::max()) < input_length) {
        return Status::Invalid(
            "Output type " + output_type.ToString() +
            " of reverse_indices is insufficient to store indices of length " +
            std::to_string(input_length));
      }
    }

    return Status::OK();
  }

  bool LikelyManyNulls() { return output_length > 2 * input_length; }

  Status AllocateValidityBufAndFill(bool valid) {
    DCHECK_EQ(validity_buf, nullptr);

    ARROW_ASSIGN_OR_RAISE(validity_buf, ctx->AllocateBitmap(output_length));
    auto validity = validity_buf->mutable_data_as<uint8_t>();
    std::memset(validity, valid ? 0xff : 0, bit_util::BytesForBits(output_length));

    return Status::OK();
  }

  Status AllocateDataBuf(const DataType& output_type) {
    DCHECK_EQ(data_buf, nullptr);

    ARROW_ASSIGN_OR_RAISE(data_buf,
                          ctx->Allocate(output_length * output_type.byte_width()));

    return Status::OK();
  }

  template <typename Type, typename OutputCType = typename Type::c_type>
  Status AllocateDataBufAndFill(const Type& output_type, OutputCType value) {
    RETURN_NOT_OK(AllocateDataBuf(output_type));

    OutputCType* data = data_buf->mutable_data_as<OutputCType>();
    for (int64_t i = 0; i < output_length; ++i) {
      data[i] = value;
    }

    return Status::OK();
  }

  template <typename Type, bool likely_many_nulls>
  Status Execute() {
    using OutputCType = typename Type::c_type;

    uint8_t* validity = nullptr;
    if constexpr (likely_many_nulls) {
      DCHECK_NE(validity_buf, nullptr);
      validity = validity_buf->mutable_data_as<uint8_t>();
    } else {
      DCHECK_EQ(validity_buf, nullptr);
    }
    DCHECK_NE(data_buf, nullptr);
    OutputCType* data = data_buf->mutable_data_as<OutputCType>();
    int64_t reverse_index = 0;
    RETURN_NOT_OK(ExecType::template VisitIndices(
        indices,
        [&](IndexCType index) {
          if (ARROW_PREDICT_TRUE(index >= 0 &&
                                 static_cast<int64_t>(index) < output_length)) {
            data[index] = static_cast<OutputCType>(reverse_index);
            // If many nulls, set validity to true for valid values.
            if constexpr (likely_many_nulls) {
              bit_util::SetBitTo(validity, index, true);
            }
          }
          ++reverse_index;
          return Status::OK();
        },
        [&]() {
          ++reverse_index;
          return Status::OK();
        }));

    // If not many nulls, run another pass iterating over the data to set the validity
    // to false if the value is "impossible". The validity buffer is on demand allocated
    // and initialized all-true when the first "impossible" value is seen.
    if constexpr (!likely_many_nulls) {
      for (int64_t i = 0; i < output_length; ++i) {
        if (ARROW_PREDICT_FALSE(data[i] == static_cast<OutputCType>(input_length))) {
          if (ARROW_PREDICT_FALSE(!validity_buf)) {
            RETURN_NOT_OK(AllocateValidityBufAndFill(true));
            validity = validity_buf->mutable_data_as<uint8_t>();
          }
          bit_util::SetBitTo(validity, i, false);
        }
      }
    }

    return Status::OK();
  }

  template <typename VISITOR, typename... ARGS>
  friend Status arrow::VisitTypeInline(const DataType&, VISITOR*, ARGS&&... args);
};

template <typename Ignored, typename Type>
struct ReverseIndices {
  using ThisType = ReverseIndices<Ignored, Type>;
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
        result->value, ReverseIndicesImpl<ThisType>::Exec(ctx, indices, indices.length,
                                                          indices.type->GetSharedPtr()));
    return Status::OK();
  }
};

template <typename Ignored, typename Type>
struct ReverseIndicesChunked {
  using ThisType = ReverseIndicesChunked<Ignored, Type>;
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
    ARROW_ASSIGN_OR_RAISE(*result, ReverseIndicesImpl<ThisType>::Exec(
                                       ctx, indices, indices->length(), indices->type()));
    return Status::OK();
  }
};

void RegisterVectorReverseIndices(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("reverse_indices", Arity::Unary(),
                                                   reverse_indices_doc,
                                                   GetDefaultReverseIndicesOptions());

  auto add_kernel = [&function](Type::type type_id) {
    VectorKernel kernel;
    kernel.signature = KernelSignature::Make({InputType(match::SameTypeId(type_id))},
                                             OutputType(ResolveReverseIndicesOutputType));
    kernel.init = ReverseIndicesState::Init;
    kernel.exec = GenerateInteger<ReverseIndices, void, ArrayKernelExec>(type_id);
    kernel.exec_chunked =
        GenerateInteger<ReverseIndicesChunked, void, VectorKernel::ChunkedExec>(type_id);
    kernel.can_execute_chunkwise = false;
    kernel.output_chunked = false;
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  };
  for (const auto& t : IntTypes()) {
    add_kernel(t->id());
  }

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

// ----------------------------------------------------------------------
// Permute

const FunctionDoc permute_doc(
    "Permute the values into specified positions according to the indices",
    "Place the `i`-th value at the position specified by the `i`-th index",
    {"values", "indices"});

const PermuteOptions* GetDefaultPermuteOptions() {
  static const auto kDefaultPermuteOptions = PermuteOptions::Defaults();
  return &kDefaultPermuteOptions;
}

class PermuteMetaFunction : public MetaFunction {
 public:
  PermuteMetaFunction()
      : MetaFunction("permute", Arity::Binary(), permute_doc,
                     GetDefaultPermuteOptions()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    DCHECK_EQ(args.size(), 2);
    const auto& values = args[0];
    // Though the way how permute is currently implemented may support record batch or
    // table, we don't want to promise that yet.
    if (!values.is_arraylike()) {
      return Status::NotImplemented("Permute does not support " +
                                    ToString(values.kind()) + " argument");
    }
    const auto& indices = args[1];
    auto* permute_options = checked_cast<const PermuteOptions*>(options);
    if (values.length() != indices.length()) {
      return Status::Invalid(
          "Input and indices of permute must have the same length, got " +
          std::to_string(values.length()) + " and " + std::to_string(indices.length()));
    }
    if (!is_integer(indices.type()->id())) {
      return Status::Invalid("Indices of permute must be of integer type, got ",
                             indices.type()->ToString());
    }
    // Apply default options semantics.
    int64_t output_length = permute_options->output_length;
    if (output_length < 0) {
      output_length = values.length();
    }
    // Internally invoke take(values, reverse_indices(indices)) to implement permute.
    // For example, with
    //   values = [a, b, c, d, e, f, g]
    //   indices = [null, 0, 3, 2, 4, 1, 1]
    // the reverse_indices(indices) is
    //   [1, 6, 3]                    if output_length = 3,
    //   [1, 6, 3, 2, 4, null, null]  if output_length = 7.
    // and take(values, reverse_indices(indices)) is
    //   [b, g, d]                    if output_length = 3,
    //   [b, g, d, c, e, null, null]  if output_length = 7.
    ReverseIndicesOptions reverse_indices_options{
        output_length,
        // Use the smallest possible uint type to store reverse indices.
        InferSmallestReverseIndicesType(values.length())};
    ARROW_ASSIGN_OR_RAISE(
        auto reverse_indices,
        CallFunction("reverse_indices", {indices}, &reverse_indices_options, ctx));
    TakeOptions take_options{/*boundcheck=*/false};
    return CallFunction("take", {values, reverse_indices}, &take_options, ctx);
  }

 private:
  static std::shared_ptr<DataType> InferSmallestReverseIndicesType(int64_t input_length) {
    if (input_length <= std::numeric_limits<uint8_t>::max()) {
      return uint8();
    } else if (input_length <= std::numeric_limits<uint16_t>::max()) {
      return uint16();
    } else if (input_length <= std::numeric_limits<uint32_t>::max()) {
      return uint32();
    } else {
      return uint64();
    }
  }
};

void RegisterVectorPermute(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<PermuteMetaFunction>()));
}

}  // namespace

// ----------------------------------------------------------------------

void RegisterVectorPlacement(FunctionRegistry* registry) {
  RegisterVectorReverseIndices(registry);
  RegisterVectorPermute(registry);
}

}  // namespace arrow::compute::internal
