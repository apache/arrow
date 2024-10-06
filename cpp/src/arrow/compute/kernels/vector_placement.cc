#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/logging.h"

namespace arrow::compute::internal {

namespace {

struct ReverseIndexState : public KernelState {
  explicit ReverseIndexState(int64_t length, std::shared_ptr<DataType> type,
                             std::shared_ptr<Buffer> validity,
                             std::shared_ptr<Buffer> data)
      : length(length),
        type(std::move(type)),
        validity(std::move(validity)),
        data(std::move(data)) {}

  const int64_t length = 0;
  const std::shared_ptr<DataType> type = nullptr;
  const std::shared_ptr<Buffer> validity = nullptr;
  const std::shared_ptr<Buffer> data = nullptr;
};

Result<std::unique_ptr<KernelState>> ReverseIndexInit(KernelContext* ctx,
                                                      const KernelInitArgs& args) {
  auto* options = checked_cast<const ReverseIndexOptions*>(args.options);
  DCHECK_NE(options, nullptr);

  if (options->output_length < 0) {
    return Status::Invalid("Output length of reverse_index must be non-negative, got " +
                           std::to_string(options->output_length));
  }
  if (!options->output_type || !is_integer(options->output_type->id())) {
    return Status::Invalid(
        "Output type of reverse_index must be integer, got " +
        (options->output_type ? options->output_type->ToString() : "null"));
  }
  if (options->output_non_taken &&
      !options->output_non_taken->type->Equals(options->output_type)) {
    return Status::Invalid(
        "Output non-taken of reverse_index must be of the same type with the output "
        "type, got " +
        options->output_non_taken->type->ToString());
  }

  std::shared_ptr<Buffer> validity = nullptr;
  ARROW_ASSIGN_OR_RAISE(auto data, ctx->Allocate(options->output_length *
                                                 options->output_type->byte_width()));
  if (!options->output_non_taken || !options->output_non_taken->is_valid) {
    ARROW_ASSIGN_OR_RAISE(validity, ctx->AllocateBitmap(options->output_length));
    std::memset(validity->mutable_data(), 0, options->output_length);
  } else {
    auto int_scalar = checked_cast<const arrow::internal::PrimitiveScalarBase*>(
        options->output_non_taken.get());
    for (int64_t i = 0; i < options->output_length; ++i) {
      std::memcpy(data->mutable_data() + int_scalar->type->byte_width() * i,
                  int_scalar->data(), int_scalar->type->byte_width());
    }
  }
  auto state = std::make_unique<ReverseIndexState>(
      options->output_length, options->output_type, std::move(validity), std::move(data));
  return state;
}

/// \brief The OutputType::Resolver of the "reverse_index" function.
Result<TypeHolder> ReverseIndexResolveOutputType(KernelContext* ctx,
                                                 const std::vector<TypeHolder>&) {
  auto state = checked_cast<const ReverseIndexState*>(ctx->state());
  DCHECK_NE(state, nullptr);
  return TypeHolder(state->type);
}

template <typename ExecType>
struct ReverseIndexImpl {
  using ThisType = ReverseIndexImpl<ExecType>;
  using IndexType = typename ExecType::IndexType;
  using IndexCType = typename IndexType::c_type;
  using ShapeType = typename ExecType::ShapeType;

  static Result<std::shared_ptr<ArrayData>> Exec(KernelContext* ctx,
                                                 const ShapeType& indices) {
    auto state = checked_cast<ReverseIndexState*>(ctx->state());
    DCHECK_NE(state, nullptr);

    if (state->length > 0) {
      ThisType impl(ctx, indices, state);
      RETURN_NOT_OK(VisitTypeInline(*state->type, &impl));
    }

    return ArrayData::Make(state->type, state->length, {state->validity, state->data});
  }

 private:
  KernelContext* ctx;
  const ShapeType& indices;
  ReverseIndexState* state;

 private:
  ReverseIndexImpl(KernelContext* ctx, const ShapeType& indices, ReverseIndexState* state)
      : ctx(ctx), indices(indices), state(state) {}

  Status Visit(const DataType& type) { return Status::Invalid(type.ToString()); }

  template <typename Type>
  enable_if_t<is_integer_type<Type>::value, Status> Visit(const Type&) {
    if (state->validity) {
      return VisitInternal<Type, true>();
    } else {
      return VisitInternal<Type, false>();
    }
  }

  template <typename Type, bool validity_preallocated>
  Status VisitInternal() {
    using OutputCType = typename Type::c_type;

    uint8_t* output_validity = nullptr;
    if constexpr (validity_preallocated) {
      output_validity = state->validity->mutable_data_as<uint8_t>();
    }
    OutputCType* output_data = state->data->mutable_data_as<OutputCType>();

    int64_t reverse_index = 0;
    return ExecType::template VisitIndex(
        indices,
        [&](IndexCType index) {
          if (ARROW_PREDICT_TRUE(index >= 0 &&
                                 static_cast<int64_t>(index) < state->length)) {
            if constexpr (validity_preallocated) {
              bit_util::SetBitTo(output_validity, index, true);
            }
            if (ARROW_PREDICT_FALSE(
                    static_cast<int64_t>(std::numeric_limits<OutputCType>::max()) <
                    reverse_index)) {
              return Status::Invalid("Overflow in reverse_index, got " +
                                     std::to_string(reverse_index) + " for " +
                                     state->type->ToString());
            }
            output_data[index] = static_cast<OutputCType>(reverse_index);
          }
          ++reverse_index;
          return Status::OK();
        },
        [&]() {
          ++reverse_index;
          return Status::OK();
        });
  }

  template <typename VISITOR, typename... ARGS>
  friend Status arrow::VisitTypeInline(const DataType&, VISITOR*, ARGS&&... args);
};

template <typename Ignored, typename Type>
struct ReverseIndex {
  using ThisType = ReverseIndex<Ignored, Type>;
  using IndexType = Type;
  using ShapeType = ArraySpan;

  template <typename ValidFunc, typename NullFunc>
  static Status VisitIndex(const ArraySpan& span, ValidFunc&& valid_func,
                           NullFunc&& null_func) {
    return VisitArraySpanInline<IndexType>(span, std::forward<ValidFunc>(valid_func),
                                           std::forward<NullFunc>(null_func));
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    DCHECK(span[0].is_array());
    const auto& indices = span[0].array;
    ARROW_ASSIGN_OR_RAISE(auto output, ReverseIndexImpl<ThisType>::Exec(ctx, indices));
    result->value = std::move(output);
    return Status::OK();
  }
};

template <typename Ignored, typename Type>
struct ReverseIndexChunked {
  using ThisType = ReverseIndexChunked<Ignored, Type>;
  using IndexType = Type;
  using ShapeType = std::shared_ptr<ChunkedArray>;

  template <typename ValidFunc, typename NullFunc>
  static Status VisitIndex(const std::shared_ptr<ChunkedArray>& chunked_array,
                           ValidFunc&& valid_func, NullFunc&& null_func) {
    for (const auto& chunk : chunked_array->chunks()) {
      ArraySpan span(*chunk->data());
      RETURN_NOT_OK(VisitArraySpanInline<IndexType>(
          span, std::forward<ValidFunc>(valid_func), std::forward<NullFunc>(null_func)));
    }
    return Status::OK();
  }

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* result) {
    DCHECK(batch[0].is_chunked_array());
    const auto& indices = batch[0].chunked_array();
    ARROW_ASSIGN_OR_RAISE(auto output, ReverseIndexImpl<ThisType>::Exec(ctx, indices));
    *result = std::move(output);
    return Status::OK();
  }
};

const FunctionDoc reverse_index_doc(
    "Compute the reverse indices from an input indices",
    "Place each input value to the output array at position specified by `indices`",
    {"indices"});

const PermuteOptions* GetDefaultReverseIndexOptions() {
  static const auto kDefaultPermuteOptions = PermuteOptions::Defaults();
  return &kDefaultPermuteOptions;
}

const FunctionDoc permute_doc(
    "Permute values of an input based on indices from another array",
    "Place each input value to the output array at position specified by `indices`",
    {"input", "indices"});

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
    const auto& permute_options = checked_cast<const PermuteOptions&>(*options);
    if (args[0].length() != args[1].length()) {
      return Status::Invalid("Input and indices must have the same length");
    }
    if (args[0].length() == 0) {
      return args[0];
    }
    int64_t output_length = permute_options.bound;
    if (output_length < 0) {
      ARROW_ASSIGN_OR_RAISE(auto max_scalar, CallFunction("max", {args[1]}, ctx));
      DCHECK(max_scalar.is_scalar());
      ARROW_ASSIGN_OR_RAISE(auto max_i64_scalar, max_scalar.scalar()->CastTo(int64()));
      output_length = checked_cast<const Int64Scalar*>(max_i64_scalar.get())->value + 1;
    }
    if (output_length <= 0) {
      ARROW_ASSIGN_OR_RAISE(auto output, MakeEmptyArray(args[0].type()));
      return output->data();
    }
    ARROW_ASSIGN_OR_RAISE(auto reverse_indices,
                          MakeArrayOfNull(int64(), output_length, ctx->memory_pool()));
    switch (args[1].kind()) {
      case Datum::ARRAY:
        RETURN_NOT_OK(ReverseIndices(*args[1].array(), reverse_indices->data()));
        break;
      case Datum::CHUNKED_ARRAY:
        for (const auto& chunk : args[1].chunked_array()->chunks()) {
          RETURN_NOT_OK(ReverseIndices(*chunk->data(), reverse_indices->data()));
        }
        break;
      default:
        return Status::NotImplemented("Unsupported shape for permute operation: indices=",
                                      args[1].ToString());
        break;
    }
    return CallFunction("take", {args[0], reverse_indices}, ctx);
  }

 private:
  Status ReverseIndices(const ArraySpan& indices,
                        const std::shared_ptr<ArrayData>& reverse_indices) const {
    auto reverse_indices_validity = reverse_indices->GetMutableValues<uint8_t>(0);
    auto reverse_indices_data = reverse_indices->GetMutableValues<int64_t>(1);
    auto length = reverse_indices->length;
    int64_t reverse_index = 0;
    return VisitArraySpanInline<Int64Type>(
        indices,
        [&](int64_t index) {
          if (ARROW_PREDICT_TRUE(index > 0 && index < length)) {
            bit_util::SetBitTo(reverse_indices_validity, index, true);
            reverse_indices_data[index] = reverse_index;
          }
          ++reverse_index;
          return Status::OK();
        },
        [&]() {
          ++reverse_index;
          return Status::OK();
        });
  };
};

}  // namespace

void RegisterVectorReverseIndex(FunctionRegistry* registry) {
  auto function =
      std::make_shared<VectorFunction>("reverse_index", Arity::Unary(), reverse_index_doc,
                                       GetDefaultReverseIndexOptions());

  auto add_kernel = [&function](Type::type type_id) {
    VectorKernel kernel;
    kernel.signature = KernelSignature::Make({InputType(match::SameTypeId(type_id))},
                                             OutputType(ReverseIndexResolveOutputType));
    kernel.init = ReverseIndexInit;
    kernel.exec = GenerateInteger<ReverseIndex, void, ArrayKernelExec>(type_id);
    kernel.exec_chunked =
        GenerateInteger<ReverseIndexChunked, void, VectorKernel::ChunkedExec>(type_id);
    kernel.can_execute_chunkwise = false;
    kernel.output_chunked = false;
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  };
  for (const auto& ty : IntTypes()) {
    add_kernel(ty->id());
  }

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

void RegisterVectorPlacement(FunctionRegistry* registry) {
  RegisterVectorReverseIndex(registry);
  DCHECK_OK(registry->AddFunction(std::make_shared<PermuteMetaFunction>()));
}

}  // namespace arrow::compute::internal
