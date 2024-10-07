#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/logging.h"

namespace arrow::compute::internal {

namespace {

// ----------------------------------------------------------------------
// ReverseIndex

const FunctionDoc reverse_index_doc(
    "Compute the reverse indices from an input indices",
    "For the `i`-th `index` in `indices`, the `index`-th output is `i`", {"indices"});

const PermuteOptions* GetDefaultReverseIndexOptions() {
  static const auto kDefaultPermuteOptions = PermuteOptions::Defaults();
  return &kDefaultPermuteOptions;
}

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

// ----------------------------------------------------------------------
// Permute

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
    const auto& values = args[0];
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
    int64_t output_length = permute_options->output_length;
    if (output_length < 0) {
      return Status::Invalid("Output length of permute must be non-negative, got " +
                             std::to_string(output_length));
    }
    std::shared_ptr<Scalar> output_non_taken = nullptr;
    if (is_signed_integer(indices.type()->id())) {
      // Using -1 (as opposed to null) as output_non_taken for signed integer types to
      // enable efficient reverse_index.
      ARROW_ASSIGN_OR_RAISE(output_non_taken, MakeScalar(indices.type(), -1));
    }
    ReverseIndexOptions reverse_index_options{output_length, indices.type(),
                                              std::move(output_non_taken)};
    ARROW_ASSIGN_OR_RAISE(
        auto reverse_indices,
        CallFunction("reverse_index", {indices}, &reverse_index_options, ctx));
    TakeOptions take_options{/*boundcheck=*/false};
    return CallFunction("take", {values, reverse_indices}, &take_options, ctx);
  }
};

// ----------------------------------------------------------------------

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

void RegisterVectorPermute(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<PermuteMetaFunction>()));
}

}  // namespace

void RegisterVectorPlacement(FunctionRegistry* registry) {
  RegisterVectorReverseIndex(registry);
  RegisterVectorPermute(registry);
}

}  // namespace arrow::compute::internal
