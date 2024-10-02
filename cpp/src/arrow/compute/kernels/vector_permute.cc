// #include "arrow/compute/kernels/vector_gather_internal.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/logging.h"

namespace arrow::compute::internal {

namespace {

template <typename IndexCType, typename ValueBitWidthConstant,
          typename OutputIsZeroInitialized = std::false_type,
          typename WithFactor = std::false_type>
struct FixedWidthTakeImpl {
  static constexpr int kValueWidthInBits = ValueBitWidthConstant::value;

  static Status Exec(KernelContext* ctx, const ArraySpan& values,
                     const ArraySpan& indices, ArrayData* out_arr, int64_t factor) {
#ifndef NDEBUG
    int64_t bit_width = util::FixedWidthInBits(*values.type);
    DCHECK(WithFactor::value || (kValueWidthInBits == bit_width && factor == 1));
    DCHECK(!WithFactor::value ||
           (factor > 0 && kValueWidthInBits == 8 &&  // factors are used with bytes
            static_cast<int64_t>(factor * kValueWidthInBits) == bit_width));
#endif
    const bool out_has_validity = values.MayHaveNulls() || indices.MayHaveNulls();

    const uint8_t* src;
    int64_t src_offset;
    std::tie(src_offset, src) = util::OffsetPointerOfFixedBitWidthValues(values);
    uint8_t* out = util::MutableFixedWidthValuesPointer(out_arr);
    int64_t valid_count = 0;
    arrow::internal::Gather<kValueWidthInBits, IndexCType, WithFactor::value> gather{
        /*src_length=*/values.length,
        src,
        src_offset,
        /*idx_length=*/indices.length,
        /*idx=*/indices.GetValues<IndexCType>(1),
        out,
        factor};
    if (out_has_validity) {
      DCHECK_EQ(out_arr->offset, 0);
      // out_is_valid must be zero-initiliazed, because Gather::Execute
      // saves time by not having to ClearBit on every null element.
      auto out_is_valid = out_arr->GetMutableValues<uint8_t>(0);
      memset(out_is_valid, 0, bit_util::BytesForBits(out_arr->length));
      valid_count = gather.template Execute<OutputIsZeroInitialized::value>(
          /*src_validity=*/values, /*idx_validity=*/indices, out_is_valid);
    } else {
      valid_count = gather.Execute();
    }
    out_arr->null_count = out_arr->length - valid_count;
    return Status::OK();
  }
};

template <template <typename...> class PermuteImpl, typename... Args>
Status PermuteIndexDispatch(KernelContext* ctx, const ArraySpan& values,
                            const ArraySpan& indices, ArrayData* out,
                            int64_t factor = 1) {
  switch (indices.type->byte_width()) {
    case 1:
      return PermuteImpl<uint8_t, Args...>::Exec(ctx, values, indices, out, factor);
    case 2:
      return PermuteImpl<uint16_t, Args...>::Exec(ctx, values, indices, out, factor);
    case 4:
      return PermuteImpl<uint32_t, Args...>::Exec(ctx, values, indices, out, factor);
    default:
      DCHECK_EQ(indices.type->byte_width(), 8);
      return PermuteImpl<uint64_t, Args...>::Exec(ctx, values, indices, out, factor);
  }
}

Status FixedWidthPermuteExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& values = batch[0].array;
  const ArraySpan& indices = batch[1].array;

  ArrayData* out_arr = out->array_data().get();
  DCHECK(util::IsFixedWidthLike(values));
  // When we know for sure that values nor indices contain nulls, we can skip
  // allocating the validity bitmap altogether and save time and space.
  const bool allocate_validity = values.MayHaveNulls() || indices.MayHaveNulls();
  RETURN_NOT_OK(util::internal::PreallocateFixedWidthArrayData(
      ctx, indices.length, /*source=*/values, allocate_validity, out_arr));
  switch (util::FixedWidthInBits(*values.type)) {
    case 0:
      DCHECK(values.type->id() == Type::FIXED_SIZE_BINARY ||
             values.type->id() == Type::FIXED_SIZE_LIST);
      return PermuteIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 0>>(
          ctx, values, indices, out_arr);
    case 1:
      // Zero-initialize the data buffer for the output array when the bit-width is 1
      // (e.g. Boolean array) to avoid having to ClearBit on every null element.
      // This might be profitable for other types as well, but we take the most
      // conservative approach for now.
      memset(out_arr->buffers[1]->mutable_data(), 0, out_arr->buffers[1]->size());
      return PermuteIndexDispatch<
          FixedWidthTakeImpl, std::integral_constant<int, 1>, /*OutputIsZeroInitialized=*/
          std::true_type>(ctx, values, indices, out_arr);
    case 8:
      return PermuteIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 8>>(
          ctx, values, indices, out_arr);
    case 16:
      return PermuteIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 16>>(
          ctx, values, indices, out_arr);
    case 32:
      return PermuteIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 32>>(
          ctx, values, indices, out_arr);
    case 64:
      return PermuteIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 64>>(
          ctx, values, indices, out_arr);
    case 128:
      // For INTERVAL_MONTH_DAY_NANO, DECIMAL128
      return PermuteIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 128>>(
          ctx, values, indices, out_arr);
    case 256:
      // For DECIMAL256
      return PermuteIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 256>>(
          ctx, values, indices, out_arr);
  }
  if (ARROW_PREDICT_TRUE(values.type->id() == Type::FIXED_SIZE_BINARY ||
                         values.type->id() == Type::FIXED_SIZE_LIST)) {
    int64_t byte_width = util::FixedWidthInBytes(*values.type);
    // 0-length fixed-size binary or lists were handled above on `case 0`
    DCHECK_GT(byte_width, 0);
    return PermuteIndexDispatch<FixedWidthTakeImpl,
                                /*ValueBitWidth=*/std::integral_constant<int, 8>,
                                /*OutputIsZeroInitialized=*/std::false_type,
                                /*WithFactor=*/std::true_type>(ctx, values, indices,
                                                               out_arr,
                                                               /*factor=*/byte_width);
  }
  return Status::NotImplemented("Unsupported primitive type for permute: ", *values.type);
  return Status::OK();
}

struct PermuteKernelSignature {
  InputType value_type;
  InputType indices_type;
  ArrayKernelExec exec;
};

std::unique_ptr<Function> MakePermuteFunction(
    std::string name, std::vector<PermuteKernelSignature>&& signatures, FunctionDoc doc,
    const FunctionOptions* default_options) {
  auto func = std::make_unique<VectorFunction>(std::move(name), Arity::Binary(),
                                               std::move(doc), default_options);
  for (auto& signature : signatures) {
    auto kernel = VectorKernel{};
    kernel.signature = KernelSignature::Make(
        {std::move(signature.value_type), std::move(signature.indices_type)},
        OutputType(FirstType));
    kernel.exec = signature.exec;
    kernel.can_execute_chunkwise = false;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  return func;
}

const FunctionDoc permute_doc(
    "Permute values of an input based on indices from another array",
    "Place each input value to the output array at position specified by `indices`",
    {"input", "indices"});

const PermuteOptions* GetDefaultPermuteOptions() {
  static const auto kDefaultPermuteOptions = PermuteOptions::Defaults();
  return &kDefaultPermuteOptions;
}

}  // namespace

void RegisterVectorPermute(FunctionRegistry* registry) {
  auto permute_indices = match::Integer();
  std::vector<PermuteKernelSignature> signatures = {
      {InputType(match::Primitive()), permute_indices, FixedWidthPermuteExec},
  };
  DCHECK_OK(registry->AddFunction(MakePermuteFunction(
      "permute", std::move(signatures), permute_doc, GetDefaultPermuteOptions())));
}

}  // namespace arrow::compute::internal
