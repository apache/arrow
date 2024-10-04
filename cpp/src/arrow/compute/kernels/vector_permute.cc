#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/logging.h"

namespace arrow::compute::internal {

namespace {

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

void RegisterVectorPermute(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<PermuteMetaFunction>()));
}

}  // namespace arrow::compute::internal
