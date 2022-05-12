#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"

namespace arrow {
namespace compute {
namespace internal {

static const FunctionDoc run_length_encode_doc(
    "Run-length array", ("Return a run-length-encoded version of the input array."),
    {"array"}, "RunLengthEncodeOptions");

void RegisterVectorRunLengthEncode(FunctionRegistry* registry) {
  auto rle = std::make_shared<VectorFunction>("run_length_encode", Arity::Unary(),
                                              run_length_encode_doc);

  DCHECK_OK(registry->AddFunction(std::move(rle)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
