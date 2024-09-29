// #include "arrow/compute/kernels/vector_gather_internal.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/logging.h"

namespace arrow::compute::internal {

namespace {

struct GatherKernelSignature {
  InputType indices_type;
  InputType value_type;
  ArrayKernelExec exec;
};

std::unique_ptr<Function> MakeGatherFunction(
    std::string name, int min_args, std::vector<GatherKernelSignature>&& signatures,
    FunctionDoc doc) {
  auto func = std::make_unique<VectorFunction>(std::move(name), Arity::VarArgs(min_args),
                                               std::move(doc));
  for (auto& signature : signatures) {
    auto kernel = VectorKernel{};
    kernel.signature = KernelSignature::Make(
        {std::move(signature.indices_type), std::move(signature.value_type)},
        OutputType(LastType), /*is_varargs=*/true);
    kernel.exec = signature.exec;
    kernel.can_execute_chunkwise = false;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
  return func;
}

const FunctionDoc gather_doc(
    "Gather values from a list of inputs with an indices vector",
    "The output is populated with values selected from the inputs, where each value is "
    "chosen based on the corresponding index from the indices that specifies which input "
    "in the list to use",
    {"indices", "*inputs"});

}  // namespace

void RegisterVectorGather(FunctionRegistry* registry) {
  std::vector<GatherKernelSignature> signatures = {};
  DCHECK_OK(registry->AddFunction(
      MakeGatherFunction("gather", /*min_args=*/2, std::move(signatures), gather_doc)));
}

}  // namespace arrow::compute::internal
