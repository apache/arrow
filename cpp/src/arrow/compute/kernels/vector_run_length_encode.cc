#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"

namespace arrow {
namespace compute {
namespace internal {

class RunLengthEncodeState : public KernelState {
public:
  Status Exec(const ExecBatch &batch, Datum* output) {
    auto &input_data = batch.values[0].array();
    if (input_data->length == 0) {
      return Status::OK();
    }

    // TODO
    ARROW_CHECK(input_data->type == int32());
    size_t num_values_output = 1;
    for (size_t index = 1; index < input_data->length; index++) {
      if (input_data->GetMutableValues<int32_t>(index - 1)
          != input_data->GetMutableValues<int32_t>(index - 1)) {
        num_values_output++;
      }
    }

    // TODO
    auto pool = default_memory_pool();

    uint64_t null_buffer_size = (num_values_output - 1) / 8 + 1;
    ARROW_ASSIGN_OR_RAISE(auto null_buffer, AllocateBuffer(null_buffer_size, pool));
    ARROW_ASSIGN_OR_RAISE(auto values_buffer, AllocateBuffer(num_values_output * sizeof(int32_t), pool));
    ARROW_ASSIGN_OR_RAISE(auto indices_buffer, AllocateBuffer(num_values_output * sizeof(uint64_t), pool));

    auto null_bitmap = reinterpret_cast<uint8_t*>(null_buffer->mutable_address());
    auto output_indexes = reinterpret_cast<int64_t*>(indices_buffer->mutable_address());
    auto output_values = reinterpret_cast<int32_t*>(values_buffer->mutable_address());
    auto input_values = input_data->GetMutableValues<int32_t>(1);

    memset(null_bitmap, 0, null_buffer_size);

    output_indexes[0] = 0;
    output_values[0] = input_values[0];
    size_t output_position = 0;
    for (size_t input_position = 1; input_position < input_data->length; input_position++) {
      if (input_values - input_values[input_position] != input_values[input_position - 1]) {
        output_position++;
        output_values[output_position] = input_values[input_position];
        output_indexes[output_position] = input_position;
      }
    }
    ARROW_DCHECK(output_position == num_values_output);

    auto output_array_data = ArrayData::Make(input_data->type, input_data->length);
    output_array_data->buffers.push_back(std::move(null_buffer));
    output_array_data->buffers.push_back(std::move(values_buffer));
    output_array_data->buffers.push_back(std::move(indices_buffer));
    *output = Datum(output_array_data);
    return Status::OK();
  }

  ArrayData output;
};

Status RunLengthEncodeExec(KernelContext* ctx, const ExecBatch& batch, Datum* output) {
  return checked_cast<RunLengthEncodeState*>(ctx->state())->Exec(batch, output);
}

static const FunctionDoc run_length_encode_doc(
    "Run-length array", ("Return a run-length-encoded version of the input array."),
    {"array"}, "RunLengthEncodeOptions");

void RegisterVectorRunLengthEncode(FunctionRegistry* registry) {
  auto rle = std::make_shared<VectorFunction>("run_length_encode", Arity::Unary(),
                                              run_length_encode_doc);


  auto sig = KernelSignature::Make({InputType(ValueDescr::ARRAY)}, OutputType([](KernelContext*, const std::vector<ValueDescr>& descrs) -> Result<ValueDescr> {
                                     return descrs[0];
                                 }));
  VectorKernel kernel(sig, RunLengthEncodeExec);

  DCHECK_OK(rle->AddKernel(std::move(kernel)));

  DCHECK_OK(registry->AddFunction(std::move(rle)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
