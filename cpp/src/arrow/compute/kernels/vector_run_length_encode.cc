#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename Type>
struct RunLengthEncodeGenerator {
  using CType = typename Type::c_type;

  struct Element {
    bool valid;
    CType value;

    bool operator!=(const Element& other) const {
      return valid != other.valid || value != other.value;
    }

    Element(const uint8_t* validity_buffer, const CType* value_buffer, size_t index) {
      if (validity_buffer != NULLPTR) {
        valid = bit_util::GetBit(validity_buffer, index);
      } else {
        valid = true;
      }
      value = value_buffer[index];
    }
  };

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* output) {
    ARROW_DCHECK(batch.num_values() == 1);
    auto& input_data = batch.values[0].array();
    if (input_data->length == 0) {
      return Status::OK();
    }
    auto input_validity = input_data->GetValues<uint8_t>(0);
    auto input_values = input_data->GetValues<CType>(1);
    bool has_validity_buffer = input_validity != NULLPTR;

    size_t num_values_output = 1;
    Element element = Element(input_validity, input_values, 0);
    for (int64_t index = 1; index < input_data->length; index++) {
      Element previous_element = element;
      element = Element(input_validity, input_values, index);
      if (element != previous_element) {
        num_values_output++;
      }
    }

    auto pool = ctx->memory_pool();

    std::shared_ptr<Buffer> validity_buffer = NULLPTR;
    // in bytes
    uint64_t validity_buffer_size = 0;
    if (has_validity_buffer) {
      validity_buffer_size = (num_values_output - 1) / 8 + 1;
      ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateBuffer(validity_buffer_size, pool));
    }
    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(num_values_output * sizeof(CType), pool));
    ARROW_ASSIGN_OR_RAISE(auto indices_buffer,
                          AllocateBuffer(num_values_output * sizeof(uint64_t), pool));

    auto output_type = std::make_shared<RunLengthEncodedType>(input_data->type);
    auto output_array_data = ArrayData::Make(std::move(output_type), input_data->length);
    auto child_array_data = ArrayData::Make(input_data->type, num_values_output);
    output_array_data->buffers.push_back(std::move(indices_buffer));
    child_array_data->buffers.push_back(std::move(validity_buffer));
    child_array_data->buffers.push_back(std::move(values_buffer));

    auto output_validity = child_array_data->GetMutableValues<uint8_t>(0);
    auto output_values = child_array_data->GetMutableValues<CType>(1);
    auto output_indexes = output_array_data->GetMutableValues<uint64_t>(0);
    output_array_data->child_data.push_back(std::move(child_array_data));

    if (has_validity_buffer) {
      // clear last byte in validity buffer, which won't completely be overwritten with
      // validity values
      output_validity[validity_buffer_size - 1] = 0;
    }

    size_t output_position = 0;
    for (int64_t input_position = 0; input_position < input_data->length;
         input_position++) {
      Element previous_element = element;
      element = Element(input_validity, input_values, input_position);
      if (output_position == 0 || element != previous_element) {
        if (has_validity_buffer) {
          bit_util::SetBitTo(output_validity, output_position, element.valid);
        }
        output_values[output_position] = element.value;
        output_indexes[output_position] = input_position;
        output_position++;
      }
    }
    ARROW_DCHECK(output_position == num_values_output);

    *output = Datum(output_array_data);
    return Status::OK();
  }
};

template <>
struct RunLengthEncodeGenerator<NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* output) {
    // TODO
    return Status::NotImplemented("TODO");
  }
};

template <>
struct RunLengthEncodeGenerator<BooleanType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* output) {
    // TODO
    return Status::NotImplemented("TODO");
  }
};

static const FunctionDoc run_length_encode_doc(
    "Run-length array", ("Return a run-length-encoded version of the input array."),
    {"array"}, "RunLengthEncodeOptions");

void RegisterVectorRunLengthEncode(FunctionRegistry* registry) {
  auto rle = std::make_shared<VectorFunction>("run_length_encode", Arity::Unary(),
                                              run_length_encode_doc);

  for (const auto& ty : NumericTypes()) {
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthEncodeGenerator>(ty);
    auto sig = KernelSignature::Make(
        {InputType(ty, ValueDescr::ARRAY)},
        OutputType([](KernelContext*,
                      const std::vector<ValueDescr>& descrs) -> Result<ValueDescr> {
          return ValueDescr(std::make_shared<RunLengthEncodedType>(descrs[0].type),
                            ValueDescr::ARRAY);
        }));
    VectorKernel kernel(sig, exec);
    DCHECK_OK(rle->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(rle)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
