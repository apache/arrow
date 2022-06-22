#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename ArrowType, bool has_validity_buffer>
struct RunLengthEncodeExec {
  using CType = typename ArrowType::c_type;

  struct Element {
    bool valid;
    CType value;

    bool operator!=(const Element& other) const {
      return valid != other.valid || value != other.value;
    }
  };

  Element Read() {
    Element result;
    if (input_validity != NULLPTR) {
      result.valid = bit_util::GetBit(input_validity, input_position);
    } else {
      result.valid = true;
    }
    result.value = (reinterpret_cast<const CType*>(input_values))[input_position];
    return result;
  }

  void WriteValue(Element element) {
    if (has_validity_buffer) {
      bit_util::SetBitTo(output_validity, output_position, element.valid);
    }
    (reinterpret_cast<CType*>(output_values))[output_position] = element.value;
  }

  RunLengthEncodeExec(KernelContext* ctx, const ExecBatch& batch, Datum* output):
    input_data{*batch.values[0].array()},
    pool{ctx->memory_pool()},
    output_datum{output} {
    ARROW_DCHECK(batch.num_values() == 1);
  }

  Status Exec() {
    if (input_data.length == 0) {
      return Status::NotImplemented("TODO");
    }
    input_validity = input_data.GetValues<uint8_t>(0);
    input_values = input_data.GetValues<uint8_t>(1);

    input_position = 0;
    Element element = Read();
    size_t num_values_output = 1;
    size_t output_null_count = element.valid ? 0 : 1;
    for (input_position = 1; input_position < input_data.length; input_position++) {
      Element previous_element = element;
      element = Read();
      if (element != previous_element) {
        num_values_output++;
        if (!element.valid) {
          output_null_count++;
        }
      }
    }

    std::shared_ptr<Buffer> validity_buffer = NULLPTR;
    // in bytes
    int64_t validity_buffer_size = 0;
    if (has_validity_buffer) {
      validity_buffer_size = (num_values_output - 1) / 8 + 1;
      ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateBuffer(validity_buffer_size, pool));
    }
    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(num_values_output * sizeof(CType), pool));
    ARROW_ASSIGN_OR_RAISE(auto run_lengths_buffer,
                          AllocateBuffer(num_values_output * sizeof(int64_t), pool));

    auto output_type = std::make_shared<RunLengthEncodedType>(input_data.type);
    auto output_array_data = ArrayData::Make(std::move(output_type), input_data.length);
    auto child_array_data = ArrayData::Make(input_data.type, num_values_output);
    output_array_data->buffers.push_back(std::move(run_lengths_buffer));
    child_array_data->buffers.push_back(std::move(validity_buffer));
    child_array_data->buffers.push_back(std::move(values_buffer));

    output_array_data->null_count.store(input_data.null_count);
    child_array_data->null_count = output_null_count;

    output_validity = child_array_data->GetMutableValues<uint8_t>(0);
    output_values = child_array_data->GetMutableValues<uint8_t>(1);
    output_run_lengths = output_array_data->GetMutableValues<int64_t>(0);
    output_array_data->child_data.push_back(std::move(child_array_data));

    if (has_validity_buffer) {
      // clear last byte in validity buffer, which won't completely be overwritten with
      // validity values
      output_validity[validity_buffer_size - 1] = 0;
    }

    input_position = 0;
    output_position = 0;
    element = Read();
    WriteValue(element);
    output_position = 1;
    for (input_position = 1; input_position < input_data.length;
         input_position++) {
      Element previous_element = element;
      element = Read();
      if (element != previous_element) {
        WriteValue(element);
        // run lengths buffer holds accumulated run length values
        output_run_lengths[output_position - 1] = input_position;
        output_position++;
      }
    }
    output_run_lengths[output_position - 1] = input_data.length;
    ARROW_DCHECK(output_position == num_values_output);

    *output_datum = Datum(output_array_data);
    return Status::OK();
  }

  const ArrayData& input_data;
  MemoryPool* pool;
  Datum* output_datum;
  const uint8_t* input_validity;
  const void* input_values;
  uint8_t* output_validity;
  void* output_values;
  int64_t* output_run_lengths;
  int64_t input_position;
  size_t output_position;
};

template<>
RunLengthEncodeExec<BooleanType, true>::Element RunLengthEncodeExec<BooleanType, true>::Read() {
  Element result;
  result.valid = bit_util::GetBit(input_validity, input_position);
  if (result.valid) {
    result.value = bit_util::GetBit(reinterpret_cast<const uint8_t*>(input_values), input_position);
  }
  return result;
}

template<>
RunLengthEncodeExec<BooleanType, false>::Element RunLengthEncodeExec<BooleanType, false>::Read() {
  return {
    .valid = true,
    .value = bit_util::GetBit(reinterpret_cast<const uint8_t*>(input_values), input_position),
  };
}

template<>
void RunLengthEncodeExec<BooleanType, true>::WriteValue(Element element) {
  bit_util::SetBitTo(output_validity, output_position, element.valid);
  if (element.valid) {
    bit_util::SetBitTo(reinterpret_cast<uint8_t*>(output_values), output_position, element.value);
  }
}

template<>
void RunLengthEncodeExec<BooleanType, false>::WriteValue(Element element) {
  bit_util::SetBitTo(reinterpret_cast<uint8_t*>(output_values), output_position, element.value);
}

template <typename Type>
struct RunLengthEncodeGenerator {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* output) {
    bool has_validity_buffer = batch.values[0].array()->null_count != 0;
    if (has_validity_buffer) {
      return RunLengthEncodeExec<Type, true>(ctx, batch, output).Exec();
    } else {
      return RunLengthEncodeExec<Type, false>(ctx, batch, output).Exec();
    }
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

template <typename Type>
struct RunLengthDecodeGenerator {
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
    auto input_validity = input_data->child_data[0]->GetValues<uint8_t>(0);
    auto input_values = input_data->child_data[0]->GetValues<CType>(1);
    auto input_accumulated_run_length = input_data->GetValues<int64_t>(0);
    bool has_validity_buffer = input_validity != NULLPTR;

    int64_t num_values_input = input_data->child_data[0]->length;
    int64_t num_values_output = input_data->length;

    auto pool = ctx->memory_pool();

    std::shared_ptr<Buffer> validity_buffer = NULLPTR;
    // in bytes
    int64_t validity_buffer_size = 0;
    if (has_validity_buffer) {
      validity_buffer_size = (num_values_output - 1) / 8 + 1;
      ARROW_ASSIGN_OR_RAISE(validity_buffer, AllocateBuffer(validity_buffer_size, pool));
    }
    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(num_values_output * sizeof(CType), pool));

    auto& input_type = checked_cast<arrow::RunLengthEncodedType&>(*input_data->type);
    auto output_type = input_type.encoded_type();
    auto output_array_data = ArrayData::Make(std::move(output_type), num_values_output);
    output_array_data->buffers.push_back(std::move(validity_buffer));
    output_array_data->buffers.push_back(std::move(values_buffer));
    output_array_data->null_count.store(input_data->null_count);

    auto output_validity = output_array_data->GetMutableValues<uint8_t>(0);
    auto output_values = output_array_data->GetMutableValues<CType>(1);

    if (has_validity_buffer) {
      // clear last byte in validity buffer, which won't completely be overwritten with
      // validity values
      output_validity[validity_buffer_size - 1] = 0;
    }

    int64_t output_position = 0;
    int64_t run_start = 0;
    for (int64_t input_position = 0; input_position < num_values_input;
         input_position++) {
      int64_t run_end = input_accumulated_run_length[input_position];
      int64_t run_length = run_end - run_start;
      run_start = run_end;

      bool valid = true;
      if (has_validity_buffer) {
        valid = bit_util::GetBit(input_validity, input_position);
        bit_util::SetBitsTo(output_validity, output_position, run_length, valid);
      }
      if (valid) {
        for (int64_t run_element = 0; run_element < run_length; run_element++) {
          output_values[output_position + run_element] = input_values[input_position];
        }
      }
      output_position += run_length;
    }
    ARROW_DCHECK(output_position == num_values_output);

    *output = Datum(output_array_data);
    return Status::OK();
  }
};

template <>
struct RunLengthDecodeGenerator<NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* output) {
    // TODO
    return Status::NotImplemented("TODO");
  }
};

static const FunctionDoc run_length_encode_doc(
    "Run-length array", ("Return a run-length-encoded version of the input array."),
    {"array"}, "RunLengthEncodeOptions");
static const FunctionDoc run_length_decode_doc(
    "Run-length array", ("Return a decoded version of a run-length-encoded input array."),
    {"array"}, "RunLengthDecodeOptions");

static Result<ValueDescr> ResolveEncodeOutput(KernelContext*,
                                              const std::vector<ValueDescr>& descrs) {
  auto output_type = std::make_shared<RunLengthEncodedType>(descrs[0].type);
  return ValueDescr(output_type, ValueDescr::ARRAY);
}

void RegisterVectorRunLengthEncode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_length_encode", Arity::Unary(),
                                                   run_length_encode_doc);

  for (const auto& ty : NumericTypes()) {
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthEncodeGenerator>(ty);
    auto sig = KernelSignature::Make({InputType(ty, ValueDescr::ARRAY)},
                                     OutputType(ResolveEncodeOutput));
    VectorKernel kernel(sig, exec);
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  }
  {
    const auto ty = boolean();
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthEncodeGenerator>(ty);
    auto sig = KernelSignature::Make({InputType(ty, ValueDescr::ARRAY)},
                                     OutputType(ResolveEncodeOutput));
    VectorKernel kernel(sig, exec);
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  }


  DCHECK_OK(registry->AddFunction(std::move(function)));
}

void RegisterVectorRunLengthDecode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_length_decode", Arity::Unary(),
                                                   run_length_decode_doc);

  for (const auto& ty : NumericTypes()) {
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthDecodeGenerator>(ty);
    auto input_type = std::make_shared<RunLengthEncodedType>(ty);
    auto sig = KernelSignature::Make({InputType(input_type, ValueDescr::ARRAY)},
                                     OutputType({ty, ValueDescr::ARRAY}));
    VectorKernel kernel(sig, exec);
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
