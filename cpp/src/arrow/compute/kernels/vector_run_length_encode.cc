#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/rle_util.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename ArrowType, bool has_validity_buffer>
struct EncodeDecodeCommonExec {
  using CType = typename ArrowType::c_type;

  struct Element {
    bool valid;
    CType value;

    bool operator!=(const Element& other) const {
      return valid != other.valid || value != other.value;
    }
  };

  EncodeDecodeCommonExec(KernelContext* kernel_context, const ExecSpan& span,
                         ExecResult* result)
      : kernel_context{kernel_context},
        input_array{span.values[0].array},
        exec_result{result} {
    ARROW_DCHECK(span.num_values() == 1);
  }

  Element ReadValue() {
    Element result;
    if (has_validity_buffer) {
      result.valid =
          bit_util::GetBit(input_validity, input_values_physical_offset + input_position);
    } else {
      result.valid = true;
    }
    result.value = (reinterpret_cast<const CType*>(
        input_values))[input_values_physical_offset + input_position];
    return result;
  }

  void WriteValue(Element element) {
    if (has_validity_buffer) {
      bit_util::SetBitsTo(output_validity, output_position, 1, element.valid);
    }
    (reinterpret_cast<CType*>(output_values))[output_position] = element.value;
  }

  KernelContext* kernel_context;
  const ArraySpan input_array;
  ExecResult* exec_result;
  const uint8_t* input_validity;
  const void* input_values;
  int64_t input_values_physical_offset;
  uint8_t* output_validity;
  void* output_values;
  int64_t input_position;
  int64_t output_position;
};

template <>
EncodeDecodeCommonExec<BooleanType, true>::Element
EncodeDecodeCommonExec<BooleanType, true>::ReadValue() {
  Element result;
  result.valid =
      bit_util::GetBit(input_validity, input_values_physical_offset + input_position);
  if (result.valid) {
    result.value = bit_util::GetBit(reinterpret_cast<const uint8_t*>(input_values),
                                    input_values_physical_offset + input_position);
  }
  return result;
}

template <>
EncodeDecodeCommonExec<BooleanType, false>::Element
EncodeDecodeCommonExec<BooleanType, false>::ReadValue() {
  return {
      .valid = true,
      .value = bit_util::GetBit(reinterpret_cast<const uint8_t*>(input_values),
                                input_values_physical_offset + input_position),
  };
}

template <>
void EncodeDecodeCommonExec<BooleanType, true>::WriteValue(Element element) {
  bit_util::SetBitTo(output_validity, output_position, element.valid);
  if (element.valid) {
    bit_util::SetBitTo(reinterpret_cast<uint8_t*>(output_values), output_position,
                       element.value);
  }
}

template <>
void EncodeDecodeCommonExec<BooleanType, false>::WriteValue(Element element) {
  bit_util::SetBitTo(reinterpret_cast<uint8_t*>(output_values), output_position,
                     element.value);
}

template <typename ArrowType, bool has_validity_buffer>
struct RunLengthEncodeExec
    : public EncodeDecodeCommonExec<ArrowType, has_validity_buffer> {
  using EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::EncodeDecodeCommonExec;
  using typename EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::CType;
  using typename EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::Element;

  Status Exec() {
    if (this->input_array.length == 0) {
      return Status::NotImplemented("TODO");
    }
    this->input_validity = this->input_array.buffers[0].data;
    this->input_values = this->input_array.buffers[1].data;
    this->input_values_physical_offset = this->input_array.offset;

    this->input_position = 0;
    Element element = this->ReadValue();
    int64_t num_values_output = 1;

    // calculate input null count by ourselves. The input span likely got sliced by an
    // ExecSpanIterator using SetOffset right before this code executes. SetOffset leaves
    // the null_count value of the ArraySpan as kUnknownNullCount.
    int64_t input_null_count = element.valid ? 0 : 1;
    int64_t output_null_count = input_null_count;
    for (this->input_position = 1; this->input_position < this->input_array.length;
         this->input_position++) {
      Element previous_element = element;
      element = this->ReadValue();
      if (element != previous_element) {
        num_values_output++;
        if (!element.valid) {
          output_null_count++;
        }
      }
      if (!element.valid) {
        input_null_count++;
      }
    }

    std::shared_ptr<Buffer> validity_buffer = NULLPTR;
    // in bytes
    int64_t validity_buffer_size = 0;
    if (has_validity_buffer) {
      validity_buffer_size = bit_util::BytesForBits(num_values_output);
      ARROW_ASSIGN_OR_RAISE(
          validity_buffer,
          AllocateBuffer(validity_buffer_size, this->kernel_context->memory_pool()));
    }
    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(bit_util::BytesForBits(num_values_output *
                                                                ArrowType().bit_width()),
                                         this->kernel_context->memory_pool()));
    ARROW_ASSIGN_OR_RAISE(auto run_lengths_buffer,
                          AllocateBuffer(num_values_output * sizeof(int64_t),
                                         this->kernel_context->memory_pool()));

    ArrayData* output_array_data = this->exec_result->array_data().get();
    output_array_data->length = this->input_array.length;
    output_array_data->offset = 0;
    output_array_data->buffers.resize(1);
    auto child_array_data =
        ArrayData::Make(const_cast<DataType*>(this->input_array.type)->shared_from_this(),
                        num_values_output);
    output_array_data->buffers[0] = std::move(run_lengths_buffer);
    child_array_data->buffers.push_back(std::move(validity_buffer));
    child_array_data->buffers.push_back(std::move(values_buffer));

    output_array_data->null_count.store(input_null_count);
    child_array_data->null_count = output_null_count;

    this->output_validity = child_array_data->template GetMutableValues<uint8_t>(0);
    this->output_values = child_array_data->template GetMutableValues<uint8_t>(1);
    output_run_lengths = output_array_data->template GetMutableValues<int64_t>(0);
    output_array_data->child_data.push_back(std::move(child_array_data));

    if (has_validity_buffer) {
      // clear last byte in validity buffer, which won't completely be overwritten with
      // validity values
      this->output_validity[validity_buffer_size - 1] = 0;
    }

    this->input_position = 0;
    this->output_position = 0;
    element = this->ReadValue();
    this->WriteValue(element);
    this->output_position = 1;
    for (this->input_position = 1; this->input_position < this->input_array.length;
         this->input_position++) {
      Element previous_element = element;
      element = this->ReadValue();
      if (element != previous_element) {
        this->WriteValue(element);
        // run lengths buffer holds accumulated run length values
        output_run_lengths[this->output_position - 1] = this->input_position;
        this->output_position++;
      }
    }
    output_run_lengths[this->output_position - 1] = this->input_array.length;
    ARROW_DCHECK(this->output_position == num_values_output);
    return Status::OK();
  }

  int64_t* output_run_lengths;
};

template <typename Type>
struct RunLengthEncodeGenerator {
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    bool has_validity_buffer = span.values[0].array.MayHaveNulls();
    if (has_validity_buffer) {
      return RunLengthEncodeExec<Type, true>(ctx, span, result).Exec();
    } else {
      return RunLengthEncodeExec<Type, false>(ctx, span, result).Exec();
    }
  }
};

template <>
struct RunLengthEncodeGenerator<NullType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    // TODO
    return Status::NotImplemented("TODO");
  }
};

template <typename ArrowType, bool has_validity_buffer>
struct RunLengthDecodeExec
    : public EncodeDecodeCommonExec<ArrowType, has_validity_buffer> {
  using EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::EncodeDecodeCommonExec;
  using typename EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::CType;
  using typename EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::Element;

  Status Exec() {
    const ArraySpan& child_array = this->input_array.child_data[0];
    this->input_validity = child_array.buffers[0].data;
    this->input_values = child_array.buffers[1].data;
    input_accumulated_run_length =
        reinterpret_cast<const int64_t*>(this->input_array.buffers[0].data);

    const int64_t logical_offset = this->input_array.offset;
    // common_physical_offset is the physical equivalent to the logical offset that is
    // stored in the offset field of input_array. It is applied to both parent and child
    // buffers.
    const int64_t common_physical_offset = rle_util::FindPhysicalOffset(
        input_accumulated_run_length, child_array.length, logical_offset);
    this->input_values_physical_offset = common_physical_offset + child_array.offset;
    // the child array is not aware of the logical offset of the parent
    const int64_t num_values_input = child_array.length - common_physical_offset;
    ARROW_DCHECK_GT(num_values_input, 0);
    const int64_t num_values_output = this->input_array.length;

    std::shared_ptr<Buffer> validity_buffer = NULLPTR;
    // in bytes
    int64_t validity_buffer_size = 0;
    if (has_validity_buffer) {
      validity_buffer_size = bit_util::BytesForBits(num_values_output);
      ARROW_ASSIGN_OR_RAISE(
          validity_buffer,
          AllocateBuffer(validity_buffer_size, this->kernel_context->memory_pool()));
    }
    ARROW_ASSIGN_OR_RAISE(auto values_buffer,
                          AllocateBuffer(bit_util::BytesForBits(num_values_output *
                                                                ArrowType().bit_width()),
                                         this->kernel_context->memory_pool()));

    ArrayData* output_array_data = this->exec_result->array_data().get();
    output_array_data->length = num_values_output;
    output_array_data->buffers.resize(2);
    output_array_data->offset = 0;
    output_array_data->buffers[0] = std::move(validity_buffer);
    output_array_data->buffers[1] = std::move(values_buffer);

    this->output_validity = output_array_data->template GetMutableValues<uint8_t>(0);
    this->output_values = output_array_data->template GetMutableValues<CType>(1);

    if (has_validity_buffer) {
      // clear last byte in validity buffer, which won't completely be overwritten with
      // validity values
      this->output_validity[validity_buffer_size - 1] = 0;
    }

    this->input_position = 0;
    this->output_position = 0;
    int64_t output_null_count = 0;
    int64_t run_start = logical_offset;
    for (this->input_position = 0; this->input_position < num_values_input;
         this->input_position++) {
      int64_t run_end =
          input_accumulated_run_length[common_physical_offset + this->input_position];
      ARROW_DCHECK_LT(run_start, run_end);
      int64_t run_length = run_end - run_start;
      run_start = run_end;

      Element element = this->ReadValue();
      if (element.valid) {
        for (int64_t run_element = 0; run_element < run_length; run_element++) {
          this->WriteValue(element);
          this->output_position++;
        }
      } else {  // !valid
        bit_util::SetBitsTo(this->output_validity, this->output_position, run_length,
                            false);
        this->output_position += run_length;
        output_null_count += run_length;
      }
    }
    ARROW_DCHECK(this->output_position == num_values_output);
    output_array_data->null_count.store(output_null_count);
    return Status::OK();
  }

  const int64_t* input_accumulated_run_length;
};

template <typename Type>
struct RunLengthDecodeGenerator {
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    bool has_validity_buffer = span.values[0].array.MayHaveNulls();
    if (has_validity_buffer) {
      return RunLengthDecodeExec<Type, true>(ctx, span, result).Exec();
    } else {
      return RunLengthDecodeExec<Type, false>(ctx, span, result).Exec();
    }
  }
};

template <>
struct RunLengthDecodeGenerator<NullType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
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

void RegisterVectorRunLengthEncode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_length_encode", Arity::Unary(),
                                                   run_length_encode_doc);

  for (const auto& ty : NumericTypes()) {
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthEncodeGenerator>(ty);
    auto sig = KernelSignature::Make(
        {InputType(ty)}, OutputType(std::make_shared<RunLengthEncodedType>(ty)));
    VectorKernel kernel(sig, exec);
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  }
  {
    const auto ty = boolean();
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthEncodeGenerator>(ty);
    auto sig = KernelSignature::Make(
        {InputType(ty)}, OutputType(std::make_shared<RunLengthEncodedType>(ty)));
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
    auto sig = KernelSignature::Make({InputType(input_type)}, OutputType({ty}));
    VectorKernel kernel(sig, exec);
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  }
  {
    const auto ty = boolean();
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthDecodeGenerator>(ty);
    auto input_type = std::make_shared<RunLengthEncodedType>(ty);
    auto sig = KernelSignature::Make({InputType(input_type)}, OutputType(ty));
    VectorKernel kernel(sig, exec);
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
