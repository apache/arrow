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
      result.valid = bit_util::GetBit(input_validity, read_offset);
    } else {
      result.valid = true;
    }
    result.value = (reinterpret_cast<const CType*>(input_values))[read_offset];
    return result;
  }

  void WriteValue(Element element) {
    if (has_validity_buffer) {
      bit_util::SetBitsTo(output_validity, write_offset, 1, element.valid);
    }
    (reinterpret_cast<CType*>(output_values))[write_offset] = element.value;
  }

  KernelContext* kernel_context;
  const ArraySpan input_array;
  ExecResult* exec_result;
  const uint8_t* input_validity;
  const void* input_values;
  uint8_t* output_validity;
  void* output_values;
  // read offset is a physical index into the values buffer, including array offsets
  int64_t read_offset;
  int64_t write_offset;
};

template <>
EncodeDecodeCommonExec<BooleanType, true>::Element
EncodeDecodeCommonExec<BooleanType, true>::ReadValue() {
  Element result;
  result.valid = bit_util::GetBit(input_validity, read_offset);
  if (result.valid) {
    result.value =
        bit_util::GetBit(reinterpret_cast<const uint8_t*>(input_values), read_offset);
  }
  return result;
}

template <>
EncodeDecodeCommonExec<BooleanType, false>::Element
EncodeDecodeCommonExec<BooleanType, false>::ReadValue() {
  return {
      .valid = true,
      .value =
          bit_util::GetBit(reinterpret_cast<const uint8_t*>(input_values), read_offset),
  };
}

template <>
void EncodeDecodeCommonExec<BooleanType, true>::WriteValue(Element element) {
  bit_util::SetBitTo(output_validity, write_offset, element.valid);
  if (element.valid) {
    bit_util::SetBitTo(reinterpret_cast<uint8_t*>(output_values), write_offset,
                       element.value);
  }
}

template <>
void EncodeDecodeCommonExec<BooleanType, false>::WriteValue(Element element) {
  bit_util::SetBitTo(reinterpret_cast<uint8_t*>(output_values), write_offset,
                     element.value);
}

struct RunLengthEncodeState : public KernelState {
  RunLengthEncodeState(std::shared_ptr<DataType> run_ends_type)
      : run_ends_type{run_ends_type} {}

  std::shared_ptr<DataType> run_ends_type;
};

template <typename ArrowType, typename RunEndsType, bool has_validity_buffer>
struct RunLengthEncodeExec
    : public EncodeDecodeCommonExec<ArrowType, has_validity_buffer> {
  using EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::EncodeDecodeCommonExec;
  using typename EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::CType;
  using typename EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::Element;
  using RunEndsCType = typename RunEndsType::c_type;

  Status Exec() {
    ArrayData* output_array_data = this->exec_result->array_data().get();
    if (this->input_array.length == 0) {
      ARROW_ASSIGN_OR_RAISE(auto run_ends_buffer,
                            AllocateBitmap(0, this->kernel_context->memory_pool()));
      output_array_data->length = 0;
      output_array_data->offset = 0;
      output_array_data->null_count = 0;
      output_array_data->buffers = {NULLPTR};
      output_array_data->child_data.resize(2);
      output_array_data->child_data[0] =
          ArrayData::Make(std::make_shared<RunEndsType>(),
                          /*length =*/0,
                          /*buffers =*/{NULLPTR, run_ends_buffer},
                          /*null_count =*/0);
      output_array_data->child_data[1] = this->input_array.ToArrayData();
      return Status::OK();
    }
    if (this->input_array.length > std::numeric_limits<RunEndsCType>::max()) {
      return Status::Invalid(
          "Cannot run-length encode Arrays with more elements than the run ends type can "
          "hold");
    }
    this->input_validity = this->input_array.buffers[0].data;
    this->input_values = this->input_array.buffers[1].data;
    int64_t input_offset = this->input_array.offset;

    this->read_offset = input_offset;
    Element element = this->ReadValue();
    int64_t num_values_output = 1;

    // calculate input null count by ourselves. The input span likely got sliced by an
    // ExecSpanIterator using SetOffset right before this code executes. SetOffset leaves
    // the null_count value of the ArraySpan as kUnknownNullCount.
    int64_t input_null_count = element.valid ? 0 : 1;
    int64_t output_null_count = input_null_count;
    for (int64_t input_position = 1; input_position < this->input_array.length;
         input_position++) {
      this->read_offset = input_offset + input_position;
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
    ARROW_ASSIGN_OR_RAISE(auto run_ends_buffer,
                          AllocateBuffer(num_values_output * sizeof(RunEndsCType),
                                         this->kernel_context->memory_pool()));

    output_array_data->length = this->input_array.length;
    output_array_data->offset = 0;
    output_array_data->buffers = {NULLPTR};
    output_array_data->null_count.store(0);
    auto values_array_data = ArrayData::Make(
        this->input_array.type->GetSharedPtr(), num_values_output,
        {std::move(validity_buffer), std::move(values_buffer)}, output_null_count);
    auto run_ends_array_data =
        ArrayData::Make(std::make_shared<RunEndsType>(), num_values_output,
                        {NULLPTR, std::move(run_ends_buffer)}, 0);

    // set mutable pointers for output; `WriteValue` uses member `output_` variables
    this->output_validity = values_array_data->template GetMutableValues<uint8_t>(0);
    this->output_values = values_array_data->template GetMutableValues<uint8_t>(1);
    auto output_run_ends =
        run_ends_array_data->template GetMutableValues<RunEndsCType>(1);

    output_array_data->child_data.resize(2);
    output_array_data->child_data[1] = std::move(values_array_data);
    output_array_data->child_data[0] = std::move(run_ends_array_data);

    if (has_validity_buffer) {
      // clear last byte in validity buffer, which won't completely be overwritten with
      // validity values
      this->output_validity[validity_buffer_size - 1] = 0;
    }

    this->read_offset = input_offset;
    this->write_offset = 0;
    element = this->ReadValue();
    this->WriteValue(element);
    this->write_offset = 1;
    for (int64_t input_position = 1; input_position < this->input_array.length;
         input_position++) {
      this->read_offset = input_offset + input_position;
      Element previous_element = element;
      element = this->ReadValue();
      if (element != previous_element) {
        this->WriteValue(element);
        // run lengths buffer holds accumulated run length values
        output_run_ends[this->write_offset - 1] = this->read_offset - input_offset;
        this->write_offset++;
      }
    }
    output_run_ends[this->write_offset - 1] = this->input_array.length;
    ARROW_DCHECK(this->write_offset == num_values_output);
    return Status::OK();
  }
};

template <typename ValuesType, typename RunEndsType>
static Status RunLengthEncodeValidiyGenerate(KernelContext* ctx, const ExecSpan& span,
                                             ExecResult* result) {
  bool has_validity_buffer = span.values[0].array.MayHaveNulls();
  if (has_validity_buffer) {
    return RunLengthEncodeExec<ValuesType, RunEndsType, true>(ctx, span, result).Exec();
  } else {
    return RunLengthEncodeExec<ValuesType, RunEndsType, false>(ctx, span, result).Exec();
  }
}

template <typename ValuesType>
struct RunLengthEncodeGenerator {
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    auto state = checked_cast<const RunLengthEncodeState*>(ctx->state());
    switch (state->run_ends_type->id()) {
      case Type::INT16:
        return RunLengthEncodeValidiyGenerate<ValuesType, Int16Type>(ctx, span, result);
      case Type::INT32:
        return RunLengthEncodeValidiyGenerate<ValuesType, Int32Type>(ctx, span, result);
      case Type::INT64:
        return RunLengthEncodeValidiyGenerate<ValuesType, Int64Type>(ctx, span, result);
      default:
        return Status::Invalid("Invalid run ends type: ", *state->run_ends_type);
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

Result<std::unique_ptr<KernelState>> RunLengthEncodeInit(KernelContext*,
                                                         const KernelInitArgs& args) {
  auto options = checked_cast<const RunLengthEncodeOptions*>(args.options);
  return std::make_unique<RunLengthEncodeState>(options->run_ends_type);
}

template <typename ArrowType, typename RunEndsType, bool has_validity_buffer>
struct RunLengthDecodeExec
    : public EncodeDecodeCommonExec<ArrowType, has_validity_buffer> {
  using EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::EncodeDecodeCommonExec;
  using typename EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::CType;
  using typename EncodeDecodeCommonExec<ArrowType, has_validity_buffer>::Element;
  using RunEndsCType = typename RunEndsType::c_type;

  Status Exec() {
    ArrayData* output_array_data = this->exec_result->array_data().get();
    const ArraySpan& data_array = rle_util::ValuesArray(this->input_array);
    this->input_validity = data_array.buffers[0].data;
    this->input_values = data_array.buffers[1].data;

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

    this->write_offset = 0;
    int64_t output_null_count = 0;
    for (auto it = rle_util::MergedRunsIterator<RunEndsCType>(this->input_array);
         it != rle_util::MergedRunsIterator(); it++) {
      this->read_offset = it.template index_into_buffer<0>();
      Element element = this->ReadValue();
      if (element.valid) {
        for (int64_t run_element = 0; run_element < it.run_length(); run_element++) {
          this->WriteValue(element);
          this->write_offset++;
        }
      } else {  // !valid
        bit_util::SetBitsTo(this->output_validity, this->write_offset, it.run_length(),
                            false);
        this->write_offset += it.run_length();
        output_null_count += it.run_length();
      }
    }
    ARROW_DCHECK(this->write_offset == num_values_output);
    output_array_data->null_count.store(output_null_count);
    return Status::OK();
  }

  const RunEndsCType* input_accumulated_run_length;
};

template <typename ValuesType, typename RunEndsType>
static Status RunLengthDecodeValidiyGenerate(KernelContext* ctx, const ExecSpan& span,
                                             ExecResult* result) {
  bool has_validity_buffer = rle_util::ValuesArray(span.values[0].array).MayHaveNulls();
  if (has_validity_buffer) {
    return RunLengthDecodeExec<ValuesType, RunEndsType, true>(ctx, span, result).Exec();
  } else {
    return RunLengthDecodeExec<ValuesType, RunEndsType, false>(ctx, span, result).Exec();
  }
}

template <typename ValuesType>
struct RunLengthDecodeGenerator {
  static Status Exec(KernelContext* ctx, const ExecSpan& span, ExecResult* result) {
    auto& run_ends_type =
        checked_cast<const RunLengthEncodedType&>(*span.GetTypes()[0]).run_ends_type();
    switch (run_ends_type->id()) {
      case Type::INT16:
        return RunLengthDecodeValidiyGenerate<ValuesType, Int16Type>(ctx, span, result);
      case Type::INT32:
        return RunLengthDecodeValidiyGenerate<ValuesType, Int32Type>(ctx, span, result);
      case Type::INT64:
        return RunLengthDecodeValidiyGenerate<ValuesType, Int64Type>(ctx, span, result);
      default:
        return Status::Invalid("Invalid run ends type: ", *run_ends_type);
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
    "Run-length encode array",
    ("Return a run-length-encoded version of the input array."), {"array"},
    "RunLengthEncodeOptions", true);
static const FunctionDoc run_length_decode_doc(
    "Decode run-length encoded array",
    ("Return a decoded version of a run-length-encoded input array."), {"array"});

static Result<TypeHolder> VectorRunLengthEncodeResolver(
    KernelContext* ctx, const std::vector<TypeHolder>& inputTypes) {
  auto state = checked_cast<const RunLengthEncodeState*>(ctx->state());
  return TypeHolder(std::make_shared<RunLengthEncodedType>(state->run_ends_type,
                                                           inputTypes[0].GetSharedPtr()));
}

void RegisterVectorRunLengthEncode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_length_encode", Arity::Unary(),
                                                   run_length_encode_doc);

  for (const auto& ty : NumericTypes()) {
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthEncodeGenerator>(ty);
    auto sig =
        KernelSignature::Make({InputType(ty)}, OutputType(VectorRunLengthEncodeResolver));
    VectorKernel kernel(sig, exec, RunLengthEncodeInit);
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  }
  {
    const auto ty = boolean();
    auto exec = GenerateTypeAgnosticPrimitive<RunLengthEncodeGenerator>(ty);
    auto sig =
        KernelSignature::Make({InputType(ty)}, OutputType(VectorRunLengthEncodeResolver));
    VectorKernel kernel(sig, exec, RunLengthEncodeInit);
    DCHECK_OK(function->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

void RegisterVectorRunLengthDecode(FunctionRegistry* registry) {
  auto function = std::make_shared<VectorFunction>("run_length_decode", Arity::Unary(),
                                                   run_length_decode_doc);

  for (const auto& run_ends_type : {int16(), int32(), int64()}) {
    for (const auto& ty : NumericTypes()) {
      auto exec = GenerateTypeAgnosticPrimitive<RunLengthDecodeGenerator>(ty);
      auto input_type = std::make_shared<RunLengthEncodedType>(run_ends_type, ty);
      auto sig = KernelSignature::Make({InputType(input_type)}, OutputType({ty}));
      VectorKernel kernel(sig, exec);
      DCHECK_OK(function->AddKernel(std::move(kernel)));
    }
    {
      const auto ty = boolean();
      auto exec = GenerateTypeAgnosticPrimitive<RunLengthDecodeGenerator>(ty);
      auto input_type = std::make_shared<RunLengthEncodedType>(run_ends_type, ty);
      auto sig = KernelSignature::Make({InputType(input_type)}, OutputType(ty));
      VectorKernel kernel(sig, exec);
      DCHECK_OK(function->AddKernel(std::move(kernel)));
    }
  }

  DCHECK_OK(registry->AddFunction(std::move(function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
