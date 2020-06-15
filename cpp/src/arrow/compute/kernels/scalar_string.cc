// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
#include <cctype>
#include <string>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_string_internal.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

// TODO: optional ascii validation

struct AsciiLength {
  template <typename OUT, typename ARG0 = util::string_view>
  static OUT Call(KernelContext*, ARG0 val) {
    return static_cast<OUT>(val.size());
  }
};

using TransformFunc = std::function<void(const uint8_t*, int64_t, uint8_t*)>;

void StringDataTransform(KernelContext* ctx, const ExecBatch& batch,
                         TransformFunc transform, Datum* out) {
  if (batch[0].kind() == Datum::ARRAY) {
    const ArrayData& input = *batch[0].array();
    ArrayData* out_arr = out->mutable_array();
    // Reuse offsets from input
    out_arr->buffers[1] = input.buffers[1];
    int64_t data_nbytes = input.buffers[2]->size();
    KERNEL_RETURN_IF_ERROR(ctx, ctx->Allocate(data_nbytes).Value(&out_arr->buffers[2]));
    transform(input.buffers[2]->data(), data_nbytes, out_arr->buffers[2]->mutable_data());
  } else {
    const auto& input = checked_cast<const BaseBinaryScalar&>(*batch[0].scalar());
    auto result = checked_pointer_cast<BaseBinaryScalar>(MakeNullScalar(out->type()));
    if (input.is_valid) {
      result->is_valid = true;
      int64_t data_nbytes = input.value->size();
      KERNEL_RETURN_IF_ERROR(ctx, ctx->Allocate(data_nbytes).Value(&result->value));
      transform(input.value->data(), data_nbytes, result->value->mutable_data());
    }
    out->value = result;
  }
}

void TransformAsciiUpper(const uint8_t* input, int64_t length, uint8_t* output) {
  for (int64_t i = 0; i < length; ++i) {
    const uint8_t utf8_code_unit = *input++;
    // Code units in the range [a-z] can only be an encoding of an ascii
    // character/codepoint, not the 2nd, 3rd or 4th code unit (byte) of an different
    // codepoint. This guaranteed by non-overlap design of the unicode standard. (see
    // section 2.5 of Unicode Standard Core Specification v13.0)
    *output++ = ((utf8_code_unit >= 'a') && (utf8_code_unit <= 'z'))
                    ? (utf8_code_unit - 32)
                    : utf8_code_unit;
  }
}

void AsciiUpperExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  StringDataTransform(ctx, batch, TransformAsciiUpper, out);
}

void TransformAsciiLower(const uint8_t* input, int64_t length, uint8_t* output) {
  for (int64_t i = 0; i < length; ++i) {
    // As with TransformAsciiUpper, the same guarantee holds for the range [A-Z]
    const uint8_t utf8_code_unit = *input++;
    *output++ = ((utf8_code_unit >= 'A') && (utf8_code_unit <= 'Z'))
                    ? (utf8_code_unit + 32)
                    : utf8_code_unit;
  }
}

void AsciiLowerExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  StringDataTransform(ctx, batch, TransformAsciiLower, out);
}

void AddAsciiLength(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("ascii_length", Arity::Unary());
  ArrayKernelExec exec_offset_32 =
      codegen::ScalarUnaryNotNull<Int32Type, StringType, AsciiLength>::Exec;
  ArrayKernelExec exec_offset_64 =
      codegen::ScalarUnaryNotNull<Int64Type, LargeStringType, AsciiLength>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, int32(), exec_offset_32));
  DCHECK_OK(func->AddKernel({large_utf8()}, int64(), exec_offset_64));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// ----------------------------------------------------------------------
// strptime string parsing

using StrptimeWrapper = OptionsWrapper<StrptimeOptions>;

struct ParseStrptime {
  explicit ParseStrptime(const StrptimeOptions& options)
      : parser(TimestampParser::MakeStrptime(options.format)), unit(options.unit) {}

  template <typename... Ignored>
  int64_t Call(KernelContext* ctx, util::string_view val) const {
    int64_t result = 0;
    if (!(*parser)(val.data(), val.size(), unit, &result)) {
      ctx->SetStatus(Status::Invalid("Failed to parse string ", val));
    }
    return result;
  }

  std::shared_ptr<TimestampParser> parser;
  TimeUnit::type unit;
};

template <typename InputType>
void StrptimeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& options = checked_cast<const StrptimeWrapper*>(ctx->state())->options;
  codegen::ScalarUnaryNotNullStateful<TimestampType, InputType, ParseStrptime> kernel{
      ParseStrptime(options)};
  return kernel.Exec(ctx, batch, out);
}

Result<ValueDescr> StrptimeResolve(KernelContext* ctx, const std::vector<ValueDescr>&) {
  const auto& options = checked_cast<const StrptimeWrapper*>(ctx->state())->options;
  return ::arrow::timestamp(options.unit);
}

void AddStrptime(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("strptime", Arity::Unary());
  DCHECK_OK(func->AddKernel({utf8()}, OutputType(StrptimeResolve),
                            StrptimeExec<StringType>, InitWrapOptions<StrptimeOptions>));
  DCHECK_OK(func->AddKernel({large_utf8()}, OutputType(StrptimeResolve),
                            StrptimeExec<LargeStringType>,
                            InitWrapOptions<StrptimeOptions>));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void MakeUnaryStringBatchKernel(std::string name, ArrayKernelExec exec,
                                FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary());
  DCHECK_OK(func->AddKernel({utf8()}, utf8(), exec));
  DCHECK_OK(func->AddKernel({large_utf8()}, large_utf8(), exec));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace

void RegisterScalarStringAscii(FunctionRegistry* registry) {
  MakeUnaryStringBatchKernel("ascii_upper", AsciiUpperExec, registry);
  MakeUnaryStringBatchKernel("ascii_lower", AsciiLowerExec, registry);
  AddAsciiLength(registry);
  AddStrptime(registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
