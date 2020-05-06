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

#include "arrow/compute/kernels/cast.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/exec.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/formatting.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/time.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"  // IWYU pragma: keep
#include "arrow/visitor_inline.h"

#include "arrow/compute/kernel.h"

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;

namespace compute {

Status CastNotImplemented(const DataType& in_type, const DataType& out_type) {
  return Status::NotImplemented("No cast implemented from ", in_type.ToString(), " to ",
                                out_type.ToString());
}

// ----------------------------------------------------------------------
// Dictionary to null

template <>
struct CastFunctor<NullType, DictionaryType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    output->buffers = {nullptr};
    output->null_count = output->length;
  }
};

// ----------------------------------------------------------------------
// Null to other things

class FromNullCastKernel : public CastKernelBase {
 public:
  explicit FromNullCastKernel(std::shared_ptr<DataType> out_type)
      : CastKernelBase(std::move(out_type)) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());

    const ArrayData& in_data = *input.array();
    DCHECK_EQ(Type::NA, in_data.type->id());
    auto length = in_data.length;

    // A ArrayData may be preallocated for the output (see InvokeUnaryArrayKernel),
    // however, it doesn't have any actual data, so throw it away and start anew.
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), out_type_, &builder));
    NullBuilderVisitor visitor = {length, builder.get()};
    RETURN_NOT_OK(VisitTypeInline(*out_type_, &visitor));

    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(visitor.builder_->Finish(&out_array));
    out->value = out_array->data();
    return Status::OK();
  }

  struct NullBuilderVisitor {
    // Generic implementation
    Status Visit(const DataType& type) { return builder_->AppendNulls(length_); }

    Status Visit(const StructType& type) {
      RETURN_NOT_OK(builder_->AppendNulls(length_));
      auto& struct_builder = checked_cast<StructBuilder&>(*builder_);
      // Append nulls to all child builders too
      for (int i = 0; i < struct_builder.num_fields(); ++i) {
        NullBuilderVisitor visitor = {length_, struct_builder.field_builder(i)};
        RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &visitor));
      }
      return Status::OK();
    }

    Status Visit(const DictionaryType& type) {
      // XXX (ARROW-5215): Cannot implement this easily, as DictionaryBuilder
      // disregards the index type given in the dictionary type, and instead
      // chooses the smallest possible index type.
      return CastNotImplemented(*null(), type);
    }

    Status Visit(const UnionType& type) { return CastNotImplemented(*null(), type); }

    int64_t length_;
    ArrayBuilder* builder_;
  };
};

// ----------------------------------------------------------------------

class IdentityCast : public CastKernelBase {
 public:
  using CastKernelBase::CastKernelBase;

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(input.kind(), Datum::ARRAY);
    out->value = input.array()->Copy();
    return Status::OK();
  }
};

class ZeroCopyCast : public CastKernelBase {
 public:
  using CastKernelBase::CastKernelBase;

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(input.kind(), Datum::ARRAY);
    auto result = input.array()->Copy();
    result->type = out_type_;
    out->value = result;
    return Status::OK();
  }
};

class ExtensionCastKernel : public CastKernelBase {
 public:
  static Status Make(const DataType& in_type, std::shared_ptr<DataType> out_type,
                     const CastOptions& options,
                     std::unique_ptr<CastKernelBase>* kernel) {
    const auto storage_type = checked_cast<const ExtensionType&>(in_type).storage_type();

    std::unique_ptr<UnaryKernel> storage_caster;
    RETURN_NOT_OK(GetCastFunction(*storage_type, out_type, options, &storage_caster));
    kernel->reset(
        new ExtensionCastKernel(std::move(storage_caster), std::move(out_type)));

    return Status::OK();
  }

  Status Init(const DataType& in_type) override {
    auto& type = checked_cast<const ExtensionType&>(in_type);
    storage_type_ = type.storage_type();
    extension_name_ = type.extension_name();
    return Status::OK();
  }

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(input.kind(), Datum::ARRAY);

    // validate: type is the same as the type the kernel was constructed with
    const auto& input_type = checked_cast<const ExtensionType&>(*input.type());
    if (input_type.extension_name() != extension_name_) {
      return Status::TypeError(
          "The cast kernel was constructed to cast from the extension type named '",
          extension_name_, "' but input has extension type named '",
          input_type.extension_name(), "'");
    }
    if (!input_type.storage_type()->Equals(storage_type_)) {
      return Status::TypeError("The cast kernel was constructed with a storage type: ",
                               storage_type_->ToString(),
                               ", but it is called with a different storage type:",
                               input_type.storage_type()->ToString());
    }

    // construct an ArrayData object with the underlying storage type
    auto new_input = input.array()->Copy();
    new_input->type = storage_type_;
    return InvokeWithAllocation(ctx, storage_caster_.get(), new_input, out);
  }

 protected:
  ExtensionCastKernel(std::unique_ptr<UnaryKernel> storage_caster,
                      std::shared_ptr<DataType> out_type)
      : CastKernelBase(std::move(out_type)), storage_caster_(std::move(storage_caster)) {}

  std::string extension_name_;
  std::shared_ptr<DataType> storage_type_;
  std::unique_ptr<UnaryKernel> storage_caster_;
};

class CastKernel : public CastKernelBase {
 public:
  CastKernel(const CastOptions& options, const CastFunction& func,
             std::shared_ptr<DataType> out_type)
      : CastKernelBase(std::move(out_type)), options_(options), func_(func) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(input.kind(), Datum::ARRAY);
    DCHECK_EQ(out->kind(), Datum::ARRAY);

    const ArrayData& in_data = *input.array();
    ArrayData* result = out->array().get();

    RETURN_NOT_OK(detail::PropagateNulls(ctx, in_data, result));

    func_(ctx, options_, in_data, result);
    ARROW_RETURN_IF_ERROR(ctx);
    return Status::OK();
  }

 private:
  CastOptions options_;
  CastFunction func_;
};

class DictionaryCastKernel : public CastKernel {
 public:
  using CastKernel::CastKernel;

  Status Init(const DataType& in_type) override {
    const auto value_type = checked_cast<const DictionaryType&>(in_type).value_type();
    if (!out_type_->Equals(value_type)) {
      return CastNotImplemented(in_type, *out_type_);
    }
    return Status::OK();
  }
};

#define CAST_CASE(InType, OutType)                                                      \
  case OutType::type_id:                                                                \
    func = [](FunctionContext* ctx, const CastOptions& options, const ArrayData& input, \
              ArrayData* out) {                                                         \
      CastFunctor<OutType, InType> func;                                                \
      func(ctx, options, input, out);                                                   \
    };                                                                                  \
    break;

#define GET_CAST_FUNCTION(CASE_GENERATOR, InType, KernelType)           \
  static std::unique_ptr<CastKernelBase> Get##InType##CastFunc(         \
      std::shared_ptr<DataType> out_type, const CastOptions& options) { \
    CastFunction func;                                                  \
    switch (out_type->id()) {                                           \
      CASE_GENERATOR(CAST_CASE);                                        \
      default:                                                          \
        break;                                                          \
    }                                                                   \
    if (func != nullptr) {                                              \
      return std::unique_ptr<CastKernelBase>(                           \
          new KernelType(options, func, std::move(out_type)));          \
    }                                                                   \
    return nullptr;                                                     \
  }

#include "generated/cast_codegen_internal.h"  // NOLINT

GET_CAST_FUNCTION(BOOLEAN_CASES, BooleanType, CastKernel)
GET_CAST_FUNCTION(UINT8_CASES, UInt8Type, CastKernel)
GET_CAST_FUNCTION(INT8_CASES, Int8Type, CastKernel)
GET_CAST_FUNCTION(UINT16_CASES, UInt16Type, CastKernel)
GET_CAST_FUNCTION(INT16_CASES, Int16Type, CastKernel)
GET_CAST_FUNCTION(UINT32_CASES, UInt32Type, CastKernel)
GET_CAST_FUNCTION(INT32_CASES, Int32Type, CastKernel)
GET_CAST_FUNCTION(UINT64_CASES, UInt64Type, CastKernel)
GET_CAST_FUNCTION(INT64_CASES, Int64Type, CastKernel)
GET_CAST_FUNCTION(FLOAT_CASES, FloatType, CastKernel)
GET_CAST_FUNCTION(DOUBLE_CASES, DoubleType, CastKernel)
GET_CAST_FUNCTION(DECIMAL128_CASES, Decimal128Type, CastKernel)
GET_CAST_FUNCTION(DATE32_CASES, Date32Type, CastKernel)
GET_CAST_FUNCTION(DATE64_CASES, Date64Type, CastKernel)
GET_CAST_FUNCTION(TIME32_CASES, Time32Type, CastKernel)
GET_CAST_FUNCTION(TIME64_CASES, Time64Type, CastKernel)
GET_CAST_FUNCTION(TIMESTAMP_CASES, TimestampType, CastKernel)
GET_CAST_FUNCTION(DURATION_CASES, DurationType, CastKernel)
GET_CAST_FUNCTION(BINARY_CASES, BinaryType, CastKernel)
GET_CAST_FUNCTION(STRING_CASES, StringType, CastKernel)
GET_CAST_FUNCTION(LARGEBINARY_CASES, LargeBinaryType, CastKernel)
GET_CAST_FUNCTION(LARGESTRING_CASES, LargeStringType, CastKernel)
GET_CAST_FUNCTION(DICTIONARY_CASES, DictionaryType, DictionaryCastKernel)

#define CAST_FUNCTION_CASE(InType)                          \
  case InType::type_id:                                     \
    cast_kernel = Get##InType##CastFunc(out_type, options); \
    break

namespace {

template <typename TypeClass>
Status GetListCastFunc(const DataType& in_type, std::shared_ptr<DataType> out_type,
                       const CastOptions& options,
                       std::unique_ptr<CastKernelBase>* kernel) {
  if (out_type->id() != TypeClass::type_id) {
    return Status::Invalid("Cannot cast from ", in_type.ToString(), " to ",
                           out_type->ToString());
  }
  const DataType& in_value_type = *checked_cast<const TypeClass&>(in_type).value_type();
  std::shared_ptr<DataType> out_value_type =
      checked_cast<const TypeClass&>(*out_type).value_type();
  std::unique_ptr<UnaryKernel> child_caster;
  RETURN_NOT_OK(GetCastFunction(in_value_type, out_value_type, options, &child_caster));
  *kernel = std::unique_ptr<CastKernelBase>(
      new ListCastKernel<TypeClass>(std::move(child_caster), std::move(out_type)));
  return Status::OK();
}

}  // namespace

inline bool IsZeroCopyCast(Type::type in_type, Type::type out_type) {
  switch (in_type) {
    case Type::INT32:
      return (out_type == Type::DATE32) || (out_type == Type::TIME32);
    case Type::INT64:
      return ((out_type == Type::DATE64) || (out_type == Type::TIME64) ||
              (out_type == Type::TIMESTAMP) || (out_type == Type::DURATION));
    case Type::DATE32:
    case Type::TIME32:
      return out_type == Type::INT32;
    case Type::DATE64:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::DURATION:
      return out_type == Type::INT64;
    default:
      break;
  }
  return false;
}

Status GetCastFunction(const DataType& in_type, std::shared_ptr<DataType> out_type,
                       const CastOptions& options, std::unique_ptr<UnaryKernel>* kernel) {
  if (in_type.Equals(out_type)) {
    kernel->reset(new IdentityCast(std::move(out_type)));
    return Status::OK();
  }

  if (IsZeroCopyCast(in_type.id(), out_type->id())) {
    kernel->reset(new ZeroCopyCast(std::move(out_type)));
    return Status::OK();
  }

  std::unique_ptr<CastKernelBase> cast_kernel;
  switch (in_type.id()) {
    CAST_FUNCTION_CASE(BooleanType);
    CAST_FUNCTION_CASE(UInt8Type);
    CAST_FUNCTION_CASE(Int8Type);
    CAST_FUNCTION_CASE(UInt16Type);
    CAST_FUNCTION_CASE(Int16Type);
    CAST_FUNCTION_CASE(UInt32Type);
    CAST_FUNCTION_CASE(Int32Type);
    CAST_FUNCTION_CASE(UInt64Type);
    CAST_FUNCTION_CASE(Int64Type);
    CAST_FUNCTION_CASE(FloatType);
    CAST_FUNCTION_CASE(DoubleType);
    CAST_FUNCTION_CASE(Decimal128Type);
    CAST_FUNCTION_CASE(Date32Type);
    CAST_FUNCTION_CASE(Date64Type);
    CAST_FUNCTION_CASE(Time32Type);
    CAST_FUNCTION_CASE(Time64Type);
    CAST_FUNCTION_CASE(TimestampType);
    CAST_FUNCTION_CASE(DurationType);
    CAST_FUNCTION_CASE(BinaryType);
    CAST_FUNCTION_CASE(StringType);
    CAST_FUNCTION_CASE(LargeBinaryType);
    CAST_FUNCTION_CASE(LargeStringType);
    CAST_FUNCTION_CASE(DictionaryType);
    case Type::NA:
      cast_kernel.reset(new FromNullCastKernel(out_type));
      break;
    case Type::LIST:
      RETURN_NOT_OK(GetListCastFunc<ListType>(in_type, out_type, options, &cast_kernel));
      break;
    case Type::LARGE_LIST:
      RETURN_NOT_OK(
          GetListCastFunc<LargeListType>(in_type, out_type, options, &cast_kernel));
      break;
    case Type::EXTENSION:
      RETURN_NOT_OK(
          ExtensionCastKernel::Make(std::move(in_type), out_type, options, &cast_kernel));
      break;
    default:
      break;
  }
  if (cast_kernel == nullptr) {
    return CastNotImplemented(in_type, *out_type);
  }
  Status st = cast_kernel->Init(in_type);
  if (st.ok()) {
    *kernel = std::move(cast_kernel);
  }
  return st;
}

Status Cast(FunctionContext* ctx, const Datum& value, std::shared_ptr<DataType> out_type,
            const CastOptions& options, Datum* out) {
  const DataType& in_type = *value.type();

  // Dynamic dispatch to obtain right cast function
  std::unique_ptr<UnaryKernel> func;
  RETURN_NOT_OK(GetCastFunction(in_type, std::move(out_type), options, &func));
  return InvokeWithAllocation(ctx, func.get(), value, out);
}

Status Cast(FunctionContext* ctx, const Array& array, std::shared_ptr<DataType> out_type,
            const CastOptions& options, std::shared_ptr<Array>* out) {
  Datum datum_out;
  RETURN_NOT_OK(Cast(ctx, Datum(array.data()), std::move(out_type), options, &datum_out));
  DCHECK_EQ(Datum::ARRAY, datum_out.kind());
  *out = MakeArray(datum_out.array());
  return Status::OK();
}
// ----------------------------------------------------------------------
// Casting

Result<std::shared_ptr<Array>> Cast(const Array& value, std::shared_ptr<DataType> to_type,
                                    const CastOptions& options, ExecContext* context) {
  return Status::NotImplemented("NYI");
}

Result<Datum> Cast(const Datum& value, std::shared_ptr<DataType> to_type,
                   const CastOptions& options, ExecContext* context) {
  return Status::NotImplemented("NYI");
}

bool CanCast(const DataType& from_type, const DataType& to_type) {
  // TODO
  return false;
}

}  // namespace compute
}  // namespace arrow
