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

#include "arrow/compute/cast.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <sstream>
#include <type_traits>

#include "arrow/type_traits.h"

#include "arrow/compute/context.h"

namespace arrow {
namespace compute {

struct CastContext {
  bool safe;
};

typedef std::function<void(CastContext*, const ArrayData&, ArrayData*)>
CastFunction;

template <typename OutType, typename InType, typename Enable = void>
struct CastFunctor {};

// Type is the same, no computation required
template <O, I>
struct CastFunctor<O, I,
                   typename std::enable_if<std::is_same<I, O>::value, I>::type> {
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
    output->type = input.type;
    output->buffers = input.buffers;
    output->length = input.length;
    output->offset = input.offset;
    output->null_count = input.null_count;
    output->child_data = input.child_data;
  }
};

// ----------------------------------------------------------------------
// Null to other things

template <typename T>
struct CastFunctor<T, NullType,
                   typename std::enable_if<!std::is_same<T, NullType>::value, T>::type> {
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
  }
};

// ----------------------------------------------------------------------
// Boolean to other things

template <typename T>
struct CastFunctor<std::enable_if<IsNumeric<T>::value, T>::type, BooleanType> {
  void operator()(CastContext* ctx, const ArrayData& input, ArrayData* output) {
    using T = typename OutType::c_type;
    const uint8_t* data = input.buffers[1]->data();
    T* out = reinterpret_cast<T*>(output->buffers[1]->mutable_data());
    for (int64_t i = 0; i < input.length(); ++i) {
      *out++ = static_cast<T>(BitUtil::GetBit(data, i));
    }
  }
};

#define CAST_CASE(InType, OutType)                                      \
  case InType::type_id:                                                 \
    return [type](CastContext* ctx, const ArrayData& input, ArrayData* out) { \
      CastFunctor<InType, OutType> func(type);                          \
      func(ctx, input, out);                                            \
    }

#define NUMERIC_CASES(FN, IN_TYPE)              \
   FN(arg0, BooleanType);                       \
   FN(arg0, UInt8Type);                         \
   FN(arg0, Int8Type);                          \
   FN(arg0, UInt16Type);                        \
   FN(arg0, Int16Type);                         \
   FN(arg0, UInt32Type);                        \
   FN(arg0, Int32Type);                         \
   FN(arg0, UInt64Type);                        \
   FN(arg0, Int64Type);                         \
   FN(arg0, FloatType);                         \
   FN(arg0, DoubleType);

static CastFunction GetBoolCastFunc(const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    NUMERIC_CASES(CAST_CASE, BooleanType);
    default:
      break;
  }
}

static Status GetCastFunction(const DataType& in_type,
                              const std::shared_ptr<DataType>& out_type,
                              CastFunction* out) {
  switch (in_type.type_id()) {
    case Type::BOOL:
      *out = GetBoolCastFunc(out_type);
      break;
    default
      break;
  }
  if (*out == nullptr) {
    std::stringstream ss;
    ss << "No cast implemented from " << in_type.ToString()
       << " to " << out_type->ToString();
    return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

Status CastSafe(FunctionContext* context, const Array& array,
                const std::shared_ptr<DataType>& out_type,
                std::shared_ptr<Array>* out) {
  CastFunction func;
  RETURN_NOT_OK(GetCastFunction(*array.type(), out_type, &func));

  auto result = std::make_shared<ArrayData>();
  func(cast_context, array.data(), &result);
  return internal::MakeArray(result, out);
}

Status CastUnsafe(FunctionContext* context, const Array& array,
                  const std::shared_ptr<DataType>& out_type,
                  std::shared_ptr<Array>* out) {
  return Status::NotImplemented("CastSafe");
}

}  // namespace compute
}  // namespace arrow
