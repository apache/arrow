// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
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
#include <memory>
#include <utility>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/mask.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

Status Mask(FunctionContext* context, const Array& values, const Array& mask,
            std::shared_ptr<Array>* out) {
  Datum out_datum;
  RETURN_NOT_OK(Mask(context, Datum(values.data()), Datum(mask.data()), &out_datum));
  *out = out_datum.make_array();
  return Status::OK();
}

Status Mask(FunctionContext* context, const Datum& values, const Datum& mask,
            Datum* out) {
  MaskKernel kernel(values.type());
  RETURN_NOT_OK(kernel.Call(context, values, mask, out));
  return Status::OK();
}

struct MaskParameters {
  FunctionContext* context;
  std::shared_ptr<Array> values, mask;
  std::shared_ptr<Array>* out;
};

template <typename Builder, typename Scalar>
static Status UnsafeAppend(Builder* builder, Scalar&& value) {
  builder->UnsafeAppend(std::forward<Scalar>(value));
  return Status::OK();
}

static Status UnsafeAppend(BinaryBuilder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

static Status UnsafeAppend(StringBuilder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

// TODO(bkietz) this can be optimized
static int64_t OutputSize(const BooleanArray& mask) {
  auto offset = mask.offset();
  auto length = mask.length();
  internal::BitmapReader mask_data(mask.data()->buffers[1]->data(), offset, length);
  int64_t size = 0;
  for (auto i = offset; i < offset + length; ++i) {
    if (mask.IsNull(i) || mask_data.IsSet()) {
      ++size;
    }
    mask_data.Next();
  }
  return size;
}

template <bool AllValuesValid, bool WholeMaskValid, typename ValueArray,
          typename OutBuilder>
Status MaskImpl(FunctionContext*, const ValueArray& values, const BooleanArray& mask,
                OutBuilder* builder) {
  auto offset = mask.offset();
  auto length = mask.length();
  internal::BitmapReader mask_data(mask.data()->buffers[1]->data(), offset, length);
  for (int64_t i = 0; i < mask.length(); mask_data.Next(), ++i) {
    if (!WholeMaskValid && mask.IsNull(i)) {
      builder->UnsafeAppendNull();
      continue;
    }
    if (mask_data.IsNotSet()) {
      continue;
    }
    if (!AllValuesValid && values.IsNull(i)) {
      builder->UnsafeAppendNull();
      continue;
    }
    RETURN_NOT_OK(UnsafeAppend(builder, values.GetView(i)));
  }
  return Status::OK();
}

template <bool AllValuesValid, typename ValueArray, typename MaskArray,
          typename OutBuilder>
Status UnpackMaskNullCount(FunctionContext* context, const ValueArray& values,
                           const MaskArray& mask, OutBuilder* builder) {
  if (mask.null_count() == 0) {
    return MaskImpl<AllValuesValid, true>(context, values, mask, builder);
  }
  return MaskImpl<AllValuesValid, false>(context, values, mask, builder);
}

template <typename ValueArray, typename MaskArray, typename OutBuilder>
Status UnpackValuesNullCount(FunctionContext* context, const ValueArray& values,
                             const MaskArray& mask, OutBuilder* builder) {
  if (values.null_count() == 0) {
    return UnpackMaskNullCount<true>(context, values, mask, builder);
  }
  return UnpackMaskNullCount<false>(context, values, mask, builder);
}

template <typename T>
using ArrayType = typename TypeTraits<T>::ArrayType;

template <typename MaskType>
struct UnpackValues {
  template <typename ValueType>
  Status Visit(const ValueType&) {
    using OutBuilder = typename TypeTraits<ValueType>::BuilderType;
    auto&& mask = static_cast<const ArrayType<MaskType>&>(*params_.mask);
    auto&& values = static_cast<const ArrayType<ValueType>&>(*params_.values);
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(params_.context->memory_pool(), values.type(), &builder));
    RETURN_NOT_OK(builder->Reserve(OutputSize(mask)));
    RETURN_NOT_OK(UnpackValuesNullCount(params_.context, values, mask,
                                        static_cast<OutBuilder*>(builder.get())));
    return builder->Finish(params_.out);
  }

  Status Visit(const NullType& t) {
    auto&& mask = static_cast<const ArrayType<MaskType>&>(*params_.mask);
    params_.out->reset(new NullArray(OutputSize(mask)));
    return Status::OK();
  }

  Status Visit(const DictionaryType& t) {
    std::shared_ptr<Array> masked_indices;
    const auto& values = internal::checked_cast<const DictionaryArray&>(*params_.values);
    {
      // To take from a dictionary, apply the current kernel to the dictionary's
      // mask. (Use UnpackValues<MaskType> since MaskType is already unpacked)
      MaskParameters params = params_;
      params.values = values.indices();
      params.out = &masked_indices;
      UnpackValues<MaskType> unpack = {params};
      RETURN_NOT_OK(VisitTypeInline(*t.index_type(), &unpack));
    }
    // create output dictionary from taken mask
    *params_.out = std::make_shared<DictionaryArray>(values.type(), masked_indices,
                                                     values.dictionary());
    return Status::OK();
  }

  Status Visit(const ExtensionType& t) {
    // XXX can we just take from its storage?
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const UnionType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const ListType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const FixedSizeListType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const StructType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  const MaskParameters& params_;
};

struct UnpackMask {
  Status Visit(const BooleanType&) {
    UnpackValues<BooleanType> unpack = {params_};
    return VisitTypeInline(*params_.values->type(), &unpack);
  }

  Status Visit(const DataType& other) {
    return Status::TypeError("mask type not supported: ", other);
  }

  const MaskParameters& params_;
};

Status MaskKernel::Call(FunctionContext* ctx, const Datum& values, const Datum& mask,
                        Datum* out) {
  if (!values.is_array() || !mask.is_array()) {
    return Status::Invalid("MaskKernel expects array values and mask");
  }
  std::shared_ptr<Array> out_array;
  MaskParameters params;
  params.context = ctx;
  params.values = values.make_array();
  params.mask = mask.make_array();
  params.out = &out_array;
  UnpackMask unpack = {params};
  RETURN_NOT_OK(VisitTypeInline(*mask.type(), &unpack));
  *out = Datum(out_array);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
