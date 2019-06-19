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
#include <memory>
#include <utility>
#include <vector>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/filter.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

using internal::checked_cast;
using internal::checked_pointer_cast;

template <typename Builder>
Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   std::unique_ptr<Builder>* out) {
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(pool, type, &builder));
  out->reset(checked_cast<Builder*>(builder.release()));
  return Status::OK();
}

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
static int64_t OutputSize(const BooleanArray& filter) {
  auto offset = filter.offset();
  auto length = filter.length();
  int64_t size = 0;
  for (auto i = offset; i < offset + length; ++i) {
    if (filter.IsNull(i) || filter.Value(i)) {
      ++size;
    }
  }
  return size;
}

template <typename ValueType>
class FilterImpl;

template <>
class FilterImpl<NullType> : public FilterKernel {
 public:
  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t length, std::shared_ptr<Array>* out) override {
    out->reset(new NullArray(length));
    return Status::OK();
  }
};

template <typename ValueType>
class FilterImpl : public FilterKernel {
 public:
  using ValueArray = typename TypeTraits<ValueType>::ArrayType;
  using OutBuilder = typename TypeTraits<ValueType>::BuilderType;

  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t length, std::shared_ptr<Array>* out) override {
    std::unique_ptr<OutBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), type_, &builder));
    RETURN_NOT_OK(builder->Resize(OutputSize(filter)));
    RETURN_NOT_OK(UnpackValuesNullCount(checked_cast<const ValueArray&>(values), filter,
                                        builder.get()));
    return builder->Finish(out);
  }

 private:
  Status UnpackValuesNullCount(const ValueArray& values, const BooleanArray& filter,
                               OutBuilder* builder) {
    if (values.null_count() == 0) {
      return UnpackIndicesNullCount<true>(values, filter, builder);
    }
    return UnpackIndicesNullCount<false>(values, filter, builder);
  }

  template <bool AllValuesValid>
  Status UnpackIndicesNullCount(const ValueArray& values, const BooleanArray& filter,
                                OutBuilder* builder) {
    if (filter.null_count() == 0) {
      return Filter<AllValuesValid, true>(values, filter, builder);
    }
    return Filter<AllValuesValid, false>(values, filter, builder);
  }

  template <bool AllValuesValid, bool AllIndicesValid>
  Status Filter(const ValueArray& values, const BooleanArray& filter,
                OutBuilder* builder) {
    for (int64_t i = 0; i < filter.length(); ++i) {
      if (!AllIndicesValid && filter.IsNull(i)) {
        builder->UnsafeAppendNull();
        continue;
      }
      if (!filter.Value(i)) {
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
};

template <>
class FilterImpl<StructType> : public FilterKernel {
 public:
  FilterImpl(const std::shared_ptr<DataType>& type,
             std::vector<std::unique_ptr<FilterKernel>> child_kernels)
      : FilterKernel(type), child_kernels_(std::move(child_kernels)) {}

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t length, std::shared_ptr<Array>* out) override {
    const auto& struct_array = checked_cast<const StructArray&>(values);

    TypedBufferBuilder<bool> null_bitmap_builder(ctx->memory_pool());
    RETURN_NOT_OK(null_bitmap_builder.Resize(length));

    ArrayVector fields(type_->num_children());
    for (int i = 0; i < type_->num_children(); ++i) {
      RETURN_NOT_OK(child_kernels_[i]->Filter(ctx, *struct_array.field(i), filter, length,
                                              &fields[i]));
    }

    for (int64_t i = 0; i < filter.length(); ++i) {
      if (filter.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        continue;
      }
      if (!filter.Value(i)) {
        continue;
      }
      if (struct_array.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        continue;
      }
      null_bitmap_builder.UnsafeAppend(true);
    }

    auto null_count = null_bitmap_builder.false_count();
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder.Finish(&null_bitmap));

    out->reset(new StructArray(type_, length, fields, null_bitmap, null_count));
    return Status::OK();
  }

 private:
  std::vector<std::unique_ptr<FilterKernel>> child_kernels_;
};

template <>
class FilterImpl<FixedSizeListType> : public FilterKernel {
 public:
  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t length, std::shared_ptr<Array>* out) override {
    const auto& list_array = checked_cast<const FixedSizeListArray&>(values);

    TypedBufferBuilder<bool> null_bitmap_builder(ctx->memory_pool());
    RETURN_NOT_OK(null_bitmap_builder.Resize(length));

    BooleanBuilder value_filter_builder(ctx->memory_pool());
    auto list_size = list_array.list_type()->list_size();
    RETURN_NOT_OK(value_filter_builder.Resize(list_size * length));

    for (int64_t i = 0; i < filter.length(); ++i) {
      if (filter.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        for (int64_t j = 0; j < list_size; ++j) {
          value_filter_builder.UnsafeAppendNull();
        }
        continue;
      }
      if (!filter.Value(i)) {
        for (int64_t j = 0; j < list_size; ++j) {
          value_filter_builder.UnsafeAppend(false);
        }
        continue;
      }
      if (values.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        for (int64_t j = 0; j < list_size; ++j) {
          value_filter_builder.UnsafeAppendNull();
        }
        continue;
      }
      for (int64_t j = 0; j < list_size; ++j) {
        value_filter_builder.UnsafeAppend(true);
      }
      null_bitmap_builder.UnsafeAppend(true);
    }

    std::shared_ptr<BooleanArray> value_filter;
    RETURN_NOT_OK(value_filter_builder.Finish(&value_filter));
    std::shared_ptr<Array> out_values;
    RETURN_NOT_OK(
        arrow::compute::Filter(ctx, *list_array.values(), *value_filter, &out_values));

    auto null_count = null_bitmap_builder.false_count();
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder.Finish(&null_bitmap));

    out->reset(
        new FixedSizeListArray(type_, length, out_values, null_bitmap, null_count));
    return Status::OK();
  }
};

template <>
class FilterImpl<ListType> : public FilterKernel {
 public:
  using FilterKernel::FilterKernel;

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t length, std::shared_ptr<Array>* out) override {
    const auto& list_array = checked_cast<const ListArray&>(values);

    TypedBufferBuilder<bool> null_bitmap_builder(ctx->memory_pool());
    RETURN_NOT_OK(null_bitmap_builder.Resize(length));

    BooleanBuilder value_filter_builder(ctx->memory_pool());

    TypedBufferBuilder<int32_t> offset_builder(ctx->memory_pool());
    RETURN_NOT_OK(offset_builder.Resize(length + 1));
    int32_t offset = 0;
    offset_builder.UnsafeAppend(offset);

    for (int64_t i = 0; i < filter.length(); ++i) {
      if (filter.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        offset_builder.UnsafeAppend(offset);
        RETURN_NOT_OK(
            value_filter_builder.AppendValues(list_array.value_length(i), false));
        continue;
      }
      if (!filter.Value(i)) {
        RETURN_NOT_OK(
            value_filter_builder.AppendValues(list_array.value_length(i), false));
        continue;
      }
      if (values.IsNull(i)) {
        null_bitmap_builder.UnsafeAppend(false);
        offset_builder.UnsafeAppend(offset);
        RETURN_NOT_OK(
            value_filter_builder.AppendValues(list_array.value_length(i), false));
        continue;
      }
      null_bitmap_builder.UnsafeAppend(true);
      offset += list_array.value_length(i);
      offset_builder.UnsafeAppend(offset);
      RETURN_NOT_OK(value_filter_builder.AppendValues(list_array.value_length(i), true));
    }

    std::shared_ptr<BooleanArray> value_filter;
    RETURN_NOT_OK(value_filter_builder.Finish(&value_filter));
    std::shared_ptr<Array> out_values;
    RETURN_NOT_OK(
        arrow::compute::Filter(ctx, *list_array.values(), *value_filter, &out_values));

    auto null_count = null_bitmap_builder.false_count();
    std::shared_ptr<Buffer> offsets, null_bitmap;
    RETURN_NOT_OK(offset_builder.Finish(&offsets));
    RETURN_NOT_OK(null_bitmap_builder.Finish(&null_bitmap));

    *out = MakeArray(ArrayData::Make(type_, length, {null_bitmap, offsets},
                                     {out_values->data()}, null_count));
    return Status::OK();
  }
};

template <>
class FilterImpl<MapType> : public FilterImpl<ListType> {
  using FilterImpl<ListType>::FilterImpl;
};

template <>
class FilterImpl<DictionaryType> : public FilterKernel {
 public:
  FilterImpl(const std::shared_ptr<DataType>& type, std::unique_ptr<FilterKernel> impl)
      : FilterKernel(type), impl_(std::move(impl)) {}

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t length, std::shared_ptr<Array>* out) override {
    auto dict_array = checked_cast<const DictionaryArray*>(&values);
    // To filter a dictionary, apply the current kernel to the dictionary's indices.
    std::shared_ptr<Array> taken_indices;
    RETURN_NOT_OK(
        impl_->Filter(ctx, *dict_array->indices(), filter, length, &taken_indices));
    return DictionaryArray::FromArrays(values.type(), taken_indices,
                                       dict_array->dictionary(), out);
  }

 private:
  std::unique_ptr<FilterKernel> impl_;
};

template <>
class FilterImpl<ExtensionType> : public FilterKernel {
 public:
  FilterImpl(const std::shared_ptr<DataType>& type, std::unique_ptr<FilterKernel> impl)
      : FilterKernel(type), impl_(std::move(impl)) {}

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t length, std::shared_ptr<Array>* out) override {
    auto ext_array = checked_cast<const ExtensionArray*>(&values);
    // To take from an extension array, apply the current kernel to storage.
    std::shared_ptr<Array> taken_storage;
    RETURN_NOT_OK(
        impl_->Filter(ctx, *ext_array->storage(), filter, length, &taken_storage));
    *out = ext_array->extension_type()->MakeArray(taken_storage->data());
    return Status::OK();
  }

 private:
  std::unique_ptr<FilterKernel> impl_;
};

Status FilterKernel::Make(const std::shared_ptr<DataType>& value_type,
                          std::unique_ptr<FilterKernel>* out) {
  switch (value_type->id()) {
#define NO_CHILD_CASE(T)                                           \
  case T##Type::type_id:                                           \
    *out = internal::make_unique<FilterImpl<T##Type>>(value_type); \
    return Status::OK()

#define SINGLE_CHILD_CASE(T, CHILD_TYPE)                                                \
  case T##Type::type_id: {                                                              \
    auto t = checked_pointer_cast<T##Type>(value_type);                                 \
    std::unique_ptr<FilterKernel> child_filter_impl;                                    \
    RETURN_NOT_OK(FilterKernel::Make(t->CHILD_TYPE(), &child_filter_impl));             \
    *out = internal::make_unique<FilterImpl<T##Type>>(t, std::move(child_filter_impl)); \
    return Status::OK();                                                                \
  }

    NO_CHILD_CASE(Null);
    NO_CHILD_CASE(Boolean);
    NO_CHILD_CASE(Int8);
    NO_CHILD_CASE(Int16);
    NO_CHILD_CASE(Int32);
    NO_CHILD_CASE(Int64);
    NO_CHILD_CASE(UInt8);
    NO_CHILD_CASE(UInt16);
    NO_CHILD_CASE(UInt32);
    NO_CHILD_CASE(UInt64);
    NO_CHILD_CASE(Date32);
    NO_CHILD_CASE(Date64);
    NO_CHILD_CASE(Time32);
    NO_CHILD_CASE(Time64);
    NO_CHILD_CASE(Timestamp);
    NO_CHILD_CASE(Duration);
    NO_CHILD_CASE(HalfFloat);
    NO_CHILD_CASE(Float);
    NO_CHILD_CASE(Double);
    NO_CHILD_CASE(String);
    NO_CHILD_CASE(Binary);
    NO_CHILD_CASE(FixedSizeBinary);
    NO_CHILD_CASE(Decimal128);

    SINGLE_CHILD_CASE(Dictionary, index_type);
    SINGLE_CHILD_CASE(Extension, storage_type);

    NO_CHILD_CASE(List);
    NO_CHILD_CASE(FixedSizeList);
    NO_CHILD_CASE(Map);

    case Type::STRUCT: {
      std::vector<std::unique_ptr<FilterKernel>> child_kernels;
      for (auto child : value_type->children()) {
        child_kernels.emplace_back();
        RETURN_NOT_OK(FilterKernel::Make(child->type(), &child_kernels.back()));
      }
      *out = internal::make_unique<FilterImpl<StructType>>(value_type,
                                                           std::move(child_kernels));
      return Status::OK();
    }

#undef NO_CHILD_CASE
#undef SINGLE_CHILD_CASE

    default:
      return Status::NotImplemented("gathering values of type ", *value_type);
  }
}

Status FilterKernel::Call(FunctionContext* ctx, const Datum& values, const Datum& filter,
                          Datum* out) {
  if (!values.is_array() || !filter.is_array()) {
    return Status::Invalid("FilterKernel expects array values and filter");
  }
  auto values_array = values.make_array();
  auto filter_array = checked_pointer_cast<BooleanArray>(filter.make_array());
  const auto length = OutputSize(*filter_array);
  std::shared_ptr<Array> out_array;
  RETURN_NOT_OK(this->Filter(ctx, *values_array, *filter_array, length, &out_array));
  *out = out_array;
  return Status::OK();
}

Status Filter(FunctionContext* context, const Array& values, const Array& filter,
              std::shared_ptr<Array>* out) {
  Datum out_datum;
  RETURN_NOT_OK(Filter(context, Datum(values.data()), Datum(filter.data()), &out_datum));
  *out = out_datum.make_array();
  return Status::OK();
}

Status Filter(FunctionContext* context, const Datum& values, const Datum& filter,
              Datum* out) {
  std::unique_ptr<FilterKernel> kernel;
  RETURN_NOT_OK(FilterKernel::Make(values.type(), &kernel));
  return kernel->Call(context, values, filter, out);
}

}  // namespace compute
}  // namespace arrow
