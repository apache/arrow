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

#include "arrow/loader.h"

#include <cstdint>
#include <memory>
#include <sstream>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/visibility.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class Array;
struct DataType;
class Status;

class ArrayLoader {
 public:
  ArrayLoader(const std::shared_ptr<DataType>& type, ArrayLoaderContext* context)
      : type_(type), context_(context) {}

  Status Load(std::shared_ptr<Array>* out) {
    if (context_->max_recursion_depth <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    RETURN_NOT_OK(VisitTypeInline(*type_, this));

    *out = std::move(result_);
    return Status::OK();
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    return context_->source->GetBuffer(buffer_index, out);
  }

  Status LoadCommon(FieldMetadata* field_meta, std::shared_ptr<Buffer>* null_bitmap) {
    // This only contains the length and null count, which we need to figure
    // out what to do with the buffers. For example, if null_count == 0, then
    // we can skip that buffer without reading from shared memory
    RETURN_NOT_OK(
        context_->source->GetFieldMetadata(context_->field_index++, field_meta));

    // extract null_bitmap which is common to all arrays
    if (field_meta->null_count == 0) {
      *null_bitmap = nullptr;
    } else {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, null_bitmap));
    }
    context_->buffer_index++;
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadPrimitive() {
    using ArrayType = typename TypeTraits<TYPE>::ArrayType;

    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap, data;

    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));
    if (field_meta.length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &data));
    } else {
      context_->buffer_index++;
      data.reset(new Buffer(nullptr, 0));
    }
    result_ = std::make_shared<ArrayType>(type_, field_meta.length, data, null_bitmap,
        field_meta.null_count, field_meta.offset);
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadBinary() {
    using CONTAINER = typename TypeTraits<TYPE>::ArrayType;

    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap, offsets, values;

    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));
    if (field_meta.length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &offsets));
      RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &values));
    } else {
      context_->buffer_index += 2;
      offsets = values = nullptr;
    }

    result_ = std::make_shared<CONTAINER>(
        field_meta.length, offsets, values, null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status LoadChild(const Field& field, std::shared_ptr<Array>* out) {
    ArrayLoader loader(field.type, context_);
    --context_->max_recursion_depth;
    RETURN_NOT_OK(loader.Load(out));
    ++context_->max_recursion_depth;
    return Status::OK();
  }

  Status LoadChildren(std::vector<std::shared_ptr<Field>> child_fields,
      std::vector<std::shared_ptr<Array>>* arrays) {
    arrays->reserve(static_cast<int>(child_fields.size()));

    for (const auto& child_field : child_fields) {
      std::shared_ptr<Array> field_array;
      RETURN_NOT_OK(LoadChild(*child_field.get(), &field_array));
      arrays->emplace_back(field_array);
    }
    return Status::OK();
  }

  Status Visit(const NullType& type) { return Status::NotImplemented("null"); }

  Status Visit(const DecimalType& type) { return Status::NotImplemented("decimal"); }

  template <typename T>
  typename std::enable_if<std::is_base_of<FixedWidthType, T>::value &&
                              !std::is_base_of<FixedSizeBinaryType, T>::value &&
                              !std::is_base_of<DictionaryType, T>::value,
      Status>::type
  Visit(const T& type) {
    return LoadPrimitive<T>();
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<BinaryType, T>::value, Status>::type Visit(
      const T& type) {
    return LoadBinary<T>();
  }

  Status Visit(const FixedSizeBinaryType& type) {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap, data;

    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));
    RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &data));

    result_ = std::make_shared<FixedSizeBinaryArray>(
        type_, field_meta.length, data, null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap, offsets;

    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));
    if (field_meta.length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, &offsets));
    } else {
      offsets = nullptr;
    }
    ++context_->buffer_index;

    const int num_children = type.num_children();
    if (num_children != 1) {
      std::stringstream ss;
      ss << "Wrong number of children: " << num_children;
      return Status::Invalid(ss.str());
    }
    std::shared_ptr<Array> values_array;

    RETURN_NOT_OK(LoadChild(*type.child(0).get(), &values_array));

    result_ = std::make_shared<ListArray>(type_, field_meta.length, offsets, values_array,
        null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));

    std::vector<std::shared_ptr<Array>> fields;
    RETURN_NOT_OK(LoadChildren(type.children(), &fields));

    result_ = std::make_shared<StructArray>(
        type_, field_meta.length, fields, null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    FieldMetadata field_meta;
    std::shared_ptr<Buffer> null_bitmap, type_ids, offsets;

    RETURN_NOT_OK(LoadCommon(&field_meta, &null_bitmap));
    if (field_meta.length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, &type_ids));
      if (type.mode == UnionMode::DENSE) {
        RETURN_NOT_OK(GetBuffer(context_->buffer_index + 1, &offsets));
      }
    }
    context_->buffer_index += type.mode == UnionMode::DENSE ? 2 : 1;

    std::vector<std::shared_ptr<Array>> fields;
    RETURN_NOT_OK(LoadChildren(type.children(), &fields));

    result_ = std::make_shared<UnionArray>(type_, field_meta.length, fields, type_ids,
        offsets, null_bitmap, field_meta.null_count);
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    std::shared_ptr<Array> indices;
    RETURN_NOT_OK(LoadArray(type.index_type(), context_, &indices));
    result_ = std::make_shared<DictionaryArray>(type_, indices);
    return Status::OK();
  }

  std::shared_ptr<Array> result() const { return result_; }

 private:
  const std::shared_ptr<DataType> type_;
  ArrayLoaderContext* context_;

  // Used in visitor pattern
  std::shared_ptr<Array> result_;
};

Status LoadArray(const std::shared_ptr<DataType>& type, ArrayComponentSource* source,
    std::shared_ptr<Array>* out) {
  ArrayLoaderContext context;
  context.source = source;
  context.field_index = context.buffer_index = 0;
  context.max_recursion_depth = kMaxNestingDepth;
  return LoadArray(type, &context, out);
}

Status LoadArray(const std::shared_ptr<DataType>& type, ArrayLoaderContext* context,
    std::shared_ptr<Array>* out) {
  ArrayLoader loader(type, context);
  RETURN_NOT_OK(loader.Load(out));

  return Status::OK();
}

class InMemorySource : public ArrayComponentSource {
 public:
  InMemorySource(const std::vector<FieldMetadata>& fields,
      const std::vector<std::shared_ptr<Buffer>>& buffers)
      : fields_(fields), buffers_(buffers) {}

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    DCHECK(buffer_index < static_cast<int>(buffers_.size()));
    *out = buffers_[buffer_index];
    return Status::OK();
  }

  Status GetFieldMetadata(int field_index, FieldMetadata* metadata) {
    DCHECK(field_index < static_cast<int>(fields_.size()));
    *metadata = fields_[field_index];
    return Status::OK();
  }

 private:
  const std::vector<FieldMetadata>& fields_;
  const std::vector<std::shared_ptr<Buffer>>& buffers_;
};

Status LoadArray(const std::shared_ptr<DataType>& type,
    const std::vector<FieldMetadata>& fields,
    const std::vector<std::shared_ptr<Buffer>>& buffers, std::shared_ptr<Array>* out) {
  InMemorySource source(fields, buffers);
  return LoadArray(type, &source, out);
}

Status MakePrimitiveArray(const std::shared_ptr<DataType>& type, int64_t length,
    const std::shared_ptr<Buffer>& data, const std::shared_ptr<Buffer>& null_bitmap,
    int64_t null_count, int64_t offset, std::shared_ptr<Array>* out) {
  std::vector<std::shared_ptr<Buffer>> buffers = {null_bitmap, data};
  return MakePrimitiveArray(type, buffers, length, null_count, offset, out);
}

Status MakePrimitiveArray(const std::shared_ptr<DataType>& type,
    const std::vector<std::shared_ptr<Buffer>>& buffers, int64_t length,
    int64_t null_count, int64_t offset, std::shared_ptr<Array>* out) {
  std::vector<FieldMetadata> fields(1);
  fields[0].length = length;
  fields[0].null_count = null_count;
  fields[0].offset = offset;

  return LoadArray(type, fields, buffers, out);
}

}  // namespace arrow
