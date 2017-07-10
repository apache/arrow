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
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/visibility.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class ArrayLoader {
 public:
  ArrayLoader(const std::shared_ptr<DataType>& type, internal::ArrayData* out,
      ArrayLoaderContext* context)
      : type_(type), context_(context), out_(out) {}

  Status Load() {
    if (context_->max_recursion_depth <= 0) {
      return Status::Invalid("Max recursion depth reached");
    }

    out_->type = type_;

    RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return Status::OK();
  }

  Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) {
    return context_->source->GetBuffer(buffer_index, out);
  }

  Status LoadCommon() {
    // This only contains the length and null count, which we need to figure
    // out what to do with the buffers. For example, if null_count == 0, then
    // we can skip that buffer without reading from shared memory
    FieldMetadata field_meta;
    RETURN_NOT_OK(
        context_->source->GetFieldMetadata(context_->field_index++, &field_meta));

    out_->length = field_meta.length;
    out_->null_count = field_meta.null_count;
    out_->offset = field_meta.offset;

    // extract null_bitmap which is common to all arrays
    if (field_meta.null_count == 0) {
      out_->buffers[0] = nullptr;
    } else {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, &out_->buffers[0]));
    }
    context_->buffer_index++;
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadPrimitive() {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon());
    if (out_->length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &out_->buffers[1]));
    } else {
      context_->buffer_index++;
      out_->buffers[1].reset(new Buffer(nullptr, 0));
    }
    return Status::OK();
  }

  template <typename TYPE>
  Status LoadBinary() {
    out_->buffers.resize(3);

    RETURN_NOT_OK(LoadCommon());
    RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &out_->buffers[1]));
    return GetBuffer(context_->buffer_index++, &out_->buffers[2]);
  }

  Status LoadChild(const Field& field, internal::ArrayData* out) {
    ArrayLoader loader(field.type(), out, context_);
    --context_->max_recursion_depth;
    RETURN_NOT_OK(loader.Load());
    ++context_->max_recursion_depth;
    return Status::OK();
  }

  Status LoadChildren(std::vector<std::shared_ptr<Field>> child_fields) {
    out_->child_data.reserve(static_cast<int>(child_fields.size()));

    for (const auto& child_field : child_fields) {
      auto field_array = std::make_shared<internal::ArrayData>();
      RETURN_NOT_OK(LoadChild(*child_field.get(), field_array.get()));
      out_->child_data.emplace_back(field_array);
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
    out_->buffers.resize(2);
    RETURN_NOT_OK(LoadCommon());
    return GetBuffer(context_->buffer_index++, &out_->buffers[1]);
  }

  Status Visit(const ListType& type) {
    out_->buffers.resize(2);

    RETURN_NOT_OK(LoadCommon());
    RETURN_NOT_OK(GetBuffer(context_->buffer_index++, &out_->buffers[1]));

    const int num_children = type.num_children();
    if (num_children != 1) {
      std::stringstream ss;
      ss << "Wrong number of children: " << num_children;
      return Status::Invalid(ss.str());
    }

    return LoadChildren(type.children());
  }

  Status Visit(const StructType& type) {
    out_->buffers.resize(1);
    RETURN_NOT_OK(LoadCommon());
    return LoadChildren(type.children());
  }

  Status Visit(const UnionType& type) {
    out_->buffers.resize(3);

    RETURN_NOT_OK(LoadCommon());
    if (out_->length > 0) {
      RETURN_NOT_OK(GetBuffer(context_->buffer_index, &out_->buffers[1]));
      if (type.mode() == UnionMode::DENSE) {
        RETURN_NOT_OK(GetBuffer(context_->buffer_index + 1, &out_->buffers[2]));
      }
    }
    context_->buffer_index += type.mode() == UnionMode::DENSE ? 2 : 1;
    return LoadChildren(type.children());
  }

  Status Visit(const DictionaryType& type) {
    RETURN_NOT_OK(LoadArray(type.index_type(), context_, out_));
    out_->type = type_;
    return Status::OK();
  }

 private:
  const std::shared_ptr<DataType>& type_;
  ArrayLoaderContext* context_;

  // Used in visitor pattern
  internal::ArrayData* out_;
};

Status LoadArray(const std::shared_ptr<DataType>& type, ArrayComponentSource* source,
    internal::ArrayData* out) {
  ArrayLoaderContext context;
  context.source = source;
  context.field_index = context.buffer_index = 0;
  context.max_recursion_depth = kMaxNestingDepth;
  return LoadArray(type, &context, out);
}

Status LoadArray(const std::shared_ptr<DataType>& type, ArrayLoaderContext* context,
    internal::ArrayData* out) {
  ArrayLoader loader(type, out, context);
  return loader.Load();
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
    const std::vector<std::shared_ptr<Buffer>>& buffers, internal::ArrayData* out) {
  InMemorySource source(fields, buffers);
  return LoadArray(type, &source, out);
}

}  // namespace arrow
