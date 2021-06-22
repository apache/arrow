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

#include "arrow/array/builder_base.h"

#include <cstdint>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

Status ArrayBuilder::CheckArrayType(const std::shared_ptr<DataType>& expected_type,
                                    const Array& array, const char* message) {
  if (!expected_type->Equals(*array.type())) {
    return Status::TypeError(message);
  }
  return Status::OK();
}

Status ArrayBuilder::CheckArrayType(Type::type expected_type, const Array& array,
                                    const char* message) {
  if (array.type_id() != expected_type) {
    return Status::TypeError(message);
  }
  return Status::OK();
}

Status ArrayBuilder::TrimBuffer(const int64_t bytes_filled, ResizableBuffer* buffer) {
  if (buffer) {
    if (bytes_filled < buffer->size()) {
      // Trim buffer
      RETURN_NOT_OK(buffer->Resize(bytes_filled));
    }
    // zero the padding
    buffer->ZeroPadding();
  } else {
    // Null buffers are allowed in place of 0-byte buffers
    DCHECK_EQ(bytes_filled, 0);
  }
  return Status::OK();
}

Status ArrayBuilder::AppendToBitmap(bool is_valid) {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

Status ArrayBuilder::AppendToBitmap(const uint8_t* valid_bytes, int64_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

Status ArrayBuilder::AppendToBitmap(int64_t num_bits, bool value) {
  RETURN_NOT_OK(Reserve(num_bits));
  UnsafeAppendToBitmap(num_bits, value);
  return Status::OK();
}

Status ArrayBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity));
  capacity_ = capacity;
  return null_bitmap_builder_.Resize(capacity);
}

Status ArrayBuilder::Advance(int64_t elements) {
  if (length_ + elements > capacity_) {
    return Status::Invalid("Builder must be expanded");
  }
  length_ += elements;
  return null_bitmap_builder_.Advance(elements);
}

struct AppendScalarImpl {
  template <typename T, typename AppendScalar,
            typename BuilderType = typename TypeTraits<T>::BuilderType,
            typename ScalarType = typename TypeTraits<T>::ScalarType>
  Status UseBuilder(const AppendScalar& append) {
    for (const auto scalar : scalars_) {
      if (scalar->is_valid) {
        RETURN_NOT_OK(append(internal::checked_cast<const ScalarType&>(*scalar),
                             static_cast<BuilderType*>(builder_)));
      } else {
        RETURN_NOT_OK(builder_->AppendNull());
      }
    }
    return Status::OK();
  }

  struct AppendValue {
    template <typename BuilderType, typename ScalarType>
    Status operator()(const ScalarType& s, BuilderType* builder) const {
      return builder->Append(s.value);
    }
  };

  struct AppendBuffer {
    template <typename BuilderType, typename ScalarType>
    Status operator()(const ScalarType& s, BuilderType* builder) const {
      const Buffer& buffer = *s.value;
      return builder->Append(util::string_view{buffer});
    }
  };

  struct AppendList {
    template <typename BuilderType, typename ScalarType>
    Status operator()(const ScalarType& s, BuilderType* builder) const {
      RETURN_NOT_OK(builder->Append());
      const Array& list = *s.value;
      for (int64_t i = 0; i < list.length(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto scalar, list.GetScalar(i));
        RETURN_NOT_OK(builder->value_builder()->AppendScalar(*scalar));
      }
      return Status::OK();
    }
  };

  template <typename T>
  enable_if_has_c_type<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendValue{});
  }

  template <typename T>
  enable_if_has_string_view<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendBuffer{});
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendValue{});
  }

  template <typename T>
  enable_if_list_like<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendList{});
  }

  Status Visit(const StructType& type) {
    auto* builder = static_cast<StructBuilder*>(builder_);
    for (const auto s : scalars_) {
      const auto& scalar = internal::checked_cast<const StructScalar&>(*s);
      for (int field_index = 0; field_index < type.num_fields(); ++field_index) {
        if (!scalar.is_valid || !scalar.value[field_index]) {
          RETURN_NOT_OK(builder->field_builder(field_index)->AppendNull());
        } else {
          RETURN_NOT_OK(builder->field_builder(field_index)
                            ->AppendScalar(*scalar.value[field_index]));
        }
      }
      RETURN_NOT_OK(builder->Append(scalar.is_valid));
    }
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("AppendScalar for type ", type);
  }

  Status Convert() { return VisitTypeInline(*scalars_[0]->type, this); }

  std::vector<const Scalar*> scalars_;
  ArrayBuilder* builder_;
};

Status ArrayBuilder::AppendScalar(const Scalar& scalar) {
  if (!scalar.type->Equals(type())) {
    return Status::Invalid("Cannot append scalar of type ", scalar.type->ToString(),
                           " to builder for type ", type()->ToString());
  }
  return AppendScalarImpl{{&scalar}, this}.Convert();
}

Status ArrayBuilder::AppendScalars(const ScalarVector& scalars) {
  if (scalars.empty()) return Status::OK();
  std::vector<const Scalar*> refs;
  refs.reserve(scalars.size());
  for (const auto& scalar : scalars) {
    if (!scalar->type->Equals(type())) {
      return Status::Invalid("Cannot append scalar of type ", scalar->type->ToString(),
                             " to builder for type ", type()->ToString());
    }
    refs.push_back(scalar.get());
  }
  return AppendScalarImpl{refs, this}.Convert();
}

Status ArrayBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<ArrayData> internal_data;
  RETURN_NOT_OK(FinishInternal(&internal_data));
  *out = MakeArray(internal_data);
  return Status::OK();
}

Result<std::shared_ptr<Array>> ArrayBuilder::Finish() {
  std::shared_ptr<Array> out;
  RETURN_NOT_OK(Finish(&out));
  return out;
}

void ArrayBuilder::Reset() {
  capacity_ = length_ = null_count_ = 0;
  null_bitmap_builder_.Reset();
}

Status ArrayBuilder::SetNotNull(int64_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeSetNotNull(length);
  return Status::OK();
}

void ArrayBuilder::UnsafeAppendToBitmap(const std::vector<bool>& is_valid) {
  for (bool element_valid : is_valid) {
    UnsafeAppendToBitmap(element_valid);
  }
}

void ArrayBuilder::UnsafeSetNotNull(int64_t length) {
  length_ += length;
  null_bitmap_builder_.UnsafeAppend(length, true);
}

void ArrayBuilder::UnsafeSetNull(int64_t length) {
  length_ += length;
  null_count_ += length;
  null_bitmap_builder_.UnsafeAppend(length, false);
}

}  // namespace arrow
