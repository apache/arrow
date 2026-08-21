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
#include <type_traits>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/util/logging_internal.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;

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

Status ArrayBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity));
  capacity_ = capacity;
  return null_bitmap_builder_.Resize(capacity);
}

namespace internal {

Status ValidateAppendScalar(const ArrayBuilder& builder, const Scalar& scalar) {
  if (!scalar.type->Equals(*builder.type())) {
    return Status::Invalid("Cannot append scalar of type ", scalar.type->ToString(),
                           " to builder for type ", builder.type()->ToString());
  }
  return Status::OK();
}

Status ValidateAppendScalars(const ArrayBuilder& builder, const ScalarVector& scalars) {
  if (scalars.empty()) return Status::OK();
  const auto& ty = *builder.type();
  for (const auto& scalar : scalars) {
    if (!scalar->type->Equals(ty)) {
      return Status::Invalid("Cannot append scalar of type ", scalar->type->ToString(),
                             " to builder for type ", ty.ToString());
    }
  }
  return Status::OK();
}

}  // namespace internal

Status ArrayBuilder::AppendScalar(const Scalar& scalar, int64_t n_repeats) {
  ARROW_RETURN_NOT_OK(internal::ValidateAppendScalar(*this, scalar));
  return Status::NotImplemented("AppendScalar for builder for ", *type());
}

Status ArrayBuilder::AppendScalars(const ScalarVector& scalars) {
  ARROW_RETURN_NOT_OK(internal::ValidateAppendScalars(*this, scalars));
  for (const auto& scalar : scalars) {
    RETURN_NOT_OK(AppendScalar(*scalar, 1));
  }
  return Status::OK();
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
