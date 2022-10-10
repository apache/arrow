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
#include "arrow/util/logging.h"
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

namespace {

struct AppendScalarImpl {
  template <typename T>
  enable_if_t<has_c_type<T>::value || is_decimal_type<T>::value ||
                  is_fixed_size_binary_type<T>::value,
              Status>
  Visit(const T&) {
    auto builder = checked_cast<typename TypeTraits<T>::BuilderType*>(builder_);
    RETURN_NOT_OK(builder->Reserve(n_repeats_ * (scalars_end_ - scalars_begin_)));

    for (int64_t i = 0; i < n_repeats_; i++) {
      for (const std::shared_ptr<Scalar>* raw = scalars_begin_; raw != scalars_end_;
           raw++) {
        auto scalar = checked_cast<const typename TypeTraits<T>::ScalarType*>(raw->get());
        if (scalar->is_valid) {
          builder->UnsafeAppend(scalar->value);
        } else {
          builder->UnsafeAppendNull();
        }
      }
    }
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    int64_t data_size = 0;
    for (const std::shared_ptr<Scalar>* raw = scalars_begin_; raw != scalars_end_;
         raw++) {
      auto scalar = checked_cast<const typename TypeTraits<T>::ScalarType*>(raw->get());
      if (scalar->is_valid) {
        data_size += scalar->value->size();
      }
    }

    auto builder = checked_cast<typename TypeTraits<T>::BuilderType*>(builder_);
    RETURN_NOT_OK(builder->Reserve(n_repeats_ * (scalars_end_ - scalars_begin_)));
    RETURN_NOT_OK(builder->ReserveData(n_repeats_ * data_size));

    for (int64_t i = 0; i < n_repeats_; i++) {
      for (const std::shared_ptr<Scalar>* raw = scalars_begin_; raw != scalars_end_;
           raw++) {
        auto scalar = checked_cast<const typename TypeTraits<T>::ScalarType*>(raw->get());
        if (scalar->is_valid) {
          builder->UnsafeAppend(std::string_view{*scalar->value});
        } else {
          builder->UnsafeAppendNull();
        }
      }
    }
    return Status::OK();
  }

  template <typename T>
  enable_if_list_like<T, Status> Visit(const T&) {
    auto builder = checked_cast<typename TypeTraits<T>::BuilderType*>(builder_);
    int64_t num_children = 0;
    for (const std::shared_ptr<Scalar>* scalar = scalars_begin_; scalar != scalars_end_;
         scalar++) {
      if (!(*scalar)->is_valid) continue;
      num_children += checked_cast<const BaseListScalar&>(**scalar).value->length();
    }
    RETURN_NOT_OK(builder->value_builder()->Reserve(num_children * n_repeats_));

    for (int64_t i = 0; i < n_repeats_; i++) {
      for (const std::shared_ptr<Scalar>* scalar = scalars_begin_; scalar != scalars_end_;
           scalar++) {
        if ((*scalar)->is_valid) {
          RETURN_NOT_OK(builder->Append());
          const Array& list = *checked_cast<const BaseListScalar&>(**scalar).value;
          for (int64_t i = 0; i < list.length(); i++) {
            ARROW_ASSIGN_OR_RAISE(auto scalar, list.GetScalar(i));
            RETURN_NOT_OK(builder->value_builder()->AppendScalar(*scalar));
          }
        } else {
          RETURN_NOT_OK(builder_->AppendNull());
        }
      }
    }
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    auto* builder = checked_cast<StructBuilder*>(builder_);
    auto count = n_repeats_ * (scalars_end_ - scalars_begin_);
    RETURN_NOT_OK(builder->Reserve(count));
    for (int field_index = 0; field_index < type.num_fields(); ++field_index) {
      RETURN_NOT_OK(builder->field_builder(field_index)->Reserve(count));
    }
    for (int64_t i = 0; i < n_repeats_; i++) {
      for (const std::shared_ptr<Scalar>* s = scalars_begin_; s != scalars_end_; s++) {
        const auto& scalar = checked_cast<const StructScalar&>(**s);
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
    }
    return Status::OK();
  }

  Status Visit(const SparseUnionType& type) { return MakeUnionArray(type); }

  Status Visit(const DenseUnionType& type) { return MakeUnionArray(type); }

  Status AppendUnionScalar(const DenseUnionType& type, const Scalar& s,
                           DenseUnionBuilder* builder) {
    const auto& scalar = checked_cast<const DenseUnionScalar&>(s);
    const auto scalar_field_index = type.child_ids()[scalar.type_code];
    RETURN_NOT_OK(builder->Append(scalar.type_code));

    for (int field_index = 0; field_index < type.num_fields(); ++field_index) {
      auto* child_builder = builder->child_builder(field_index).get();
      if (field_index == scalar_field_index) {
        if (scalar.is_valid) {
          RETURN_NOT_OK(child_builder->AppendScalar(*scalar.value));
        } else {
          RETURN_NOT_OK(child_builder->AppendNull());
        }
      }
    }
    return Status::OK();
  }

  Status AppendUnionScalar(const SparseUnionType& type, const Scalar& s,
                           SparseUnionBuilder* builder) {
    // For each scalar,
    //  1. append the type code,
    //  2. append the value to the corresponding child,
    //  3. append null to the other children.
    const auto& scalar = checked_cast<const SparseUnionScalar&>(s);
    RETURN_NOT_OK(builder->Append(scalar.type_code));

    for (int field_index = 0; field_index < type.num_fields(); ++field_index) {
      auto* child_builder = builder->child_builder(field_index).get();
      if (field_index == scalar.child_id) {
        if (scalar.is_valid) {
          RETURN_NOT_OK(child_builder->AppendScalar(*scalar.value[field_index]));
        } else {
          RETURN_NOT_OK(child_builder->AppendNull());
        }
      } else {
        RETURN_NOT_OK(child_builder->AppendNull());
      }
    }
    return Status::OK();
  }

  template <typename T>
  Status MakeUnionArray(const T& type) {
    using BuilderType = typename TypeTraits<T>::BuilderType;

    auto* builder = checked_cast<BuilderType*>(builder_);
    const auto count = n_repeats_ * (scalars_end_ - scalars_begin_);

    RETURN_NOT_OK(builder->Reserve(count));

    DCHECK_EQ(type.num_fields(), builder->num_children());
    for (int field_index = 0; field_index < type.num_fields(); ++field_index) {
      RETURN_NOT_OK(builder->child_builder(field_index)->Reserve(count));
    }

    for (int64_t i = 0; i < n_repeats_; i++) {
      for (const std::shared_ptr<Scalar>* s = scalars_begin_; s != scalars_end_; s++) {
        RETURN_NOT_OK(AppendUnionScalar(type, **s, builder));
      }
    }
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("AppendScalar for type ", type);
  }

  Status Convert() { return VisitScalarTypeInline(*(*scalars_begin_)->type, this); }

  const std::shared_ptr<Scalar>* scalars_begin_;
  const std::shared_ptr<Scalar>* scalars_end_;
  int64_t n_repeats_;
  ArrayBuilder* builder_;
};

}  // namespace

Status ArrayBuilder::AppendScalar(const Scalar& scalar, int64_t n_repeats) {
  if (!scalar.type->Equals(type())) {
    return Status::Invalid("Cannot append scalar of type ", scalar.type->ToString(),
                           " to builder for type ", type()->ToString());
  }
  std::shared_ptr<Scalar> shared{const_cast<Scalar*>(&scalar), [](Scalar*) {}};
  return AppendScalarImpl{&shared, &shared + 1, n_repeats, this}.Convert();
}

Status ArrayBuilder::AppendScalars(const ScalarVector& scalars) {
  if (scalars.empty()) return Status::OK();
  const auto ty = type();
  for (const auto& scalar : scalars) {
    if (!scalar->type->Equals(ty)) {
      return Status::Invalid("Cannot append scalar of type ", scalar->type->ToString(),
                             " to builder for type ", type()->ToString());
    }
  }
  return AppendScalarImpl{scalars.data(), scalars.data() + scalars.size(),
                          /*n_repeats=*/1, this}
      .Convert();
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
