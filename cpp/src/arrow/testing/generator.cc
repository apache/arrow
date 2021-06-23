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

#include "arrow/testing/generator.h"

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"

namespace arrow {

template <typename ArrowType, typename CType = typename TypeTraits<ArrowType>::CType,
          typename BuilderType = typename TypeTraits<ArrowType>::BuilderType>
static inline std::shared_ptr<Array> ConstantArray(int64_t size, CType value) {
  auto type = TypeTraits<ArrowType>::type_singleton();
  auto builder_fn = [&](BuilderType* builder) { builder->UnsafeAppend(value); };
  return ArrayFromBuilderVisitor(type, size, builder_fn).ValueOrDie();
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Boolean(int64_t size, bool value) {
  return ConstantArray<BooleanType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt8(int64_t size, uint8_t value) {
  return ConstantArray<UInt8Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int8(int64_t size, int8_t value) {
  return ConstantArray<Int8Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt16(int64_t size,
                                                             uint16_t value) {
  return ConstantArray<UInt16Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int16(int64_t size, int16_t value) {
  return ConstantArray<Int16Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt32(int64_t size,
                                                             uint32_t value) {
  return ConstantArray<UInt32Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int32(int64_t size, int32_t value) {
  return ConstantArray<Int32Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::UInt64(int64_t size,
                                                             uint64_t value) {
  return ConstantArray<UInt64Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Int64(int64_t size, int64_t value) {
  return ConstantArray<Int64Type>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Float32(int64_t size, float value) {
  return ConstantArray<FloatType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::Float64(int64_t size,
                                                              double value) {
  return ConstantArray<DoubleType>(size, value);
}

std::shared_ptr<arrow::Array> ConstantArrayGenerator::String(int64_t size,
                                                             std::string value) {
  return ConstantArray<StringType>(size, value);
}

struct ScalarVectorToArrayImpl {
  template <typename T, typename AppendScalar,
            typename BuilderType = typename TypeTraits<T>::BuilderType,
            typename ScalarType = typename TypeTraits<T>::ScalarType>
  Status UseBuilder(const AppendScalar& append) {
    BuilderType builder(type_, default_memory_pool());
    for (const auto& s : scalars_) {
      if (s->is_valid) {
        RETURN_NOT_OK(append(internal::checked_cast<const ScalarType&>(*s), &builder));
      } else {
        RETURN_NOT_OK(builder.AppendNull());
      }
    }
    return builder.FinishInternal(&data_);
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

  template <typename T>
  enable_if_primitive_ctype<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendValue{});
  }

  template <typename T>
  enable_if_has_string_view<T, Status> Visit(const T&) {
    return UseBuilder<T>(AppendBuffer{});
  }

  Status Visit(const StructType& type) {
    data_ = ArrayData::Make(type_, static_cast<int64_t>(scalars_.size()),
                            {/*null_bitmap=*/nullptr});
    data_->child_data.resize(type_->num_fields());

    ScalarVector field_scalars(scalars_.size());

    for (int field_index = 0; field_index < type.num_fields(); ++field_index) {
      for (size_t i = 0; i < scalars_.size(); ++i) {
        field_scalars[i] =
            internal::checked_cast<StructScalar*>(scalars_[i].get())->value[field_index];
      }

      ARROW_ASSIGN_OR_RAISE(data_->child_data[field_index],
                            ScalarVectorToArrayImpl{}.Convert(field_scalars));
    }
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("ScalarVectorToArray for type ", type);
  }

  Result<std::shared_ptr<ArrayData>> Convert(const ScalarVector& scalars) && {
    if (scalars.size() == 0) {
      return Status::NotImplemented("ScalarVectorToArray with no scalars");
    }
    scalars_ = std::move(scalars);
    type_ = scalars_[0]->type;
    RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return std::move(data_);
  }

  std::shared_ptr<DataType> type_;
  ScalarVector scalars_;
  std::shared_ptr<ArrayData> data_;
};

Result<std::shared_ptr<Array>> ScalarVectorToArray(const ScalarVector& scalars) {
  ARROW_ASSIGN_OR_RAISE(auto data, ScalarVectorToArrayImpl{}.Convert(scalars));
  return MakeArray(std::move(data));
}

}  // namespace arrow
