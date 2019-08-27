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
static inline std::shared_ptr<Array> ConstantArray(int64_t size, CType value = 0) {
  auto type = TypeTraits<ArrowType>::type_singleton();
  auto builder_fn = [](BuilderType* builder) { builder->UnsafeAppend(CType(0)); };
  ASSERT_OK_AND_ASSIGN(auto array, ArrayFromBuilderVisitor(type, size, builder_fn));
  return array;
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

}  // namespace arrow
