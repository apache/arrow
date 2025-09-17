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

#pragma once

#include "arrow/sparse_tensor.h"  // IWYU pragma: export

#include <memory>
#include <utility>

#include "arrow/visit_type_inline.h"

namespace arrow {
namespace internal {

struct SparseTensorConverterMixin {
  static bool IsNonZero(const uint8_t val) { return val != 0; }

  static void AssignIndex(uint8_t* indices, int64_t val, const int elsize);

  static int64_t GetIndexValue(const uint8_t* value_ptr, const int elsize);
};

Status MakeSparseCOOTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data);

Status MakeSparseCSXMatrixFromTensor(SparseMatrixCompressedAxis axis,
                                     const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data);

Status MakeSparseCSFTensorFromTensor(const Tensor& tensor,
                                     const std::shared_ptr<DataType>& index_value_type,
                                     MemoryPool* pool,
                                     std::shared_ptr<SparseIndex>* out_sparse_index,
                                     std::shared_ptr<Buffer>* out_data);

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCOOTensor(
    MemoryPool* pool, const SparseCOOTensor* sparse_tensor);

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCSRMatrix(
    MemoryPool* pool, const SparseCSRMatrix* sparse_tensor);

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCSCMatrix(
    MemoryPool* pool, const SparseCSCMatrix* sparse_tensor);

Result<std::shared_ptr<Tensor>> MakeTensorFromSparseCSFTensor(
    MemoryPool* pool, const SparseCSFTensor* sparse_tensor);

template <typename Convertor>
struct ConverterVisitor {
  explicit ConverterVisitor(Convertor& converter) : converter(converter) {}
  template <typename ValueType, typename IndexType>
  Status operator()(const ValueType& value, const IndexType& index_type) {
    return converter.Convert(value, index_type);
  }

  Convertor& converter;
};

struct ValueTypeVisitor {
  template <typename ValueType, typename IndexType, typename Function>
  enable_if_number<ValueType, Status> Visit(const ValueType& value_type,
                                            const IndexType& index_type,
                                            Function&& function) {
    return function(value_type, index_type);
  }

  template <typename IndexType, typename Function>
  Status Visit(const DataType& value_type, const IndexType&, Function&&) {
    return Status::Invalid("Invalid value type and the type is ", value_type.name());
  }
};

struct IndexAndValueTypeVisitor {
  template <typename IndexType, typename Function>
  enable_if_integer<IndexType, Status> Visit(const IndexType& index_type,
                                             const std::shared_ptr<DataType>& value_type,
                                             Function&& function) {
    ValueTypeVisitor visitor;
    return VisitTypeInline(*value_type, &visitor, index_type,
                           std::forward<Function>(function));
  }

  template <typename Function>
  Status Visit(const DataType& type, const std::shared_ptr<DataType>&, Function&&) {
    return Status::Invalid("Invalid index type and the type is ", type.name());
  }
};

template <typename Function>
Status VisitValueAndIndexType(const std::shared_ptr<DataType>& value_type,
                              const std::shared_ptr<DataType>& index_type,
                              Function&& function) {
  IndexAndValueTypeVisitor visitor;
  return VisitTypeInline(*index_type, &visitor, value_type,
                         std::forward<Function>(function));
}

}  // namespace internal
}  // namespace arrow
