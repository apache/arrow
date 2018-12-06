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

#ifndef ARROW_SPARSE_TENSOR_H
#define ARROW_SPARSE_TENSOR_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/tensor.h"

namespace arrow {

// ----------------------------------------------------------------------
// SparseIndex class

class ARROW_EXPORT SparseIndex {
 public:
  enum format_type {
    COO,
    CSR
  };

  explicit SparseIndex(format_type format_type_id, int64_t length)
      : format_type_id_(format_type_id), length_(length) {}

  virtual ~SparseIndex() = default;

  format_type format_type_id() const { return format_type_id_; }
  int64_t length() const { return length_; }

  virtual std::string ToString() const = 0;

 protected:
  format_type format_type_id_;
  int64_t length_;
};

template <typename SparseIndexType>
class SparseIndexBase : public SparseIndex {
 public:
  explicit SparseIndexBase(int64_t length)
      : SparseIndex(SparseIndexType::format_type_id, length) {}
};

// ----------------------------------------------------------------------
// SparseCOOIndex class

class ARROW_EXPORT SparseCOOIndex : public SparseIndexBase<SparseCOOIndex> {
 public:
  using CoordsTensor = NumericTensor<Int64Type>;

  static constexpr SparseIndex::format_type format_type_id = SparseIndex::COO;

  // Constructor with a column-major NumericTensor
  explicit SparseCOOIndex(const std::shared_ptr<CoordsTensor>& coords);

  const std::shared_ptr<CoordsTensor>& indices() const { return coords_; }

  std::string ToString() const override;

 protected:
  std::shared_ptr<CoordsTensor> coords_;
};

// ----------------------------------------------------------------------
// SparseCSRIndex class

class ARROW_EXPORT SparseCSRIndex : public SparseIndexBase<SparseCSRIndex> {
 public:
  using IndexTensor = NumericTensor<Int64Type>;

  static constexpr SparseIndex::format_type format_type_id = SparseIndex::CSR;

  // Constructor with two index vectors
  explicit SparseCSRIndex(const std::shared_ptr<IndexTensor>& indptr,
                          const std::shared_ptr<IndexTensor>& indices);

  const std::shared_ptr<IndexTensor>& indptr() const { return indptr_; }
  const std::shared_ptr<IndexTensor>& indices() const { return indices_; }

  std::string ToString() const override;

 protected:
  std::shared_ptr<IndexTensor> indptr_;
  std::shared_ptr<IndexTensor> indices_;
};

// ----------------------------------------------------------------------
// SparseTensorBase class

class ARROW_EXPORT SparseTensorBase {
 public:
  virtual ~SparseTensorBase() = default;

  virtual SparseIndex::format_type sparse_index_format_type_id() const = 0;

  std::shared_ptr<DataType> type() const { return type_; }
  std::shared_ptr<Buffer> data() const { return data_; }

  const uint8_t* raw_data() const { return data_->data(); }
  uint8_t* raw_mutable_data() const { return data_->mutable_data(); }

  const std::vector<int64_t>& shape() const { return shape_; }

  const std::shared_ptr<SparseIndex>& sparse_index() const { return sparse_index_; }

  int ndim() const { return static_cast<int>(shape_.size()); }

  const std::string& dim_name(int i) const;

  /// Total number of value cells in the sparse tensor
  int64_t size() const;

  /// Return true if the underlying data buffer is mutable
  bool is_mutable() const { return data_->is_mutable(); }

  /// Total number of non-zero cells in the sparse tensor
  virtual int64_t length() const = 0;

 protected:
  // Constructor with all attributes
  SparseTensorBase(const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
                   const std::vector<int64_t>& shape, const std::shared_ptr<SparseIndex>& sparse_index,
                   const std::vector<std::string>& dim_names);

  std::shared_ptr<DataType> type_;
  std::shared_ptr<Buffer> data_;
  std::vector<int64_t> shape_;
  std::shared_ptr<SparseIndex> sparse_index_;

  /// These names are optional
  std::vector<std::string> dim_names_;
};

// ----------------------------------------------------------------------
// SparseTensor class

template <typename SparseIndexType>
class ARROW_EXPORT SparseTensor : public SparseTensorBase {
 public:
  virtual ~SparseTensor() = default;

  // Constructor with all attributes
  SparseTensor(const std::shared_ptr<SparseIndexType>& sparse_index,
               const std::shared_ptr<DataType>& type, const std::shared_ptr<Buffer>& data,
               const std::vector<int64_t>& shape,
               const std::vector<std::string>& dim_names)
      : SparseTensorBase(type, data, shape, sparse_index, dim_names) {}

  // Constructor for empty sparse tensor
  SparseTensor(const std::shared_ptr<DataType>& type, const std::vector<int64_t>& shape,
               const std::vector<std::string>& dim_names = {});

  // Constructor with a dense numeric tensor
  template <typename TYPE>
  explicit SparseTensor(const NumericTensor<TYPE>& tensor);

  // Constructor with a dense tensor
  explicit SparseTensor(const Tensor& tensor);

  SparseIndex::format_type sparse_index_format_type_id() const { return SparseIndexType::format_type_id; }

  /// Total number of non-zero cells in the sparse tensor
  int64_t length() const { return sparse_index_ ? sparse_index_->length() : 0; }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(SparseTensor);
};

}  // namespace arrow

#endif  // ARROW_SPARSE_TENSOR_H
