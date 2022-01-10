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

#include "benchmark/benchmark.h"

#include <numeric>
#include <random>

#include "arrow/sparse_tensor.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

enum ContiguousType { ROW_MAJOR, COLUMN_MAJOR, STRIDED };

template <ContiguousType contiguous_type, typename ValueType, typename IndexType>
class TensorConversionFixture : public benchmark::Fixture {
 protected:
  using c_value_type = typename ValueType::c_type;
  using c_index_type = typename IndexType::c_type;

  std::shared_ptr<DataType> value_type_ = TypeTraits<ValueType>::type_singleton();
  std::shared_ptr<DataType> index_type_ = TypeTraits<IndexType>::type_singleton();

  std::vector<c_value_type> values_;
  std::shared_ptr<Tensor> tensor_;

 public:
  void SetUp(const ::benchmark::State& state) {
    std::vector<int64_t> shape = {30, 8, 20, 9};
    auto n = std::accumulate(shape.begin(), shape.end(), int64_t(1),
                             [](int64_t acc, int64_t i) { return acc * i; });
    auto m = n / 100;

    switch (contiguous_type) {
      case STRIDED:
        values_.resize(2 * n);
        for (int64_t i = 0; i < 100; ++i) {
          values_[2 * i * m] = static_cast<c_value_type>(i);
        }
        break;
      default:
        values_.resize(n);
        for (int64_t i = 0; i < 100; ++i) {
          values_[i * m] = static_cast<c_value_type>(i);
        }
        break;
    }

    std::vector<int64_t> strides;
    int64_t total = sizeof(c_value_type);
    switch (contiguous_type) {
      case ROW_MAJOR:
        break;
      case COLUMN_MAJOR: {
        for (auto i : shape) {
          strides.push_back(total);
          total *= i;
        }
        break;
      }
      case STRIDED: {
        total *= 2;
        for (auto i : shape) {
          strides.push_back(total);
          total *= i;
        }
        break;
      }
    }
    ABORT_NOT_OK(
        Tensor::Make(value_type_, Buffer::Wrap(values_), shape, strides).Value(&tensor_));
  }

  void SetUpRowMajor() {}
};

template <ContiguousType contiguous_type, typename ValueType, typename IndexType>
class MatrixConversionFixture : public benchmark::Fixture {
 protected:
  using c_value_type = typename ValueType::c_type;
  using c_index_type = typename IndexType::c_type;

  std::shared_ptr<DataType> value_type_ = TypeTraits<ValueType>::type_singleton();
  std::shared_ptr<DataType> index_type_ = TypeTraits<IndexType>::type_singleton();

  std::vector<c_value_type> values_;
  std::shared_ptr<Tensor> tensor_;

 public:
  void SetUp(const ::benchmark::State& state) {
    std::vector<int64_t> shape = {88, 113};
    auto n = std::accumulate(shape.begin(), shape.end(), int64_t(1),
                             [](int64_t acc, int64_t i) { return acc * i; });
    auto m = n / 100;

    switch (contiguous_type) {
      case STRIDED:
        values_.resize(2 * n);
        for (int64_t i = 0; i < 100; ++i) {
          values_[2 * i * m] = static_cast<c_value_type>(i);
        }
        break;
      default:
        values_.resize(n);
        for (int64_t i = 0; i < 100; ++i) {
          values_[i * m] = static_cast<c_value_type>(i);
        }
        break;
    }

    std::vector<int64_t> strides;
    int64_t total = sizeof(c_value_type);
    switch (contiguous_type) {
      case ROW_MAJOR:
        break;
      case COLUMN_MAJOR: {
        for (auto i : shape) {
          strides.push_back(total);
          total *= i;
        }
        break;
      }
      case STRIDED: {
        total *= 2;
        for (auto i : shape) {
          strides.push_back(total);
          total *= i;
        }
        break;
      }
    }
    ABORT_NOT_OK(Tensor::Make(value_type_, Buffer::Wrap(values_), shape).Value(&tensor_));
  }
};

#define DEFINE_TYPED_TENSOR_CONVERSION_FIXTURE(value_type_name)                \
  template <typename IndexType>                                                \
  using value_type_name##RowMajorTensorConversionFixture =                     \
      TensorConversionFixture<ROW_MAJOR, value_type_name##Type, IndexType>;    \
  template <typename IndexType>                                                \
  using value_type_name##ColumnMajorTensorConversionFixture =                  \
      TensorConversionFixture<COLUMN_MAJOR, value_type_name##Type, IndexType>; \
  template <typename IndexType>                                                \
  using value_type_name##StridedTensorConversionFixture =                      \
      TensorConversionFixture<STRIDED, value_type_name##Type, IndexType>

DEFINE_TYPED_TENSOR_CONVERSION_FIXTURE(Int8);
DEFINE_TYPED_TENSOR_CONVERSION_FIXTURE(Float);
DEFINE_TYPED_TENSOR_CONVERSION_FIXTURE(Double);

#define DEFINE_TYPED_MATRIX_CONVERSION_FIXTURE(value_type_name)                \
  template <typename IndexType>                                                \
  using value_type_name##RowMajorMatrixConversionFixture =                     \
      MatrixConversionFixture<ROW_MAJOR, value_type_name##Type, IndexType>;    \
  template <typename IndexType>                                                \
  using value_type_name##ColumnMajorMatrixConversionFixture =                  \
      MatrixConversionFixture<COLUMN_MAJOR, value_type_name##Type, IndexType>; \
  template <typename IndexType>                                                \
  using value_type_name##StridedMatrixConversionFixture =                      \
      MatrixConversionFixture<STRIDED, value_type_name##Type, IndexType>

DEFINE_TYPED_MATRIX_CONVERSION_FIXTURE(Int8);
DEFINE_TYPED_MATRIX_CONVERSION_FIXTURE(Float);
DEFINE_TYPED_MATRIX_CONVERSION_FIXTURE(Double);

#define BENCHMARK_CONVERT_TENSOR_(Contiguous, kind, format, value_type_name,     \
                                  index_type_name)                               \
  BENCHMARK_TEMPLATE_F(value_type_name##Contiguous##kind##ConversionFixture,     \
                       ConvertToSparse##format##kind##index_type_name,           \
                       index_type_name##Type)                                    \
  (benchmark::State & state) { /* NOLINT non-const reference */                  \
    std::shared_ptr<Sparse##format##kind> sparse_tensor;                         \
    for (auto _ : state) {                                                       \
      ABORT_NOT_OK(Sparse##format##kind::Make(*this->tensor_, this->index_type_) \
                       .Value(&sparse_tensor));                                  \
    }                                                                            \
    benchmark::DoNotOptimize(sparse_tensor);                                     \
    state.SetItemsProcessed(state.iterations() * this->tensor_->size());         \
    state.SetBytesProcessed(state.iterations() * this->tensor_->data()->size()); \
  }

#define BENCHMARK_CONVERT_TENSOR(kind, format, value_type_name, index_type_name)       \
  BENCHMARK_CONVERT_TENSOR_(RowMajor, kind, format, value_type_name, index_type_name); \
  BENCHMARK_CONVERT_TENSOR_(ColumnMajor, kind, format, value_type_name,                \
                            index_type_name);                                          \
  BENCHMARK_CONVERT_TENSOR_(Strided, kind, format, value_type_name, index_type_name)

BENCHMARK_CONVERT_TENSOR(Tensor, COO, Int8, Int32);
BENCHMARK_CONVERT_TENSOR(Tensor, COO, Int8, Int64);
BENCHMARK_CONVERT_TENSOR(Tensor, COO, Float, Int32);
BENCHMARK_CONVERT_TENSOR(Tensor, COO, Float, Int64);
BENCHMARK_CONVERT_TENSOR(Tensor, COO, Double, Int32);
BENCHMARK_CONVERT_TENSOR(Tensor, COO, Double, Int64);

BENCHMARK_CONVERT_TENSOR(Matrix, CSR, Int8, Int8);
BENCHMARK_CONVERT_TENSOR(Matrix, CSR, Int8, Int16);
BENCHMARK_CONVERT_TENSOR(Matrix, CSR, Float, Int32);
BENCHMARK_CONVERT_TENSOR(Matrix, CSR, Float, Int64);
BENCHMARK_CONVERT_TENSOR(Matrix, CSR, Double, Int32);
BENCHMARK_CONVERT_TENSOR(Matrix, CSR, Double, Int64);

BENCHMARK_CONVERT_TENSOR(Matrix, CSC, Int8, Int32);
BENCHMARK_CONVERT_TENSOR(Matrix, CSC, Int8, Int64);
BENCHMARK_CONVERT_TENSOR(Matrix, CSC, Float, Int32);
BENCHMARK_CONVERT_TENSOR(Matrix, CSC, Float, Int64);
BENCHMARK_CONVERT_TENSOR(Matrix, CSC, Double, Int32);
BENCHMARK_CONVERT_TENSOR(Matrix, CSC, Double, Int64);

BENCHMARK_CONVERT_TENSOR(Tensor, CSF, Int8, Int32);
BENCHMARK_CONVERT_TENSOR(Tensor, CSF, Int8, Int64);
BENCHMARK_CONVERT_TENSOR(Tensor, CSF, Float, Int32);
BENCHMARK_CONVERT_TENSOR(Tensor, CSF, Float, Int64);
BENCHMARK_CONVERT_TENSOR(Tensor, CSF, Double, Int32);
BENCHMARK_CONVERT_TENSOR(Tensor, CSF, Double, Int64);

}  // namespace arrow
