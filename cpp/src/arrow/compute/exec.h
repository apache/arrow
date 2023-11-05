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

// NOTE: API is EXPERIMENTAL and will change without going through a
// deprecation cycle

#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

// It seems like 64K might be a good default chunksize to use for execution
// based on the experience of other query processing systems. The current
// default is not to chunk contiguous arrays, though, but this may change in
// the future once parallel execution is implemented
static constexpr int64_t kDefaultExecChunksize = UINT16_MAX;

/// \brief Context for expression-global variables and options used by
/// function evaluation
class ARROW_EXPORT ExecContext {
 public:
  // If no function registry passed, the default is used.
  explicit ExecContext(MemoryPool* pool = default_memory_pool(),
                       ::arrow::internal::Executor* executor = NULLPTR,
                       FunctionRegistry* func_registry = NULLPTR);

  /// \brief The MemoryPool used for allocations, default is
  /// default_memory_pool().
  MemoryPool* memory_pool() const { return pool_; }

  const ::arrow::internal::CpuInfo* cpu_info() const;

  /// \brief An Executor which may be used to parallelize execution.
  ::arrow::internal::Executor* executor() const { return executor_; }

  /// \brief The FunctionRegistry for looking up functions by name and
  /// selecting kernels for execution. Defaults to the library-global function
  /// registry provided by GetFunctionRegistry.
  FunctionRegistry* func_registry() const { return func_registry_; }

  // \brief Set maximum length unit of work for kernel execution. Larger
  // contiguous array inputs will be split into smaller chunks, and, if
  // possible and enabled, processed in parallel. The default chunksize is
  // INT64_MAX, so contiguous arrays are not split.
  void set_exec_chunksize(int64_t chunksize) { exec_chunksize_ = chunksize; }

  // \brief Maximum length for ExecBatch data chunks processed by
  // kernels. Contiguous array inputs with longer length will be split into
  // smaller chunks.
  int64_t exec_chunksize() const { return exec_chunksize_; }

  /// \brief Set whether to use multiple threads for function execution. This
  /// is not yet used.
  void set_use_threads(bool use_threads = true) { use_threads_ = use_threads; }

  /// \brief If true, then utilize multiple threads where relevant for function
  /// execution. This is not yet used.
  bool use_threads() const { return use_threads_; }

  // Set the preallocation strategy for kernel execution as it relates to
  // chunked execution. For chunked execution, whether via ChunkedArray inputs
  // or splitting larger Array arguments into smaller pieces, contiguous
  // allocation (if permitted by the kernel) will allocate one large array to
  // write output into yielding it to the caller at the end. If this option is
  // set to off, then preallocations will be performed independently for each
  // chunk of execution
  //
  // TODO: At some point we might want the limit the size of contiguous
  // preallocations. For example, even if the exec_chunksize is 64K or less, we
  // might limit contiguous allocations to 1M records, say.
  void set_preallocate_contiguous(bool preallocate) {
    preallocate_contiguous_ = preallocate;
  }

  /// \brief If contiguous preallocations should be used when doing chunked
  /// execution as specified by exec_chunksize(). See
  /// set_preallocate_contiguous() for more information.
  bool preallocate_contiguous() const { return preallocate_contiguous_; }

 private:
  MemoryPool* pool_;
  ::arrow::internal::Executor* executor_;
  FunctionRegistry* func_registry_;
  int64_t exec_chunksize_ = std::numeric_limits<int64_t>::max();
  bool preallocate_contiguous_ = true;
  bool use_threads_ = true;
};

// TODO: Consider standardizing on uint16 selection vectors and only use them
// when we can ensure that each value is 64K length or smaller

/// \brief Container for an array of value selection indices that were
/// materialized from a filter.
///
/// Columnar query engines (see e.g. [1]) have found that rather than
/// materializing filtered data, the filter can instead be converted to an
/// array of the "on" indices and then "fusing" these indices in operator
/// implementations. This is especially relevant for aggregations but also
/// applies to scalar operations.
///
/// We are not yet using this so this is mostly a placeholder for now.
///
/// [1]: http://cidrdb.org/cidr2005/papers/P19.pdf
class ARROW_EXPORT SelectionVector {
 public:
  explicit SelectionVector(std::shared_ptr<ArrayData> data);

  explicit SelectionVector(const Array& arr);

  /// \brief Create SelectionVector from boolean mask
  static Result<std::shared_ptr<SelectionVector>> FromMask(const BooleanArray& arr);

  const int32_t* indices() const { return indices_; }
  int32_t length() const;

 private:
  std::shared_ptr<ArrayData> data_;
  const int32_t* indices_;
};

/// An index to represent that a batch does not belong to an ordered stream
constexpr int64_t kUnsequencedIndex = -1;

/// \brief A unit of work for kernel execution. It contains a collection of
/// Array and Scalar values and an optional SelectionVector indicating that
/// there is an unmaterialized filter that either must be materialized, or (if
/// the kernel supports it) pushed down into the kernel implementation.
///
/// ExecBatch is semantically similar to RecordBatch in that in a SQL context
/// it represents a collection of records, but constant "columns" are
/// represented by Scalar values rather than having to be converted into arrays
/// with repeated values.
///
/// TODO: Datum uses arrow/util/variant.h which may be a bit heavier-weight
/// than is desirable for this class. Microbenchmarks would help determine for
/// sure. See ARROW-8928.

/// \addtogroup acero-internals
/// @{
/*
ExecBatch 是一种类似于 RecordBatch 的二维数据结构。它可以拥有零个或多个列，并且所有的列必须
具有相同的长度。与 RecordBatch 类似，ExecBatch 用于在计算引擎中处理和传递数据。它通常被用作执
行计划中的中间结果或者是计算操作的输入和输出。

以下是关于 ExecBatch 的特性和约束：
  列数：ExecBatch 可以包含任意数量的列，从零列到多列都可以。
  列类型：每个列可以有不同的数据类型，例如整数、浮点数、字符串等。
  列长度：所有的列必须具有相同的长度，这是 ExecBatch 的一个重要约束条件。换句话说，所有
         的列在维度上必须对齐，以保持数据的一致性和正确性。
  数据存储：ExecBatch 中的数据通常以列式存储（columnar storage）的方式进行组织，这有助
          于高效的数据处理和查询操作。

ExecBatch没有自己的模式声明（schema）。这是因为假设ExecBatch是批量数据流的一部分，并且该数据
流具有一致的模式声明。因此，ExecBatch的模式通常存储在ExecNode中。

ExecBatch中的列可以是Array（数组）或Scalar（标量）。当列是Scalar时，意味着当前列中的每一行
的值都是同一个。ExecBatch还具有一个长度属性，用于描述批次中的行数。因此，可以将Scalar视为具有
长度元素的常量数组。

ExecBatch包含执行计划所使用的附加信息。例如，索引可用于描述批次在有序数据流中的位置。我们预计
ExecBatch还将发展出包含其他字段的形式，比如选择向量（selection vector）。

当将RecordBatch转换为ExecBatch时，它们共享相同的底层数组，没有发生实际的数据复制。这意味着对
RecordBatch 或 ExecBatch 进行更改时，它们之间的数据是共享的。

然而，将 ExecBatch 转换回 RecordBatch 时情况会有所不同。如果 ExecBatch 中存在标量列，那么转
换过程将包括创建新的数组，并将标量值复制到这些新数组中。这是因为 RecordBatch 和 ExecBatch 在内
部存储数据的方式不同，后者允许标量存在，而前者不允许。

因此，在将 ExecBatch 转换回 RecordBatch 时，只有当 ExecBatch 中不存在标量列时，转换才能保持
零拷贝。如果存在标量列，将需要额外的内存分配和数据复制操作。

总结起来，将 RecordBatch 转换为 ExecBatch 始终是零拷贝的，因为它们共享相同的底层数组。将
ExecBatch 转换为 RecordBatch 只有在无标量列的情况下才是零拷贝的。在存在标量列的情况下，将涉
及到额外的内存分配和数据复制。

参考: https://arrow.apache.org/docs/cpp/acero/overview.html
*/
struct ARROW_EXPORT ExecBatch {
  ExecBatch() = default;
  ExecBatch(std::vector<Datum> values, int64_t length)
      : values(std::move(values)), length(length) {}

  explicit ExecBatch(const RecordBatch& batch);

  /// \brief Infer the ExecBatch length from values.
  static Result<int64_t> InferLength(const std::vector<Datum>& values);

  /// Creates an ExecBatch with length-validation.
  ///
  /// If any value is given, then all values must have a common length. If the given
  /// length is negative, then the length of the ExecBatch is set to this common length,
  /// or to 1 if no values are given. Otherwise, the given length must equal the common
  /// length, if any value is given.
  static Result<ExecBatch> Make(std::vector<Datum> values, int64_t length = -1);

  Result<std::shared_ptr<RecordBatch>> ToRecordBatch(
      std::shared_ptr<Schema> schema, MemoryPool* pool = default_memory_pool()) const;

  /// The values representing positional arguments to be passed to a kernel's
  /// exec function for processing.
  std::vector<Datum> values;

  /// A deferred filter represented as an array of indices into the values.
  ///
  /// For example, the filter [true, true, false, true] would be represented as
  /// the selection vector [0, 1, 3]. When the selection vector is set,
  /// ExecBatch::length is equal to the length of this array.
  std::shared_ptr<SelectionVector> selection_vector;

  /// A predicate Expression guaranteed to evaluate to true for all rows in this batch.
  Expression guarantee = literal(true);

  /// The semantic length of the ExecBatch. When the values are all scalars,
  /// the length should be set to 1 for non-aggregate kernels, otherwise the
  /// length is taken from the array values, except when there is a selection
  /// vector. When there is a selection vector set, the length of the batch is
  /// the length of the selection. Aggregate kernels can have an ExecBatch
  /// formed by projecting just the partition columns from a batch in which
  /// case, it would have scalar rows with length greater than 1.
  ///
  /// If the array values are of length 0 then the length is 0 regardless of
  /// whether any values are Scalar.
  int64_t length = 0;

  /// \brief index of this batch in a sorted stream of batches
  ///
  /// This index must be strictly monotonic starting at 0 without gaps or
  /// it can be set to kUnsequencedIndex if there is no meaningful order
  int64_t index = kUnsequencedIndex;

  /// \brief The sum of bytes in each buffer referenced by the batch
  ///
  /// Note: Scalars are not counted
  /// Note: Some values may referenced only part of a buffer, for
  ///       example, an array with an offset.  The actual data
  ///       visible to this batch will be smaller than the total
  ///       buffer size in this case.
  int64_t TotalBufferSize() const;

  /// \brief Return the value at the i-th index
  template <typename index_type>
  inline const Datum& operator[](index_type i) const {
    return values[i];
  }

  bool Equals(const ExecBatch& other) const;

  /// \brief A convenience for the number of values / arguments.
  int num_values() const { return static_cast<int>(values.size()); }

  ExecBatch Slice(int64_t offset, int64_t length) const;

  Result<ExecBatch> SelectValues(const std::vector<int>& ids) const;

  /// \brief A convenience for returning the types from the batch.
  std::vector<TypeHolder> GetTypes() const {
    std::vector<TypeHolder> result;
    for (const auto& value : this->values) {
      result.emplace_back(value.type());
    }
    return result;
  }

  std::string ToString() const;
};

inline bool operator==(const ExecBatch& l, const ExecBatch& r) { return l.Equals(r); }
inline bool operator!=(const ExecBatch& l, const ExecBatch& r) { return !l.Equals(r); }

ARROW_EXPORT void PrintTo(const ExecBatch&, std::ostream*);

/// @}

/// \defgroup compute-internals Utilities for calling functions, useful for those
/// extending the function registry
///
/// @{

struct ExecValue {
  ArraySpan array = {};
  const Scalar* scalar = NULLPTR;

  ExecValue(Scalar* scalar)  // NOLINT implicit conversion
      : scalar(scalar) {}

  ExecValue(ArraySpan array)  // NOLINT implicit conversion
      : array(std::move(array)) {}

  ExecValue(const ArrayData& array) {  // NOLINT implicit conversion
    this->array.SetMembers(array);
  }

  ExecValue() = default;
  ExecValue(const ExecValue& other) = default;
  ExecValue& operator=(const ExecValue& other) = default;
  ExecValue(ExecValue&& other) = default;
  ExecValue& operator=(ExecValue&& other) = default;

  int64_t length() const { return this->is_array() ? this->array.length : 1; }

  bool is_array() const { return this->scalar == NULLPTR; }
  bool is_scalar() const { return !this->is_array(); }

  void SetArray(const ArrayData& array) {
    this->array.SetMembers(array);
    this->scalar = NULLPTR;
  }

  void SetScalar(const Scalar* scalar) { this->scalar = scalar; }

  template <typename ExactType>
  const ExactType& scalar_as() const {
    return ::arrow::internal::checked_cast<const ExactType&>(*this->scalar);
  }

  /// XXX: here temporarily for compatibility with datum, see
  /// e.g. MakeStructExec in scalar_nested.cc
  int64_t null_count() const {
    if (this->is_array()) {
      return this->array.GetNullCount();
    } else {
      return this->scalar->is_valid ? 0 : 1;
    }
  }

  const DataType* type() const {
    if (this->is_array()) {
      return array.type;
    } else {
      return scalar->type.get();
    }
  }
};

struct ARROW_EXPORT ExecResult {
  // The default value of the variant is ArraySpan
  std::variant<ArraySpan, std::shared_ptr<ArrayData>> value;

  int64_t length() const {
    if (this->is_array_span()) {
      return this->array_span()->length;
    } else {
      return this->array_data()->length;
    }
  }

  const DataType* type() const {
    if (this->is_array_span()) {
      return this->array_span()->type;
    } else {
      return this->array_data()->type.get();
    }
  }

  const ArraySpan* array_span() const { return &std::get<ArraySpan>(this->value); }
  ArraySpan* array_span_mutable() { return &std::get<ArraySpan>(this->value); }

  bool is_array_span() const { return this->value.index() == 0; }

  const std::shared_ptr<ArrayData>& array_data() const {
    return std::get<std::shared_ptr<ArrayData>>(this->value);
  }
  ArrayData* array_data_mutable() {
    return std::get<std::shared_ptr<ArrayData>>(this->value).get();
  }

  bool is_array_data() const { return this->value.index() == 1; }
};

/// \brief A "lightweight" column batch object which contains no
/// std::shared_ptr objects and does not have any memory ownership
/// semantics. Can represent a view onto an "owning" ExecBatch.
struct ARROW_EXPORT ExecSpan {
  ExecSpan() = default;
  ExecSpan(const ExecSpan& other) = default;
  ExecSpan& operator=(const ExecSpan& other) = default;
  ExecSpan(ExecSpan&& other) = default;
  ExecSpan& operator=(ExecSpan&& other) = default;

  explicit ExecSpan(std::vector<ExecValue> values, int64_t length)
      : length(length), values(std::move(values)) {}

  explicit ExecSpan(const ExecBatch& batch) {
    this->length = batch.length;
    this->values.resize(batch.values.size());
    for (size_t i = 0; i < batch.values.size(); ++i) {
      const Datum& in_value = batch[i];
      ExecValue* out_value = &this->values[i];
      if (in_value.is_array()) {
        out_value->SetArray(*in_value.array());
      } else {
        out_value->SetScalar(in_value.scalar().get());
      }
    }
  }

  /// \brief Return the value at the i-th index
  template <typename index_type>
  inline const ExecValue& operator[](index_type i) const {
    return values[i];
  }

  /// \brief A convenience for the number of values / arguments.
  int num_values() const { return static_cast<int>(values.size()); }

  std::vector<TypeHolder> GetTypes() const {
    std::vector<TypeHolder> result;
    for (const auto& value : this->values) {
      result.emplace_back(value.type());
    }
    return result;
  }

  ExecBatch ToExecBatch() const {
    ExecBatch result;
    result.length = this->length;
    for (const ExecValue& value : this->values) {
      if (value.is_array()) {
        result.values.push_back(value.array.ToArrayData());
      } else {
        result.values.push_back(value.scalar->GetSharedPtr());
      }
    }
    return result;
  }

  int64_t length = 0;
  std::vector<ExecValue> values;
};

/// \defgroup compute-call-function One-shot calls to compute functions
///
/// @{

/// \brief One-shot invoker for all types of functions.
///
/// Does kernel dispatch, argument checking, iteration of ChunkedArray inputs,
/// and wrapping of outputs.
ARROW_EXPORT
Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           const FunctionOptions* options, ExecContext* ctx = NULLPTR);

/// \brief Variant of CallFunction which uses a function's default options.
///
/// NB: Some functions require FunctionOptions be provided.
ARROW_EXPORT
Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           ExecContext* ctx = NULLPTR);

/// \brief One-shot invoker for all types of functions.
///
/// Does kernel dispatch, argument checking, iteration of ChunkedArray inputs,
/// and wrapping of outputs.
ARROW_EXPORT
Result<Datum> CallFunction(const std::string& func_name, const ExecBatch& batch,
                           const FunctionOptions* options, ExecContext* ctx = NULLPTR);

/// \brief Variant of CallFunction which uses a function's default options.
///
/// NB: Some functions require FunctionOptions be provided.
ARROW_EXPORT
Result<Datum> CallFunction(const std::string& func_name, const ExecBatch& batch,
                           ExecContext* ctx = NULLPTR);

/// @}

/// \defgroup compute-function-executor One-shot calls to obtain function executors
///
/// @{

/// \brief One-shot executor provider for all types of functions.
///
/// This function creates and initializes a `FunctionExecutor` appropriate
/// for the given function name, input types and function options.
ARROW_EXPORT
Result<std::shared_ptr<FunctionExecutor>> GetFunctionExecutor(
    const std::string& func_name, std::vector<TypeHolder> in_types,
    const FunctionOptions* options = NULLPTR, FunctionRegistry* func_registry = NULLPTR);

/// \brief One-shot executor provider for all types of functions.
///
/// This function creates and initializes a `FunctionExecutor` appropriate
/// for the given function name, input types (taken from the Datum arguments)
/// and function options.
ARROW_EXPORT
Result<std::shared_ptr<FunctionExecutor>> GetFunctionExecutor(
    const std::string& func_name, const std::vector<Datum>& args,
    const FunctionOptions* options = NULLPTR, FunctionRegistry* func_registry = NULLPTR);

/// @}

}  // namespace compute
}  // namespace arrow
