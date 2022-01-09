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

#include <algorithm>
#include <cstdint>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {
namespace internal {

// Visit all physical types for which sorting is implemented.
#define VISIT_SORTABLE_PHYSICAL_TYPES(VISIT) \
  VISIT(BooleanType)                         \
  VISIT(Int8Type)                            \
  VISIT(Int16Type)                           \
  VISIT(Int32Type)                           \
  VISIT(Int64Type)                           \
  VISIT(UInt8Type)                           \
  VISIT(UInt16Type)                          \
  VISIT(UInt32Type)                          \
  VISIT(UInt64Type)                          \
  VISIT(FloatType)                           \
  VISIT(DoubleType)                          \
  VISIT(BinaryType)                          \
  VISIT(LargeBinaryType)                     \
  VISIT(FixedSizeBinaryType)                 \
  VISIT(Decimal128Type)                      \
  VISIT(Decimal256Type)

// The target chunk in a chunked array.
template <typename ArrayType>
struct ResolvedChunk {
  using V = GetViewType<typename ArrayType::TypeClass>;
  using LogicalValueType = typename V::T;

  // The target array in chunked array.
  const ArrayType* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const ArrayType* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->IsNull(index); }

  LogicalValueType Value() const { return V::LogicalValue(array->GetView(index)); }
};

// ResolvedChunk specialization for StructArray
template <>
struct ResolvedChunk<StructArray> {
  // The target struct in chunked array.
  const StructArray* array;
  // The field index in the target struct.
  const int64_t index;

  ResolvedChunk(const StructArray* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->field(0)->IsNull(index); }
};

// ResolvedChunk specialization for untyped arrays when all is needed is null lookup
template <>
struct ResolvedChunk<Array> {
  // The target array in chunked array.
  const Array* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const Array* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->IsNull(index); }
};

struct ChunkLocation {
  int64_t chunk_index, index_in_chunk;
};

// An object that resolves an array chunk depending on the index.
struct ChunkResolver {
  explicit ChunkResolver(std::vector<int64_t> lengths)
      : num_chunks_(static_cast<int64_t>(lengths.size())),
        offsets_(MakeEndOffsets(std::move(lengths))),
        cached_chunk_(0) {}

  ChunkLocation Resolve(int64_t index) const {
    // It is common for the algorithms below to make consecutive accesses at
    // a relatively small distance from each other, hence often falling in
    // the same chunk.
    // This is trivial when merging (assuming each side of the merge uses
    // its own resolver), but also in the inner recursive invocations of
    // partitioning.
    const bool cache_hit =
        (index >= offsets_[cached_chunk_] && index < offsets_[cached_chunk_ + 1]);
    if (ARROW_PREDICT_TRUE(cache_hit)) {
      return {cached_chunk_, index - offsets_[cached_chunk_]};
    } else {
      return ResolveMissBisect(index);
    }
  }

  static ChunkResolver FromBatches(const RecordBatchVector& batches) {
    std::vector<int64_t> lengths(batches.size());
    std::transform(
        batches.begin(), batches.end(), lengths.begin(),
        [](const std::shared_ptr<RecordBatch>& batch) { return batch->num_rows(); });
    return ChunkResolver(std::move(lengths));
  }

 protected:
  ChunkLocation ResolveMissBisect(int64_t index) const {
    // Like std::upper_bound(), but hand-written as it can help the compiler.
    const int64_t* raw_offsets = offsets_.data();
    // Search [lo, lo + n)
    int64_t lo = 0, n = num_chunks_;
    while (n > 1) {
      int64_t m = n >> 1;
      int64_t mid = lo + m;
      if (index >= raw_offsets[mid]) {
        lo = mid;
        n -= m;
      } else {
        n = m;
      }
    }
    cached_chunk_ = lo;
    return {lo, index - offsets_[lo]};
  }

  static std::vector<int64_t> MakeEndOffsets(std::vector<int64_t> lengths) {
    int64_t offset = 0;
    for (auto& v : lengths) {
      const auto this_length = v;
      v = offset;
      offset += this_length;
    }
    lengths.push_back(offset);
    return lengths;
  }

  int64_t num_chunks_;
  std::vector<int64_t> offsets_;

  mutable int64_t cached_chunk_;
};

struct ChunkedArrayResolver : protected ChunkResolver {
  explicit ChunkedArrayResolver(const std::vector<const Array*>& chunks)
      : ChunkResolver(MakeLengths(chunks)), chunks_(chunks) {}

  template <typename ArrayType>
  ResolvedChunk<ArrayType> Resolve(int64_t index) const {
    const auto loc = ChunkResolver::Resolve(index);
    return ResolvedChunk<ArrayType>(
        checked_cast<const ArrayType*>(chunks_[loc.chunk_index]), loc.index_in_chunk);
  }

 protected:
  static std::vector<int64_t> MakeLengths(const std::vector<const Array*>& chunks) {
    std::vector<int64_t> lengths(chunks.size());
    std::transform(chunks.begin(), chunks.end(), lengths.begin(),
                   [](const Array* arr) { return arr->length(); });
    return lengths;
  }

  const std::vector<const Array*> chunks_;
};

inline std::vector<const Array*> GetArrayPointers(const ArrayVector& arrays) {
  std::vector<const Array*> pointers(arrays.size());
  std::transform(arrays.begin(), arrays.end(), pointers.begin(),
                 [&](const std::shared_ptr<Array>& array) { return array.get(); });
  return pointers;
}

template <typename ArrayType>
struct ResolvedChunkComparator {
  bool Compare(const ResolvedChunk<ArrayType>& chunk_left,
               const ResolvedChunk<ArrayType>& chunk_right) const {
    return chunk_left.Value() < chunk_right.Value();
  }

  bool CompareChunks(const ChunkedArrayResolver& left_resolver,
                     const ChunkedArrayResolver& right_resolver, uint64_t left,
                     uint64_t right) {
    const auto chunk_left = left_resolver.Resolve<ArrayType>(left);
    const auto chunk_right = right_resolver.Resolve<ArrayType>(right);
    return Compare(chunk_left, chunk_right);
  }
};

template <>
struct ResolvedChunkComparator<StructArray> {
  bool CompareChunks(const ChunkedArrayResolver& left_resolver,
                     const ChunkedArrayResolver& right_resolver, uint64_t left,
                     uint64_t right) {
    const auto chunk_left = left_resolver.Resolve<StructArray>(left);
    const auto chunk_right = right_resolver.Resolve<StructArray>(right);
    return Compare(chunk_left, chunk_right);
  }

  bool Compare(const ResolvedChunk<StructArray>& chunk_left,
               const ResolvedChunk<StructArray>& chunk_right) const {
    std::shared_ptr<DataType> field_type = chunk_left.array->field(0)->type();
    ValComparatorVisitor type_visitor;
    auto vCompare = type_visitor.Create(*field_type);
    return vCompare(chunk_left.array->field(0), chunk_left.index,
                    chunk_right.array->field(0), chunk_right.index);
  }

  using ValComparator = std::function<bool(std::shared_ptr<arrow::Array>, int64_t,
                                           std::shared_ptr<arrow::Array>, int64_t)>;

  struct ValComparatorVisitor {
#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return VisitGeneric(type); }

    VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
#undef VISIT

    Status Visit(const DataType& type) {
      return Status::TypeError("Unsupported type for ValComparatorVisitor: ",
                               type.ToString());
    }

    template <typename T>
    Status VisitGeneric(const T&) {
      using ArrayType = typename TypeTraits<T>::ArrayType;

      out = [](std::shared_ptr<arrow::Array> base, int64_t base_index,
               std::shared_ptr<arrow::Array> target, int64_t target_index) {
        auto base_arr = arrow::internal::checked_pointer_cast<ArrayType>(base);
        auto target_arr = arrow::internal::checked_pointer_cast<ArrayType>(target);
        return base_arr->Value(base_index) < target_arr->Value(target_index);
      };
      return Status::OK();
    }

    ValComparator Create(const DataType& type) {
      DCHECK_OK(VisitTypeInline(type, this));
      return out;
    }

    ValComparator out;
  };
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow
