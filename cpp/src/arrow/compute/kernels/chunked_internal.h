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
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/chunk_resolver.h"
#include "arrow/compute/kernels/codegen_internal.h"

namespace arrow {
namespace compute {
namespace internal {

// The target chunk in a chunked array.
template <typename ArrayType>
struct ResolvedChunk {
  using ViewType = GetViewType<typename ArrayType::TypeClass>;
  using LogicalValueType = typename ViewType::T;

  // The target array in chunked array.
  const ArrayType* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const ArrayType* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->IsNull(index); }

  LogicalValueType Value() const { return ViewType::LogicalValue(array->GetView(index)); }
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

struct ChunkedArrayResolver : protected ::arrow::internal::ChunkResolver {
  ChunkedArrayResolver(const ChunkedArrayResolver& other)
      : ::arrow::internal::ChunkResolver(other.chunks_), chunks_(other.chunks_) {}

  explicit ChunkedArrayResolver(const std::vector<const Array*>& chunks)
      : ::arrow::internal::ChunkResolver(chunks), chunks_(chunks) {}

  template <typename ArrayType>
  ResolvedChunk<ArrayType> Resolve(int64_t index) const {
    const auto loc = ::arrow::internal::ChunkResolver::Resolve(index);
    return {checked_cast<const ArrayType*>(chunks_[loc.chunk_index]), loc.index_in_chunk};
  }

 protected:
  const std::vector<const Array*> chunks_;
};

inline std::vector<const Array*> GetArrayPointers(const ArrayVector& arrays) {
  std::vector<const Array*> pointers(arrays.size());
  std::transform(arrays.begin(), arrays.end(), pointers.begin(),
                 [&](const std::shared_ptr<Array>& array) { return array.get(); });
  return pointers;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
