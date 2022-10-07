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

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"

namespace arrow {

/// \addtogroup encoded-builders
///
/// @{

// ----------------------------------------------------------------------
// RunLengthEncoded builder

class ARROW_EXPORT RunLengthEncodedBuilder : public ArrayBuilder {
 public:
  RunLengthEncodedBuilder(MemoryPool* pool,
                          const std::shared_ptr<ArrayBuilder>& run_end_builder,
                          const std::shared_ptr<ArrayBuilder>& value_builder,
                          std::shared_ptr<DataType> type);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) final;
  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Resize(int64_t capacity) final;
  void Reset() final;

  Status Finish(std::shared_ptr<RunLengthEncodedArray>* out) { return FinishTyped(out); }
  Status AppendNull() final;
  Status AppendNulls(int64_t length) final;
  Status AppendEmptyValue() final;
  Status AppendEmptyValues(int64_t length) final;
  Status AppendScalar(const Scalar& scalar, int64_t n_repeats) final;
  Status AppendScalars(const ScalarVector& scalars) final;
  Status AppendArraySlice(const ArraySpan& array, int64_t offset, int64_t length) final;
  std::shared_ptr<DataType> type() const final;

  /// \brief Allocate enough memory for a given number of runs. Like Resize on non-RLE
  /// builders, it does not account for variable size data.
  Status ResizePhyiscal(int64_t capacity);

 private:
  Status FinishRun();
  Status AddLength(int64_t added_length);
  template <typename RunEndsType>
  Status DoAppendArray(const ArraySpan& to_append);
  template <typename RunEndsType>
  Status DoAppendRunEnd();
  ArrayBuilder& run_end_builder();
  ArrayBuilder& value_builder();

  std::shared_ptr<RunLengthEncodedType> type_;
  // must be null pointer for non-valid values
  std::shared_ptr<const Scalar> current_value_;
  int64_t run_start_ = 0;
};

/// @}

}  // namespace arrow
