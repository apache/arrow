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

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \addtogroup encoded-builders
///
/// @{

// ----------------------------------------------------------------------
// RunLengthEncoded builder

class ARROW_EXPORT RunLengthEncodedBuilder : public ArrayBuilder {
 public:
  RunLengthEncodedBuilder(std::shared_ptr<ArrayBuilder> values_builder, MemoryPool *pool);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

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
  Status ResizePhysical(int64_t capacity);

 private:
  Status FinishRun();
  Status AddLength(int64_t added_length);

  std::shared_ptr<RunLengthEncodedType> type_;
  ArrayBuilder& values_builder_;
  TypedBufferBuilder<int32_t> run_ends_builder_;
    // must be empty pointer for non-valid values
  std::shared_ptr<const Scalar> current_value_;
  int64_t run_start_ = 0;
};

/// @}

}  // namespace arrow
