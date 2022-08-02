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

// Array accessor classes for List, LargeList, FixedSizeList, Map, Struct, and
// Union

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \addtogroup encoded-arrays
///
/// @{

// ----------------------------------------------------------------------
// RunLengthEncoded

/// Concrete Array class for run-length encoded data
class ARROW_EXPORT RunLengthEncodedArray : public Array {
 public:
  using TypeClass = RunLengthEncodedType;

  explicit RunLengthEncodedArray(const std::shared_ptr<ArrayData>& data);

  RunLengthEncodedArray(const std::shared_ptr<DataType>& type, int64_t length,
                        std::shared_ptr<Array>& values_array,
                        std::shared_ptr<Buffer> run_ends_buffer,
                        int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  /// \brief Construct a RunLengthEncodedArray from values and run ends arrays
  ///
  /// The data type is automatically inferred from the arguments.
  /// The run_ends_array and values_array must be the same length.
  static Result<std::shared_ptr<RunLengthEncodedArray>> Make(
      std::shared_ptr<Array>& values_array, std::shared_ptr<Array>& run_ends_array,
      int64_t logical_length, int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  /// \brief Returns an array holding the values of each run. This function does apply
  /// neiher the physical offset to the array
  std::shared_ptr<Array> values_array() const;

  /// \brief Returns the buffer holding the logical indexes of each run end. This function
  /// does apply neiher the physical offset to the start of the buffer nor the logical
  /// offset to the values.
  std::shared_ptr<Buffer> run_ends_buffer() const;

  /// \brief Returns a pointer to logical indexes of each run end. This function does
  /// apply neiher the physical offset to the start of the buffer nor the logical offset
  /// to the values. This function does only work if the run ends are stored in a CPU
  /// buffer.
  const int32_t* run_ends() const;
};

/// @}

}  // namespace arrow
