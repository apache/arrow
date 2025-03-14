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

#include <arrow/array.h>
#include <arrow/scalar.h>
#include <odbcabstraction/diagnostics.h>
#include <odbcabstraction/types.h>
#include <algorithm>
#include <cstdint>
#include "types.h"

namespace driver {
namespace flight_sql {

using namespace arrow;
using namespace odbcabstraction;

template <typename ARRAY_TYPE>
inline size_t CopyFromArrayValuesToBinding(ARRAY_TYPE* array, ColumnBinding* binding,
                                           int64_t starting_row, int64_t cells) {
  constexpr ssize_t element_size = sizeof(typename ARRAY_TYPE::value_type);

  if (binding->strlen_buffer) {
    for (int64_t i = 0; i < cells; ++i) {
      int64_t current_row = starting_row + i;
      if (array->IsNull(current_row)) {
        binding->strlen_buffer[i] = NULL_DATA;
      } else {
        binding->strlen_buffer[i] = element_size;
      }
    }
  } else {
    // Duplicate this loop to avoid null checks within the loop.
    for (int64_t i = starting_row; i < starting_row + cells; ++i) {
      if (array->IsNull(i)) {
        throw odbcabstraction::NullWithoutIndicatorException();
      }
    }
  }

  // Copy the entire array to the bound ODBC buffers.
  // Note that the array should already have been sliced down to the same number
  // of elements in the ODBC data array by the point in which this function is called.
  const auto* values = array->raw_values();
  memcpy(binding->buffer, &values[starting_row], element_size * cells);

  return cells;
}

}  // namespace flight_sql
}  // namespace driver
