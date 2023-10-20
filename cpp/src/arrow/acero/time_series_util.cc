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

<<<<<<< HEAD
=======

>>>>>>> b34c999b6 (Create sorted merge node)
#include "arrow/array/data.h"

#include "arrow/acero/time_series_util.h"
#include "arrow/util/logging.h"

namespace arrow::acero {

// normalize the value to 64-bits while preserving ordering of values
template <typename T, enable_if_t<std::is_integral<T>::value, bool>>
<<<<<<< HEAD
inline uint64_t NormalizeTime(T t) {
=======
static inline uint64_t get_time_normalized(T t) {
>>>>>>> b34c999b6 (Create sorted merge node)
  uint64_t bias = std::is_signed<T>::value ? (uint64_t)1 << (8 * sizeof(T) - 1) : 0;
  return t < 0 ? static_cast<uint64_t>(t + bias) : static_cast<uint64_t>(t);
}

<<<<<<< HEAD
uint64_t GetTime(const RecordBatch* batch, Type::type time_type, int col, uint64_t row) {
=======
uint64_t get_time(const RecordBatch* batch, Type::type time_type, int col, uint64_t row) {
>>>>>>> b34c999b6 (Create sorted merge node)
#define LATEST_VAL_CASE(id, val)                     \
  case Type::id: {                                   \
    using T = typename TypeIdTraits<Type::id>::Type; \
    using CType = typename TypeTraits<T>::CType;     \
    return val(data->GetValues<CType>(1)[row]);      \
  }

  auto data = batch->column_data(col);
  switch (time_type) {
<<<<<<< HEAD
    LATEST_VAL_CASE(INT8, NormalizeTime)
    LATEST_VAL_CASE(INT16, NormalizeTime)
    LATEST_VAL_CASE(INT32, NormalizeTime)
    LATEST_VAL_CASE(INT64, NormalizeTime)
    LATEST_VAL_CASE(UINT8, NormalizeTime)
    LATEST_VAL_CASE(UINT16, NormalizeTime)
    LATEST_VAL_CASE(UINT32, NormalizeTime)
    LATEST_VAL_CASE(UINT64, NormalizeTime)
    LATEST_VAL_CASE(DATE32, NormalizeTime)
    LATEST_VAL_CASE(DATE64, NormalizeTime)
    LATEST_VAL_CASE(TIME32, NormalizeTime)
    LATEST_VAL_CASE(TIME64, NormalizeTime)
    LATEST_VAL_CASE(TIMESTAMP, NormalizeTime)
=======
    LATEST_VAL_CASE(INT8, get_time_normalized)
    LATEST_VAL_CASE(INT16, get_time_normalized)
    LATEST_VAL_CASE(INT32, get_time_normalized)
    LATEST_VAL_CASE(INT64, get_time_normalized)
    LATEST_VAL_CASE(UINT8, get_time_normalized)
    LATEST_VAL_CASE(UINT16, get_time_normalized)
    LATEST_VAL_CASE(UINT32, get_time_normalized)
    LATEST_VAL_CASE(UINT64, get_time_normalized)
    LATEST_VAL_CASE(DATE32, get_time_normalized)
    LATEST_VAL_CASE(DATE64, get_time_normalized)
    LATEST_VAL_CASE(TIME32, get_time_normalized)
    LATEST_VAL_CASE(TIME64, get_time_normalized)
    LATEST_VAL_CASE(TIMESTAMP, get_time_normalized)
>>>>>>> b34c999b6 (Create sorted merge node)
    default:
      DCHECK(false);
      return 0;  // cannot happen
  }

#undef LATEST_VAL_CASE
}

}  // namespace arrow::acero
