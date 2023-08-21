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

#include "arrow/type_traits.h"

#include "arrow/util/logging.h"

namespace arrow {

int RequiredValueAlignmentForBuffer(Type::type type_id, int buffer_index) {
  if (buffer_index == 2 && type_id == Type::DENSE_UNION) {
    // A dense union array is the only array (so far) that requires alignment
    // on a buffer with a buffer_index that is not equal to 1
    return 4;
  }
  if (buffer_index != 1) {
    // If the buffer index is 0 then either:
    //  * The array type has no buffers, thus this shouldn't be called anyways
    //  * The array has a validity buffer at 0, no alignment needed
    //  * The array is a union array and has a types buffer at 0, no alignment needed
    // If the buffer index is > 1 then, in all current cases, it represents binary
    //  data and no alignment is needed.  The only exception is dense union buffers
    //  which are checked above.
    return 1;
  }
  DCHECK_NE(type_id, Type::DICTIONARY);
  DCHECK_NE(type_id, Type::EXTENSION);

  switch (type_id) {
    case Type::NA:                 // No buffers
    case Type::FIXED_SIZE_LIST:    // No second buffer (values in child array)
    case Type::FIXED_SIZE_BINARY:  // Fixed size binary could be dangerous but the
                                   // compute kernels don't type pun this.  E.g. if
                                   // an extension type is storing some kind of struct
                                   // here then the user should do their own alignment
                                   // check before casting to an array of structs
    case Type::BOOL:               // Always treated as uint8_t*
    case Type::INT8:               // Always treated as uint8_t*
    case Type::UINT8:              // Always treated as uint8_t*
    case Type::DENSE_UNION:        // Union arrays have a uint8_t* types buffer here
    case Type::SPARSE_UNION:       // Union arrays have a uint8_t* types buffer here
    case Type::RUN_END_ENCODED:    // No buffers
    case Type::STRUCT:             // No second buffer
      return 1;
    case Type::INT16:
    case Type::UINT16:
    case Type::HALF_FLOAT:
      return 2;
    case Type::INT32:
    case Type::UINT32:
    case Type::FLOAT:
    case Type::STRING:  // Offsets may be cast to int32_t*
    case Type::BINARY:  // Offsets may be cast to int32_t*
    case Type::DATE32:
    case Type::TIME32:
    case Type::LIST:  // Offsets may be cast to int32_t*, data is in child array
    case Type::MAP:   // This is a list array
    case Type::INTERVAL_MONTHS:    // Stored as int32_t*
    case Type::INTERVAL_DAY_TIME:  // Stored as two contiguous 32-bit integers
      return 4;
    case Type::INT64:
    case Type::UINT64:
    case Type::DOUBLE:
    case Type::DECIMAL128:    // May be cast to GenericBasicDecimal* which requires
                              // alignment of 8
    case Type::DECIMAL256:    // May be cast to GenericBasicDecimal* which requires
                              // alignment of 8
    case Type::LARGE_BINARY:  // Offsets may be cast to int64_t*
    case Type::LARGE_LIST:    // Offsets may be cast to int64_t*
    case Type::LARGE_STRING:  // Offsets may be cast to int64_t*
    case Type::DATE64:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::DURATION:
    case Type::INTERVAL_MONTH_DAY_NANO:  // Stored as two 32-bit integers and a 64-bit
                                         // integer
      return 8;
    case Type::DICTIONARY:
    case Type::EXTENSION:
    case Type::MAX_ID:
      break;
  }
  Status::Invalid("RequiredValueAlignmentForBuffer called with invalid type id ", type_id)
      .Warn();
  return 1;
}

}  // namespace arrow
