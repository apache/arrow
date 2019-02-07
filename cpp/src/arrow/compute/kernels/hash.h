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

#ifndef ARROW_COMPUTE_KERNELS_HASH_H
#define ARROW_COMPUTE_KERNELS_HASH_H

#include <memory>

#include "arrow/compute/kernel.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;
struct ArrayData;

namespace compute {

class FunctionContext;

/// \brief Compute unique elements from an array-like object
///
/// Note if a null occurs in the input it will NOT be included in the output.
///
/// \param[in] context the FunctionContext
/// \param[in] datum array-like input
/// \param[out] out result as Array
///
/// \since 0.8.0
/// \note API not yet finalized
ARROW_EXPORT
Status Unique(FunctionContext* context, const Datum& datum, std::shared_ptr<Array>* out);

/// \brief Return counts of unique elements from an array-like object.
///
/// Note that the counts do not include counts for nulls in the array.  These can be
/// obtained separately from metadata.
///
/// For floating point arrays there is no attempt to normalize -0.0, 0.0 and NaN values
/// which can lead to unexpected results if the input Array has these values.
///
/// \param[in] context the FunctionContext
/// \param[in] value array-like input
/// \param[out] out_uniques unique elements as Array
/// \param[out] out_counts counts per element as Array, same shape as out_uniques
///
/// \since 0.13.0
/// \note API not yet finalized
ARROW_EXPORT
Status CountValues(FunctionContext* context, const Datum& value,
                   std::shared_ptr<Array>* out_uniques,
                   std::shared_ptr<Array>* out_counts);

/// \brief Dictionary-encode values in an array-like object
/// \param[in] context the FunctionContext
/// \param[in] data array-like input
/// \param[out] out result with same shape and type as input
///
/// \since 0.8.0
/// \note API not yet finalized
ARROW_EXPORT
Status DictionaryEncode(FunctionContext* context, const Datum& data, Datum* out);

// TODO(wesm): Define API for incremental dictionary encoding

// TODO(wesm): Define API for regularizing DictionaryArray objects with
// different dictionaries

// class DictionaryEncoder {
//  public:
//   virtual Encode(const Datum& data, Datum* out) = 0;
// };

//
// ARROW_EXPORT
// Status DictionaryEncode(FunctionContext* context, const Datum& data,
//                         const Array& prior_dictionary, Datum* out);

// TODO(wesm): Implement these next
// ARROW_EXPORT
// Status Match(FunctionContext* context, const Datum& values, const Datum& member_set,
//              Datum* out);

// ARROW_EXPORT
// Status IsIn(FunctionContext* context, const Datum& values, const Datum& member_set,
//             Datum* out);

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_HASH_H
