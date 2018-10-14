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

/// \brief Invoke hash table kernel on input array, returning any output
/// values. Implementations should be thread-safe
class ARROW_EXPORT HashKernel : public UnaryKernel {
 public:
  virtual Status Append(FunctionContext* ctx, const ArrayData& input) = 0;
  virtual Status Flush(Datum* out) = 0;
  virtual Status GetDictionary(std::shared_ptr<ArrayData>* out) = 0;
};

/// \since 0.8.0
/// \note API not yet finalized
ARROW_EXPORT
Status GetUniqueKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                       std::unique_ptr<HashKernel>* kernel);

ARROW_EXPORT
Status GetDictionaryEncodeKernel(FunctionContext* ctx,
                                 const std::shared_ptr<DataType>& type,
                                 std::unique_ptr<HashKernel>* kernel);

/// \brief Compute unique elements from an array-like object
/// \param[in] context the FunctionContext
/// \param[in] datum array-like input
/// \param[out] out result as Array
///
/// \since 0.8.0
/// \note API not yet finalized
ARROW_EXPORT
Status Unique(FunctionContext* context, const Datum& datum, std::shared_ptr<Array>* out);

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

// ARROW_EXPORT
// Status CountValues(FunctionContext* context, const Datum& values,
//                    std::shared_ptr<Array>* out_uniques,
//                    std::shared_ptr<Array>* out_counts);

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_HASH_H
