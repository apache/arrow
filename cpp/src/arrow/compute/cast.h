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

#ifndef ARROW_COMPUTE_CAST_H
#define ARROW_COMPUTE_CAST_H

#include <memory>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;

namespace compute {

class FunctionContext;
class UnaryKernel;

struct CastOptions {
  CastOptions() : allow_int_overflow(false) {}

  bool allow_int_overflow;
};

/// \since 0.7.0
/// \note API not yet finalized
ARROW_EXPORT
Status GetCastFunction(const DataType& in_type, const std::shared_ptr<DataType>& to_type,
                       const CastOptions& options, std::unique_ptr<UnaryKernel>* kernel);

/// \brief Cast from one array type to another
/// \param[in] context the FunctionContext
/// \param[in] array array to cast
/// \param[in] to_type type to cast to
/// \param[in] options casting options
/// \param[out] out resulting array
///
/// \since 0.7.0
/// \note API not yet finalized
ARROW_EXPORT
Status Cast(FunctionContext* context, const Array& array,
            const std::shared_ptr<DataType>& to_type, const CastOptions& options,
            std::shared_ptr<Array>* out);

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_CAST_H
