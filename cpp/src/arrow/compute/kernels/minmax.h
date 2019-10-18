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

#include <memory>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;
class Status;

namespace compute {

struct Datum;
class FunctionContext;
class AggregateFunction;

/// \class MinMaxOptions
///
/// The user can control the MinMax kernel behavior with this class. By default,
/// it will return null if there is a null value present.
struct ARROW_EXPORT MinMaxOptions {
  //   MinMaxOptions() : skip_nulls(false) {}

  //   bool skip_nulls;
};

/// \brief Return a Min/Max Kernel
///
/// \param[in] type required to specialize the kernel
/// \param[in] context the FunctionContext
/// \param[in] options, see MinMaxOptions for more information
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
std::shared_ptr<AggregateFunction> MakeMinMaxAggregateFunction(
    const DataType& type, FunctionContext* ctx, const MinMaxOptions& options);

/// \brief Calculate the min / max of a numeric array
///
/// \param[in] context the FunctionContext
/// \param[in] options, see MinMaxOptions for more information
/// \param[in] input value datum, expecting Array or ChunkedArray
/// \param[out] out resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Status MinMax(FunctionContext* context, const MinMaxOptions& options, const Datum& value,
              Datum* min);

/// \brief Calculate the min / max of a numeric array.
///
/// \param[in] context the FunctionContext
/// \param[in] options, see MinMaxOptions for more information
/// \param[in] input array
/// \param[out] out resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Status MinMax(FunctionContext* context, const MinMaxOptions& options, const Array& array,
              Datum* min);

}  // namespace compute
}  // namespace arrow
