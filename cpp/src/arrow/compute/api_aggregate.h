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

// Eager evaluation convenience APIs for invoking common functions, including
// necessary memory allocations

#pragma once

#include "arrow/compute/function.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

namespace compute {

class ExecContext;

// ----------------------------------------------------------------------
// Aggregate functions

/// \class CountOptions
///
/// The user control the Count kernel behavior with this class. By default, the
/// it will count all non-null values.
struct ARROW_EXPORT CountOptions : public FunctionOptions {
  enum mode {
    // Count all non-null values.
    COUNT_ALL = 0,
    // Count all null values.
    COUNT_NULL,
  };

  explicit CountOptions(enum mode count_mode) : count_mode(count_mode) {}

  static CountOptions Defaults() { return CountOptions(COUNT_ALL); }

  enum mode count_mode = COUNT_ALL;
};

/// \brief Count non-null (or null) values in an array.
///
/// \param[in] options counting options, see CountOptions for more information
/// \param[in] datum to count
/// \param[in] ctx the function execution context, optional
/// \return out resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Count(const Datum& datum, CountOptions options = CountOptions::Defaults(),
                    ExecContext* ctx = NULLPTR);

/// \brief Compute the mean of a numeric array.
///
/// \param[in] value datum to compute the mean, expecting Array
/// \param[in] ctx the function execution context, optional
/// \return datum of the computed mean as a DoubleScalar
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Mean(const Datum& value, ExecContext* ctx = NULLPTR);

/// \brief Sum values of a numeric array.
///
/// \param[in] value datum to sum, expecting Array or ChunkedArray
/// \param[in] ctx the function execution context, optional
/// \return datum of the computed sum as a Scalar
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Sum(const Datum& value, ExecContext* ctx = NULLPTR);

/// \class MinMaxOptions
///
/// The user can control the MinMax kernel behavior with this class. By default,
/// it will skip null if there is a null value present.
struct ARROW_EXPORT MinMaxOptions : public FunctionOptions {
  enum mode {
    /// skip null values
    SKIP = 0,
    /// any nulls will result in null output
    OUTPUT_NULL
  };

  explicit MinMaxOptions(enum mode null_handling = SKIP) : null_handling(null_handling) {}

  static MinMaxOptions Defaults() { return MinMaxOptions{}; }

  enum mode null_handling = SKIP;
};

/// \brief Calculate the min / max of a numeric array
///
/// This function returns both the min and max as a struct scalar, with type
/// struct<min: T, max: T>, where T is ht einput type
///
/// \param[in] value input datum, expecting Array or ChunkedArray
/// \param[in] options see MinMaxOptions for more information
/// \param[in] ctx the function execution context, optional
/// \return resulting datum as a struct<min: T, max: T> scalar
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> MinMax(const Datum& value,
                     const MinMaxOptions& options = MinMaxOptions::Defaults(),
                     ExecContext* ctx = NULLPTR);

/// \brief Calculate the min / max of a numeric array.
///
/// This function returns both the min and max as a collection. The resulting
/// datum thus consists of two scalar datums: {Datum(min), Datum(max)}
///
/// \param[in] array input array
/// \param[in] options see MinMaxOptions for more information
/// \param[in] ctx the function execution context, optional
/// \return resulting datum containing a {min, max} collection
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> MinMax(const Array& array,
                     const MinMaxOptions& options = MinMaxOptions::Defaults(),
                     ExecContext* ctx = NULLPTR);

}  // namespace compute
}  // namespace arrow
