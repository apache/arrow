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

#include <memory>

#include "arrow/compute/exec.h"
#include "arrow/compute/options.h"
#include "arrow/datum.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {

class ExecContext;

// ----------------------------------------------------------------------
// Convenience invocation APIs for a number of kernels

/// \brief Cast from one array type to another
/// \param[in] value array to cast
/// \param[in] to_type type to cast to
/// \param[in] options casting options
/// \param[in] context the function execution context, optional
/// \return the resulting array
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<std::shared_ptr<Array>> Cast(const Array& value, std::shared_ptr<DataType> to_type,
                                    const CastOptions& options = CastOptions::Safe(),
                                    ExecContext* context = NULLPTR);

/// \brief Cast from one value to another
/// \param[in] value datum to cast
/// \param[in] to_type type to cast to
/// \param[in] options casting options
/// \param[in] context the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Cast(const Datum& value, std::shared_ptr<DataType> to_type,
                   const CastOptions& options = CastOptions::Safe(),
                   ExecContext* context = NULLPTR);

/// \brief Return true if a cast function is defined
ARROW_EXPORT
bool CanCast(const DataType& from_type, const DataType& to_type);

// ----------------------------------------------------------------------

/// \brief Add two values together. Array values must be the same length. If a
/// value is null in either addend, the result is null
///
/// \param[in] left the first value
/// \param[in] right the second value
/// \param[in] ctx the function execution context, optional
/// \return the elementwise addition of the values
ARROW_EXPORT
Result<Datum> Add(const Datum& left, const Datum& right, ExecContext* ctx = NULLPTR);

/// \brief Compare a numeric array with a scalar.
///
/// \param[in] left datum to compare, must be an Array
/// \param[in] right datum to compare, must be a Scalar of the same type than
///            left Datum.
/// \param[in] options compare options
/// \param[in] ctx the function execution context, optional
/// \return resulting datum
///
/// Note on floating point arrays, this uses ieee-754 compare semantics.
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Compare(const Datum& left, const Datum& right,
                      struct CompareOptions options, ExecContext* ctx = NULLPTR);

/// \brief Invert the values of a boolean datum
/// \param[in] value datum to invert
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Invert(const Datum& value, ExecContext* ctx = NULLPTR);

/// \brief Element-wise AND of two boolean datums which always propagates nulls
/// (null and false is null).
///
/// \param[in] left left operand (array)
/// \param[in] right right operand (array)
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> And(const Datum& left, const Datum& right, ExecContext* ctx = NULLPTR);

/// \brief Element-wise AND of two boolean datums with a Kleene truth table
/// (null and false is false).
///
/// \param[in] left left operand (array)
/// \param[in] right right operand (array)
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> KleeneAnd(const Datum& left, const Datum& right,
                        ExecContext* ctx = NULLPTR);

/// \brief Element-wise OR of two boolean datums which always propagates nulls
/// (null and true is null).
///
/// \param[in] left left operand (array)
/// \param[in] right right operand (array)
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Or(const Datum& left, const Datum& right, ExecContext* ctx = NULLPTR);

/// \brief Element-wise OR of two boolean datums with a Kleene truth table
/// (null or true is true).
///
/// \param[in] left left operand (array)
/// \param[in] right right operand (array)
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> KleeneOr(const Datum& left, const Datum& right, ExecContext* ctx = NULLPTR);

/// \brief Element-wise XOR of two boolean datums
/// \param[in] left left operand (array)
/// \param[in] right right operand (array)
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Xor(const Datum& left, const Datum& right, ExecContext* ctx = NULLPTR);

/// \brief IsIn returns true for each element of `values` that is contained in
/// `value_set`
///
/// If null occurs in left, if null count in right is not 0,
/// it returns true, else returns null.
///
/// \param[in] values array-like input to look up in value_set
/// \param[in] value_set Array input
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> IsIn(const Datum& values, std::shared_ptr<Array> value_set,
                   ExecContext* ctx = NULLPTR);

/// \brief Match examines each slot in the haystack against a needles array.
/// If the value is not found in needles, null will be output.
/// If found, the index of occurrence within needles (ignoring duplicates)
/// will be output.
///
/// For example given haystack = [99, 42, 3, null] and
/// needles = [3, 3, 99], the output will be = [1, null, 0, null]
///
/// Note: Null in the haystack is considered to match
/// a null in the needles array. For example given
/// haystack = [99, 42, 3, null] and needles = [3, 99, null],
/// the output will be = [1, null, 0, 2]
///
/// \param[in] haystack array-like input
/// \param[in] needles Array input
/// \param[in] ctx the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Match(const Datum& haystack, std::shared_ptr<Array> needles,
                    ExecContext* ctx = NULLPTR);

}  // namespace compute
}  // namespace arrow
