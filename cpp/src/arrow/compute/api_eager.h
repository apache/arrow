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

#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/filter.h"
#include "arrow/compute/options.h"
#include "arrow/compute/take.h"
#include "arrow/datum.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {

class ExecContext;

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

/// \brief Returns indices that partition an array around n-th
/// sorted element.
///
/// Find index of n-th(0 based) smallest value and perform indirect
/// partition of an array around that element. Output indices[0 ~ n-1]
/// holds values no greater than n-th element, and indices[n+1 ~ end]
/// holds values no less than n-th element. Elements in each partition
/// is not sorted. Nulls will be partitioned to the end of the output.
/// Output is not guaranteed to be stable.
///
/// \param[in] values array to be partitioned
/// \param[in] n pivot array around sorted n-th element
/// \param[in] ctx the function execution context, optional
/// \return offsets indices that would partition an array
ARROW_EXPORT
Result<std::shared_ptr<Array>> NthToIndices(const Array& values, int64_t n,
                                            ExecContext* ctx = NULLPTR);

/// \brief Returns the indices that would sort an array.
///
/// Perform an indirect sort of array. The output array will contain
/// indices that would sort an array, which would be the same length
/// as input. Nulls will be stably partitioned to the end of the output.
///
/// For example given values = [null, 1, 3.3, null, 2, 5.3], the output
/// will be [1, 4, 2, 5, 0, 3]
///
/// \param[in] values array to sort
/// \param[in] ctx the function execution context, optional
/// \return offsets indices that would sort an array
ARROW_EXPORT
Result<std::shared_ptr<Array>> SortToIndices(const Array& values,
                                             ExecContext* ctx = NULLPTR);

/// \brief Compute unique elements from an array-like object
///
/// Note if a null occurs in the input it will NOT be included in the output.
///
/// \param[in] datum array-like input
/// \param[in] ctx the function execution context, optional
/// \return result as Array
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<std::shared_ptr<Array>> Unique(const Datum& datum, ExecContext* ctx = NULLPTR);

// Constants for accessing the output of ValueCounts
ARROW_EXPORT extern const char kValuesFieldName[];
ARROW_EXPORT extern const char kCountsFieldName[];
ARROW_EXPORT extern const int32_t kValuesFieldIndex;
ARROW_EXPORT extern const int32_t kCountsFieldIndex;
/// \brief Return counts of unique elements from an array-like object.
///
/// Note that the counts do not include counts for nulls in the array.  These can be
/// obtained separately from metadata.
///
/// For floating point arrays there is no attempt to normalize -0.0, 0.0 and NaN values
/// which can lead to unexpected results if the input Array has these values.
///
/// \param[in] value array-like input
/// \param[in] ctx the function execution context, optional
/// \return counts An array of  <input type "Values", int64_t "Counts"> structs.
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<std::shared_ptr<Array>> ValueCounts(const Datum& value,
                                           ExecContext* ctx = NULLPTR);

/// \brief Dictionary-encode values in an array-like object
/// \param[in] data array-like input
/// \param[in] ctx the function execution context, optional
/// \return result with same shape and type as input
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> DictionaryEncode(const Datum& data, ExecContext* ctx = NULLPTR);

// ----------------------------------------------------------------------
// Aggregate functions

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
