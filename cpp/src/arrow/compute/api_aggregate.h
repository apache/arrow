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

/// \addtogroup compute-concrete-options
/// @{

/// \brief Control Count kernel behavior
///
/// By default, all non-null values are counted.
struct ARROW_EXPORT CountOptions : public FunctionOptions {
  enum Mode {
    /// Count all non-null values.
    COUNT_NON_NULL = 0,
    /// Count all null values.
    COUNT_NULL,
  };

  explicit CountOptions(enum Mode count_mode = COUNT_NON_NULL) : count_mode(count_mode) {}

  static CountOptions Defaults() { return CountOptions(COUNT_NON_NULL); }

  enum Mode count_mode;
};

/// \brief Control MinMax kernel behavior
///
/// By default, null values are ignored
struct ARROW_EXPORT MinMaxOptions : public FunctionOptions {
  enum Mode {
    /// Skip null values
    SKIP = 0,
    /// Any nulls will result in null output
    EMIT_NULL
  };

  explicit MinMaxOptions(enum Mode null_handling = SKIP) : null_handling(null_handling) {}

  static MinMaxOptions Defaults() { return MinMaxOptions{}; }

  enum Mode null_handling;
};

/// \brief Control Mode kernel behavior
///
/// Returns top-n common values and counts.
/// By default, returns the most common value and count.
struct ARROW_EXPORT ModeOptions : public FunctionOptions {
  explicit ModeOptions(int64_t n = 1) : n(n) {}

  static ModeOptions Defaults() { return ModeOptions{}; }

  int64_t n = 1;
};

/// \brief Control Delta Degrees of Freedom (ddof) of Variance and Stddev kernel
///
/// The divisor used in calculations is N - ddof, where N is the number of elements.
/// By default, ddof is zero, and population variance or stddev is returned.
struct ARROW_EXPORT VarianceOptions : public FunctionOptions {
  explicit VarianceOptions(int ddof = 0) : ddof(ddof) {}

  static VarianceOptions Defaults() { return VarianceOptions{}; }

  int ddof = 0;
};

/// \brief Control Quantile kernel behavior
///
/// By default, returns the median value.
struct ARROW_EXPORT QuantileOptions : public FunctionOptions {
  /// Interpolation method to use when quantile lies between two data points
  enum Interpolation {
    LINEAR = 0,
    LOWER,
    HIGHER,
    NEAREST,
    MIDPOINT,
  };

  explicit QuantileOptions(double q = 0.5, enum Interpolation interpolation = LINEAR)
      : q{q}, interpolation{interpolation} {}

  explicit QuantileOptions(std::vector<double> q,
                           enum Interpolation interpolation = LINEAR)
      : q{std::move(q)}, interpolation{interpolation} {}

  static QuantileOptions Defaults() { return QuantileOptions{}; }

  /// quantile must be between 0 and 1 inclusive
  std::vector<double> q;
  enum Interpolation interpolation;
};

/// \brief Control TDigest approximate quantile kernel behavior
///
/// By default, returns the median value.
struct ARROW_EXPORT TDigestOptions : public FunctionOptions {
  explicit TDigestOptions(double q = 0.5, uint32_t delta = 100,
                          uint32_t buffer_size = 500)
      : q{q}, delta{delta}, buffer_size{buffer_size} {}

  explicit TDigestOptions(std::vector<double> q, uint32_t delta = 100,
                          uint32_t buffer_size = 500)
      : q{std::move(q)}, delta{delta}, buffer_size{buffer_size} {}

  static TDigestOptions Defaults() { return TDigestOptions{}; }

  /// quantile must be between 0 and 1 inclusive
  std::vector<double> q;
  /// compression parameter, default 100
  uint32_t delta;
  /// input buffer size, default 500
  uint32_t buffer_size;
};

/// @}

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
/// struct<min: T, max: T>, where T is the input type
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

/// \brief Test whether any element in a boolean array evaluates to true.
///
/// This function returns true if any of the elements in the array evaluates
/// to true and false otherwise. Null values are skipped.
///
/// \param[in] value input datum, expecting a boolean array
/// \param[in] ctx the function execution context, optional
/// \return resulting datum as a BooleanScalar
///
/// \since 3.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Any(const Datum& value, ExecContext* ctx = NULLPTR);

/// \brief Test whether all elements in a boolean array evaluate to true.
///
/// This function returns true if all of the elements in the array evaluate
/// to true and false otherwise. Null values are skipped.
///
/// \param[in] value input datum, expecting a boolean array
/// \param[in] ctx the function execution context, optional
/// \return resulting datum as a BooleanScalar

/// \since 3.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> All(const Datum& value, ExecContext* ctx = NULLPTR);

/// \brief Calculate the modal (most common) value of a numeric array
///
/// This function returns top-n most common values and number of times they occur as
/// an array of `struct<mode: T, count: int64>`, where T is the input type.
/// Values with larger counts are returned before smaller ones.
/// If there are more than one values with same count, smaller value is returned first.
///
/// \param[in] value input datum, expecting Array or ChunkedArray
/// \param[in] options see ModeOptions for more information
/// \param[in] ctx the function execution context, optional
/// \return resulting datum as an array of struct<mode: T, count: int64>
///
/// \since 2.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Mode(const Datum& value,
                   const ModeOptions& options = ModeOptions::Defaults(),
                   ExecContext* ctx = NULLPTR);

/// \brief Calculate the standard deviation of a numeric array
///
/// \param[in] value input datum, expecting Array or ChunkedArray
/// \param[in] options see VarianceOptions for more information
/// \param[in] ctx the function execution context, optional
/// \return datum of the computed standard deviation as a DoubleScalar
///
/// \since 2.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Stddev(const Datum& value,
                     const VarianceOptions& options = VarianceOptions::Defaults(),
                     ExecContext* ctx = NULLPTR);

/// \brief Calculate the variance of a numeric array
///
/// \param[in] value input datum, expecting Array or ChunkedArray
/// \param[in] options see VarianceOptions for more information
/// \param[in] ctx the function execution context, optional
/// \return datum of the computed variance as a DoubleScalar
///
/// \since 2.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Variance(const Datum& value,
                       const VarianceOptions& options = VarianceOptions::Defaults(),
                       ExecContext* ctx = NULLPTR);

/// \brief Calculate the quantiles of a numeric array
///
/// \param[in] value input datum, expecting Array or ChunkedArray
/// \param[in] options see QuantileOptions for more information
/// \param[in] ctx the function execution context, optional
/// \return resulting datum as an array
///
/// \since 4.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Quantile(const Datum& value,
                       const QuantileOptions& options = QuantileOptions::Defaults(),
                       ExecContext* ctx = NULLPTR);

/// \brief Calculate the approximate quantiles of a numeric array with T-Digest algorithm
///
/// \param[in] value input datum, expecting Array or ChunkedArray
/// \param[in] options see TDigestOptions for more information
/// \param[in] ctx the function execution context, optional
/// \return resulting datum as an array
///
/// \since 4.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> TDigest(const Datum& value,
                      const TDigestOptions& options = TDigestOptions::Defaults(),
                      ExecContext* ctx = NULLPTR);

namespace internal {

/// Internal use only: streaming group identifier.
/// Consumes batches of keys and yields batches of the group ids.
class ARROW_EXPORT Grouper {
 public:
  virtual ~Grouper() = default;

  /// Construct a Grouper which receives the specified key types
  static Result<std::unique_ptr<Grouper>> Make(const std::vector<ValueDescr>& descrs,
                                               ExecContext* ctx = default_exec_context());

  /// Consume a batch of keys, producing the corresponding group ids as an integer array.
  /// Currently only uint32 indices will be produced, eventually the bit width will only
  /// be as wide as necessary.
  virtual Result<Datum> Consume(const ExecBatch& batch) = 0;

  /// Get current unique keys. May be called multiple times.
  virtual Result<ExecBatch> GetUniques() = 0;

  /// Get the current number of groups.
  virtual uint32_t num_groups() const = 0;

  /// \brief Assemble lists of indices of identical elements.
  ///
  /// \param[in] ids An unsigned, all-valid integral array which will be
  ///                used as grouping criteria.
  /// \param[in] num_groups An upper bound for the elements of ids
  /// \return A num_groups-long ListArray where the slot at i contains a
  ///         list of indices where i appears in ids.
  ///
  ///   MakeGroupings([
  ///       2,
  ///       2,
  ///       5,
  ///       5,
  ///       2,
  ///       3
  ///   ], 8) == [
  ///       [],
  ///       [],
  ///       [0, 1, 4],
  ///       [5],
  ///       [],
  ///       [2, 3],
  ///       [],
  ///       []
  ///   ]
  static Result<std::shared_ptr<ListArray>> MakeGroupings(
      const UInt32Array& ids, uint32_t num_groups,
      ExecContext* ctx = default_exec_context());

  /// \brief Produce a ListArray whose slots are selections of `array` which correspond to
  /// the provided groupings.
  ///
  /// For example,
  ///   ApplyGroupings([
  ///       [],
  ///       [],
  ///       [0, 1, 4],
  ///       [5],
  ///       [],
  ///       [2, 3],
  ///       [],
  ///       []
  ///   ], [2, 2, 5, 5, 2, 3]) == [
  ///       [],
  ///       [],
  ///       [2, 2, 2],
  ///       [3],
  ///       [],
  ///       [5, 5],
  ///       [],
  ///       []
  ///   ]
  static Result<std::shared_ptr<ListArray>> ApplyGroupings(
      const ListArray& groupings, const Array& array,
      ExecContext* ctx = default_exec_context());
};

/// \brief Configure a grouped aggregation
struct ARROW_EXPORT Aggregate {
  /// the name of the aggregation function
  std::string function;

  /// options for the aggregation function
  const FunctionOptions* options;
};

/// Internal use only: helper function for testing HashAggregateKernels.
/// This will be replaced by streaming execution operators.
ARROW_EXPORT
Result<Datum> GroupBy(const std::vector<Datum>& arguments, const std::vector<Datum>& keys,
                      const std::vector<Aggregate>& aggregates,
                      ExecContext* ctx = default_exec_context());

}  // namespace internal
}  // namespace compute
}  // namespace arrow
