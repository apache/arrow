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

#include <cmath>
#include <cstdint>
#include <memory>

#include "parquet/platform.h"
#include "parquet/types.h"

namespace parquet {

/// \brief Structure represented encoded statistics to be written to and read from Parquet
/// serialized metadata.
///
/// See the Parquet Thrift definition and GeoStatistics for the specific definition
/// of field values.
class PARQUET_EXPORT EncodedGeoStatistics {
 public:
  static constexpr double kInf = std::numeric_limits<double>::infinity();

  double xmin{kInf};
  double xmax{-kInf};
  double ymin{kInf};
  double ymax{-kInf};
  double zmin{kInf};
  double zmax{-kInf};
  double mmin{kInf};
  double mmax{-kInf};
  std::vector<int32_t> geospatial_types;

  bool has_x() const { return !std::isinf(xmin - xmax); }
  bool has_y() const { return !std::isinf(ymin - ymax); }
  bool has_z() const { return !std::isinf(zmin - zmax); }
  bool has_m() const { return !std::isinf(mmin - mmax); }

  bool is_set() const {
    return !geospatial_types.empty() || (has_x() && has_y());
  }
};

class GeoStatisticsImpl;

/// \brief Base type for computing geospatial column statistics while writing a file
/// or representing them when reading a file
///
/// EXPERIMENTAL
class PARQUET_EXPORT GeoStatistics {
 public:
  GeoStatistics();
  explicit GeoStatistics(const EncodedGeoStatistics& encoded);

  ~GeoStatistics();

  /// \brief Return true if bounds, geometry types, and validity are identical
  bool Equals(const GeoStatistics& other) const;

  /// \brief Update these statistics based on previously calculated or decoded statistics
  void Merge(const GeoStatistics& other);

  /// \brief Update these statistics based on values
  void Update(const ByteArray* values, int64_t num_values, int64_t null_count);

  /// \brief Update these statistics based on the non-null elements of values
  void UpdateSpaced(const ByteArray* values, const uint8_t* valid_bits,
                    int64_t valid_bits_offset, int64_t num_spaced_values,
                    int64_t num_values, int64_t null_count);

  /// \brief Update these statistics based on the non-null elements of values
  ///
  /// Currently, BinaryArray and LargeBinaryArray input is supported.
  void Update(const ::arrow::Array& values);

  /// \brief Return these statistics to an empty state
  void Reset();

  /// \brief Encode the statistics for serializing to Thrift
  ///
  /// If invalid WKB was encountered, empty encoded statistics are returned
  /// (such that is_set() returns false and they should not be written).
  EncodedGeoStatistics Encode() const;

  /// \brief Returns true if all WKB encountered was valid or false otherwise
  bool is_valid() const;

  /// \brief Reset existing statistics and populate them from previously-encoded ones
  void Decode(const EncodedGeoStatistics& encoded);

  /// \brief The minimum encountered value in the X dimension, or Inf if no X values were
  /// encountered.
  ///
  /// The Parquet definition allows for "wrap around" bounds where xmin > xmax. In this
  /// case, these bounds represent the union of the intervals [xmax, Inf] and [-Inf,
  /// xmin]. This implementation does not yet generate these types of bounds but they may
  /// be encountered in files written by other writers.
  double get_xmin() const;

  /// \brief The maximum encountered value in the X dimension, or -Inf if no X values were
  /// encountered, subject to "wrap around" bounds (see GetXMin()).
  double get_xmax() const;

  /// \brief The minimum encountered value in the Y dimension, or Inf if no Y values were
  /// encountered.
  ///
  /// The Parquet definition allows for "wrap around" bounds where ymin > ymax. In this
  /// case, these bounds represent the union of the intervals [ymax, Inf] and [-Inf,
  /// ymin]. This implementation does not yet generate these types of bounds but they may
  /// be encountered in files written by other readers.
  double get_ymin() const;

  /// \brief The maximum encountered value in the Y dimension, or -Inf if no Y values were
  /// encountered, subject to "wrap around" bounds (see GetXMin()).
  double get_ymax() const;

  /// \brief The minimum encountered value in the Z dimension, or Inf if no Z values were
  /// encountered. Wrap around bounds are not permitted in the Z dimension.
  double get_zmin() const;

  /// \brief The maximum encountered value in the Z dimension, or -Inf if no Z values were
  /// encountered. Wrap around bounds are not permitted in the Z dimension.
  double get_zmax() const;

  /// \brief The minimum encountered value in the M dimension, or Inf if no M values were
  /// encountered.  Wrap around bounds are not permitted in the M dimension.
  double get_mmin() const;

  /// \brief The maximum encountered value in the M dimension, or -Inf if no M values were
  /// encountered.  Wrap around bounds are not permitted in the M dimension.
  double get_mmax() const;

  /// \brief All minimum values in XYZM order
  std::array<double, 4> get_lower_bound() const;

  /// \brief All maximum values in XYZM order
  std::array<double, 4> get_upper_bound() const;

  /// \brief Returns true if zero finite coordinates or geometry types were encountered
  bool is_empty() const;

  /// \brief Returns true if any Z values were encountered or false otherwise
  bool has_z() const;

  /// \brief Returns true if any M values were encountered or false otherwise
  bool has_m() const;

  /// \brief Return the geometry type codes from the well-known binary encountered
  ///
  /// This implementation always returns sorted output with no duplicates.
  std::vector<int32_t> get_geometry_types() const;

  /// \brief Return a string representation of these statistics
  std::string ToString() const;

 private:
  std::unique_ptr<GeoStatisticsImpl> impl_;
};

}  // namespace parquet
