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

#include <cstdint>
#include <memory>

#include "parquet/platform.h"
#include "parquet/types.h"

namespace parquet {

/// \brief Structure represented encoded statistics to be written to and read from Parquet
/// serialized metadata.
///
/// See the Parquet Thrift definition and GeospatialStatistics for the specific definition
/// of field values.
class PARQUET_EXPORT EncodedGeospatialStatistics {
 public:
  static constexpr double kInf = std::numeric_limits<double>::infinity();

  EncodedGeospatialStatistics() = default;
  EncodedGeospatialStatistics(const EncodedGeospatialStatistics&) = default;
  EncodedGeospatialStatistics(EncodedGeospatialStatistics&&) = default;
  EncodedGeospatialStatistics& operator=(const EncodedGeospatialStatistics&) = default;

  double xmin{kInf};
  double xmax{-kInf};
  double ymin{kInf};
  double ymax{-kInf};
  double zmin{kInf};
  double zmax{-kInf};
  double mmin{kInf};
  double mmax{-kInf};
  std::vector<int32_t> geospatial_types;

  bool has_z() const { return (zmax - zmin) != -kInf; }

  bool has_m() const { return (mmax - mmin) != -kInf; }

  bool is_set() const { return !geospatial_types.empty(); }
};

class GeospatialStatisticsImpl;

/// \brief Base type for computing geospatial column statistics while writing a file
class PARQUET_EXPORT GeospatialStatistics {
 public:
  GeospatialStatistics();
  explicit GeospatialStatistics(std::unique_ptr<GeospatialStatisticsImpl> impl);
  explicit GeospatialStatistics(const EncodedGeospatialStatistics& encoded);
  GeospatialStatistics(GeospatialStatistics&&);

  ~GeospatialStatistics();

  /// \brief Return true if bounds, geometry types, and validity are identical
  bool Equals(const GeospatialStatistics& other) const;

  /// \brief Update these statistics based on previously calculated or decoded statistics
  void Merge(const GeospatialStatistics& other);

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
  EncodedGeospatialStatistics Encode() const;

  /// \brief Returns true if all WKB encountered was valid or false otherwise
  bool is_valid() const;

  std::shared_ptr<GeospatialStatistics> clone() const;

  /// \brief Update these statistics with previously generated statistics
  void Decode(const EncodedGeospatialStatistics& encoded);

  /// \brief The minimum encountered value in the X dimension, or Inf if no X values were
  /// encountered.
  ///
  /// The Parquet definition allows for "wrap around" bounds where xmin > xmax. In this
  /// case, these bounds represent the union of the intervals [xmax, Inf] and [-Inf,
  /// xmin]. This implementation does not yet generate these types of bounds but they may
  /// be encountered in files written by other readers.
  double GetXMin() const;

  /// \brief The maximum encountered value in the X dimension, or -Inf if no X values were
  /// encountered, subject to "wrap around" bounds (see GetXMin()).
  double GetXMax() const;

  /// \brief The minimum encountered value in the Y dimension, or Inf if no Y values were
  /// encountered.
  ///
  /// The Parquet definition allows for "wrap around" bounds where ymin > ymax. In this
  /// case, these bounds represent the union of the intervals [ymax, Inf] and [-Inf,
  /// ymin]. This implementation does not yet generate these types of bounds but they may
  /// be encountered in files written by other readers.
  double GetYMin() const;

  /// \brief The maximum encountered value in the Y dimension, or -Inf if no Y values were
  /// encountered, subject to "wrap around" bounds (see GetXMin()).
  double GetYMax() const;

  /// \brief The minimum encountered value in the Z dimension, or Inf if no Z values were
  /// encountered. Wrap around bounds are not permitted in the Z dimension.
  double GetZMin() const;

  /// \brief The maximum encountered value in the Z dimension, or -Inf if no Z values were
  /// encountered. Wrap around bounds are not permitted in the Z dimension.
  double GetZMax() const;

  /// \brief The minimum encountered value in the M dimension, or Inf if no M values were
  /// encountered.  Wrap around bounds are not permitted in the M dimension.
  double GetMMin() const;

  /// \brief The maximum encountered value in the M dimension, or -Inf if no M values were
  /// encountered.  Wrap around bounds are not permitted in the M dimension.
  double GetMMax() const;

  /// \brief Returns true if any Z values were encountered or false otherwise
  bool HasZ() const;

  /// \brief Returns true if any M values were encountered or false otherwise
  bool HasM() const;

  /// \brief Return the geometry type codes from the well-known binary encountered
  ///
  /// This implementation always returns sorted output with no duplicates.
  std::vector<int32_t> GetGeometryTypes() const;

 private:
  std::unique_ptr<GeospatialStatisticsImpl> impl_;
};

}  // namespace parquet
