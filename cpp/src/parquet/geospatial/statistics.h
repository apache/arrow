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

namespace parquet::geospatial {

/// \brief The maximum number of dimensions represented by a geospatial type
/// (i.e., X, Y, Z, and M)
static constexpr int kMaxDimensions = 4;

/// \brief Structure represented encoded statistics to be written to and read from Parquet
/// serialized metadata.
///
/// See the Parquet Thrift definition and GeoStatistics for the specific definition
/// of field values.
struct PARQUET_EXPORT EncodedGeoStatistics {
  bool has_xy{};
  double xmin{};
  double xmax{};
  double ymin{};
  double ymax{};

  bool has_z{};
  double zmin{};
  double zmax{};

  bool has_m{};
  double mmin{};
  double mmax{};
  std::vector<int32_t> geospatial_types;

  bool is_empty() const {
    return !(!geospatial_types.empty() || has_xy || has_z || has_m);
  }
};

class GeoStatisticsImpl;

/// \brief Base type for computing geospatial column statistics while writing a file
/// or representing them when reading a file
///
/// Note that NaN values that were encountered within coordinates are omitted; however,
/// NaN values that were obtained via decoding encoded statistics are propagated. This
/// behaviour ensures C++ clients that are inspecting statistics via the column metadata
/// can detect the case where a writer generated NaNs (even though this implementation
/// does not generate them).
///
/// The handling of NaN values in coordinates is not well-defined among bounding
/// implementations except for the WKB convention for POINT EMPTY, which is consistently
/// represented as a point whose ordinates are all NaN. Any other geometry that contains
/// NaNs cannot expect defined behaviour here or elsewhere; however, a row group that
/// contains both NaN-containing and normal (completely finite) geometries should not be
/// excluded from predicate pushdown.
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
  void Update(const ByteArray* values, int64_t num_values);

  /// \brief Update these statistics based on the non-null elements of values
  void UpdateSpaced(const ByteArray* values, const uint8_t* valid_bits,
                    int64_t valid_bits_offset, int64_t num_spaced_values,
                    int64_t num_values);

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

  /// \brief The minimum encountered value in the X dimension, or Inf if no non-NaN X
  /// values were encountered.
  ///
  /// The Parquet definition allows for "wrap around" bounds where xmin > xmax. In this
  /// case, these bounds represent the union of the intervals [xmax, Inf] and [-Inf,
  /// xmin]. This implementation does not yet generate these types of bounds but they may
  /// be encountered in files written by other writers.
  double xmin() const;

  /// \brief The maximum encountered value in the X dimension, or -Inf if no non-NaN X
  /// values were encountered, subject to "wrap around" bounds (see xmin()).
  double xmax() const;

  /// \brief The minimum encountered value in the Y dimension, or Inf if no non-NaN Y
  /// values were encountered. Wrap around bounds are not permitted in the Y dimension.
  double ymin() const;

  /// \brief The maximum encountered value in the Y dimension, or -Inf if no non-NaN Y
  /// values were encountered. Wrap around bounds are not permitted in the Y dimension.
  double ymax() const;

  /// \brief The minimum encountered value in the Z dimension, or Inf if no non-NaN Z
  /// values were encountered. Wrap around bounds are not permitted in the Z dimension.
  double zmin() const;

  /// \brief The maximum encountered value in the Z dimension, or -Inf if no non-NaN Z
  /// values were encountered. Wrap around bounds are not permitted in the Z dimension.
  double zmax() const;

  /// \brief The minimum encountered value in the M dimension, or Inf if no non-NaN M
  /// values were encountered.  Wrap around bounds are not permitted in the M dimension.
  double mmin() const;

  /// \brief The maximum encountered value in the M dimension, or -Inf if no non-NaN M
  /// values were encountered.  Wrap around bounds are not permitted in the M dimension.
  double mmax() const;

  /// \brief All minimum values in XYZM order
  std::array<double, kMaxDimensions> lower_bound() const;

  /// \brief All maximum values in XYZM order
  std::array<double, kMaxDimensions> upper_bound() const;

  /// \brief Returns true if zero finite coordinates or geometry types were encountered
  ///
  /// This will occur, for example, if all encountered values were null. If all
  /// encountered values were EMPTY (e.g., all values were POINT EMPTY, LINESTRING EMPTY,
  /// etc.), is_empty() will still return false because the geometry types list will
  /// contain values.
  bool is_empty() const;

  /// \brief Returns true if any non-NaN X values were encountered or false otherwise
  bool has_x() const;

  /// \brief Returns true if any non-NaN Y values were encountered or false otherwise
  bool has_y() const;

  /// \brief Returns true if any non-NaN Z values were encountered or false otherwise
  bool has_z() const;

  /// \brief Returns true if any non-NaN M values were encountered or false otherwise
  bool has_m() const;

  /// \brief Returns true if any non-NaN values were encountered in the given dimension
  /// in XYZM order
  std::array<bool, kMaxDimensions> has_dimension() const;

  /// \brief Return the geometry type codes from the well-known binary encountered
  ///
  /// This implementation always returns sorted output with no duplicates.
  std::vector<int32_t> geometry_types() const;

  /// \brief Return a string representation of these statistics
  std::string ToString() const;

 private:
  std::unique_ptr<GeoStatisticsImpl> impl_;
};

}  // namespace parquet::geospatial
