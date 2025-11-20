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

#include <algorithm>
#include <array>
#include <cmath>
#include <iostream>
#include <limits>
#include <string>
#include <unordered_set>

#include "arrow/util/logging_internal.h"
#include "parquet/platform.h"

namespace parquet::geospatial {

/// \brief Infinity, used to define bounds of empty bounding boxes
constexpr double kInf = std::numeric_limits<double>::infinity();

/// \brief Valid combinations of dimensions allowed by ISO well-known binary
///
/// These values correspond to the 0, 1000, 2000, 3000 component of the WKB integer
/// geometry type (i.e., the value of geometry_type // 1000).
enum class Dimensions {
  kXY = 0,
  kXYZ = 1,
  kXYM = 2,
  kXYZM = 3,
  kValueMin = 0,
  kValueMax = 3
};

/// \brief The supported set of geometry types allowed by ISO well-known binary
///
/// These values correspond to the 1, 2, ..., 7 component of the WKB integer
/// geometry type (i.e., the value of geometry_type % 1000).
enum class GeometryType {
  kPoint = 1,
  kLinestring = 2,
  kPolygon = 3,
  kMultiPoint = 4,
  kMultiLinestring = 5,
  kMultiPolygon = 6,
  kGeometryCollection = 7,
  kValueMin = 1,
  kValueMax = 7
};

/// \brief A collection of intervals representing the encountered ranges of values
/// in each dimension.
///
/// The Parquet specification also supports wraparound bounding boxes in the X
/// dimension; however, this structure assumes min < max always as it is used for
/// the purposes of accumulating this type of bounds.
///
/// This class will ignore any NaN values it visits via UpdateXY[Z[M]](). This is
/// consistent with GEOS and ensures that ranges are accumulated in the same way that
/// other statistics in Parquet are accumulated (e.g., statistics of a double column will
/// return a min/max of all non-NaN values). In WKB specifically, POINT EMPTY is
/// represented by convention as all ordinate values filled with NaN, so this behaviour
/// allows for no special-casing of POINT EMPTY in the WKB reader.
///
/// This class will propagate any NaN values (per dimension) that were explicitly
/// specified via setting mins/maxes directly or by merging another BoundingBox that
/// contained NaN values. This definition ensures that NaN bounds obtained via
/// EncodedGeoStatistics (which may have been written by some other writer that generated
/// NaNs, either on purpose or by accident) are not silently overwritten.
struct PARQUET_EXPORT BoundingBox {
  using XY = std::array<double, 2>;
  using XYZ = std::array<double, 3>;
  using XYM = std::array<double, 3>;
  using XYZM = std::array<double, 4>;

  BoundingBox(const XYZM& mins, const XYZM& maxes) : min(mins), max(maxes) {}
  BoundingBox() : min{kInf, kInf, kInf, kInf}, max{-kInf, -kInf, -kInf, -kInf} {}

  BoundingBox(const BoundingBox& other) = default;
  BoundingBox& operator=(const BoundingBox&) = default;

  /// \brief Update the X and Y bounds to ensure these bounds contain coord
  void UpdateXY(::arrow::util::span<const double> coord) {
    DCHECK_EQ(coord.size(), 2);
    UpdateInternal(coord);
  }

  /// \brief Update the X, Y, and Z bounds to ensure these bounds contain coord
  void UpdateXYZ(::arrow::util::span<const double> coord) {
    DCHECK_EQ(coord.size(), 3);
    UpdateInternal(coord);
  }

  /// \brief Update the X, Y, and M bounds to ensure these bounds contain coord
  void UpdateXYM(::arrow::util::span<const double> coord) {
    DCHECK_EQ(coord.size(), 3);
    min[0] = std::min(min[0], coord[0]);
    min[1] = std::min(min[1], coord[1]);
    min[3] = std::min(min[3], coord[2]);
    max[0] = std::max(max[0], coord[0]);
    max[1] = std::max(max[1], coord[1]);
    max[3] = std::max(max[3], coord[2]);
  }

  /// \brief Update the X, Y, Z, and M bounds to ensure these bounds contain coord
  void UpdateXYZM(::arrow::util::span<const double> coord) {
    DCHECK_EQ(coord.size(), 4);
    UpdateInternal(coord);
  }

  /// \brief Reset these bounds to an empty state such that they contain no coordinates
  void Reset() {
    for (int i = 0; i < 4; i++) {
      min[i] = kInf;
      max[i] = -kInf;
    }
  }

  /// \brief Update these bounds such they also contain other
  void Merge(const BoundingBox& other) {
    for (int i = 0; i < 4; i++) {
      if (std::isnan(min[i]) || std::isnan(max[i]) || std::isnan(other.min[i]) ||
          std::isnan(other.max[i])) {
        min[i] = std::numeric_limits<double>::quiet_NaN();
        max[i] = std::numeric_limits<double>::quiet_NaN();
      } else {
        min[i] = std::min(min[i], other.min[i]);
        max[i] = std::max(max[i], other.max[i]);
      }
    }
  }

  std::string ToString() const;

  XYZM min;
  XYZM max;

 private:
  // This works for XY, XYZ, and XYZM
  template <typename Coord>
  void UpdateInternal(Coord coord) {
    for (size_t i = 0; i < coord.size(); i++) {
      min[i] = std::min(min[i], coord[i]);
      max[i] = std::max(max[i], coord[i]);
    }
  }
};

inline bool operator==(const BoundingBox& lhs, const BoundingBox& rhs) {
  return lhs.min == rhs.min && lhs.max == rhs.max;
}

inline bool operator!=(const BoundingBox& lhs, const BoundingBox& rhs) {
  return !(lhs == rhs);
}

inline std::ostream& operator<<(std::ostream& os, const BoundingBox& obj) {
  os << obj.ToString();
  return os;
}

class WKBBuffer;

/// \brief Accumulate a BoundingBox and geometry types based on zero or more well-known
/// binary blobs
///
/// Note that this class is NOT appropriate for bounding a GEOGRAPHY,
/// whose bounds are not a function purely of the vertices. Geography bounding
/// is not yet implemented.
class PARQUET_EXPORT WKBGeometryBounder {
 public:
  /// \brief Accumulate the bounds of a serialized well-known binary geometry
  ///
  /// Throws ParquetException for any parse errors encountered. Bounds for
  /// any encountered coordinates are accumulated and the geometry type of
  /// the geometry is added to the internal geometry type list.
  void MergeGeometry(std::string_view bytes_wkb);

  void MergeGeometry(::arrow::util::span<const uint8_t> bytes_wkb);

  /// \brief Accumulate the bounds of a previously-calculated BoundingBox
  void MergeBox(const BoundingBox& box) { box_.Merge(box); }

  /// \brief Accumulate a previously-calculated list of geometry types
  void MergeGeometryTypes(::arrow::util::span<const int32_t> geospatial_types) {
    geospatial_types_.insert(geospatial_types.begin(), geospatial_types.end());
  }

  /// \brief Retrieve the accumulated bounds
  const BoundingBox& Bounds() const { return box_; }

  /// \brief Retrieve the accumulated geometry types
  std::vector<int32_t> GeometryTypes() const;

  /// \brief Reset the internal bounds and geometry types list to an empty state
  void Reset() {
    box_.Reset();
    geospatial_types_.clear();
  }

 private:
  BoundingBox box_;
  std::unordered_set<int32_t> geospatial_types_;

  void MergeGeometryInternal(WKBBuffer* src, bool record_wkb_type);

  void MergeSequence(WKBBuffer* src, Dimensions dimensions, uint32_t n_coords, bool swap);
};

}  // namespace parquet::geospatial
