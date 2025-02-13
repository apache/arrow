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
#include <limits>
#include <sstream>
#include <string>
#include <unordered_set>

#include "parquet/platform.h"

namespace parquet::geometry {

/// \brief Infinity, used to define bounds of empty bounding boxes
constexpr double kInf = std::numeric_limits<double>::infinity();

/// \brief Valid combinations of dimensions allowed by ISO well-known binary
enum class Dimensions { XY = 0, XYZ = 1, XYM = 2, XYZM = 3, MIN = 0, MAX = 3 };

/// \brief The supported set of geometry types allowed by ISO well-known binary
enum class GeometryType {
  POINT = 1,
  LINESTRING = 2,
  POLYGON = 3,
  MULTIPOINT = 4,
  MULTILINESTRING = 5,
  MULTIPOLYGON = 6,
  GEOMETRYCOLLECTION = 7,
  MIN = 1,
  MAX = 7
};

/// \brief A collection of intervals representing the encountered ranges of values
/// in each dimension.
struct BoundingBox {
  using XY = std::array<double, 2>;
  using XYZ = std::array<double, 3>;
  using XYM = std::array<double, 3>;
  using XYZM = std::array<double, 4>;

  BoundingBox(const XYZM& mins, const XYZM& maxes) : min(mins), max(maxes) {}
  BoundingBox() : min{kInf, kInf, kInf, kInf}, max{-kInf, -kInf, -kInf, -kInf} {}

  BoundingBox(const BoundingBox& other) = default;
  BoundingBox& operator=(const BoundingBox&) = default;

  /// \brief Update the X and Y bounds to ensure these bounds contain coord
  void UpdateXY(const XY& coord) { UpdateInternal(coord); }

  /// \brief Update the X, Y, and Z bounds to ensure these bounds contain coord
  void UpdateXYZ(const XYZ& coord) { UpdateInternal(coord); }

  /// \brief Update the X, Y, and M bounds to ensure these bounds contain coord
  void UpdateXYM(const XYM& coord) {
    min[0] = std::min(min[0], coord[0]);
    min[1] = std::min(min[1], coord[1]);
    min[3] = std::min(min[3], coord[2]);
    max[0] = std::max(max[0], coord[0]);
    max[1] = std::max(max[1], coord[1]);
    max[3] = std::max(max[3], coord[2]);
  }

  /// \brief Update the X, Y, Z, and M bounds to ensure these bounds contain coord
  void UpdateXYZM(const XYZM& coord) { UpdateInternal(coord); }

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
      min[i] = std::min(min[i], other.min[i]);
      max[i] = std::max(max[i], other.max[i]);
    }
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "BoundingBox [" << min[0] << " => " << max[0];
    for (int i = 1; i < 4; i++) {
      ss << ", " << min[i] << " => " << max[i];
    }

    ss << "]";

    return ss.str();
  }

  XYZM min;
  XYZM max;

 private:
  // This works for XY, XYZ, and XYZM
  template <typename Coord>
  void UpdateInternal(Coord coord) {
    static_assert(coord.size() <= 4);

    for (size_t i = 0; i < coord.size(); i++) {
      min[i] = std::min(min[i], coord[i]);
      max[i] = std::max(max[i], coord[i]);
    }
  }
};

inline bool operator==(const BoundingBox& lhs, const BoundingBox& rhs) {
  return lhs.min == rhs.min && lhs.max == rhs.max;
}

class WKBBuffer;

/// \brief Accumulate a BoundingBox and geometry types based on zero or more well-known
/// binary blobs
class PARQUET_EXPORT WKBGeometryBounder {
 public:
  WKBGeometryBounder() = default;
  WKBGeometryBounder(const WKBGeometryBounder&) = default;

  /// \brief Accumulate the bounds of a serialized well-known binary geometry
  ///
  /// Returns SerializationError for any parse errors encountered. Bounds for
  /// any encountered coordinates are accumulated and the geometry type of
  /// the geometry is added to the internal geometry type list.
  ///
  /// Note that this method is NOT appropriate for bounding a GEOGRAPHY,
  /// whose bounds are not a function purely of the vertices. Geography bounding
  /// is not yet implemented.
  ::arrow::Status ReadGeometry(const uint8_t* data, int64_t size);

  /// \brief Accumulate the bounds of a previously-calculated BoundingBox
  void ReadBox(const BoundingBox& box) { box_.Merge(box); }

  /// \brief Accumulate a previously-calculated list of geometry types
  void ReadGeometryTypes(const std::vector<int32_t>& geospatial_types) {
    geospatial_types_.insert(geospatial_types.begin(), geospatial_types.end());
  }

  /// \brief Retrieve the accumulated bounds
  const BoundingBox& Bounds() const { return box_; }

  /// \brief Retrieve the accumulated geometry types
  std::vector<int32_t> GeometryTypes() const {
    std::vector<int32_t> out(geospatial_types_.begin(), geospatial_types_.end());
    std::sort(out.begin(), out.end());
    return out;
  }

  /// \brief Reset the internal bounds and geometry types list to an empty state
  void Reset() {
    box_.Reset();
    geospatial_types_.clear();
  }

 private:
  BoundingBox box_;
  std::unordered_set<int32_t> geospatial_types_;

  ::arrow::Status ReadGeometryInternal(WKBBuffer* src, bool record_wkb_type);

  ::arrow::Status ReadSequence(WKBBuffer* src, Dimensions dimensions, uint32_t n_coords,
                               bool swap);
};

}  // namespace parquet::geometry
