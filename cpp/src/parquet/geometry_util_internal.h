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
#include <string>
#include <unordered_set>

#include "arrow/util/endian.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"
#include "parquet/exception.h"

namespace parquet::geometry {

constexpr double kInf = std::numeric_limits<double>::infinity();

struct Dimensions {
  enum class dimensions { XY = 0, XYZ = 1, XYM = 2, XYZM = 3 };

  static ::arrow::Result<dimensions> FromWKB(uint32_t wkb_geometry_type) {
    switch (wkb_geometry_type / 1000) {
      case 0:
        return dimensions::XY;
      case 1:
        return dimensions::XYZ;
      case 2:
        return dimensions::XYM;
      case 3:
        return dimensions::XYZM;
      default:
        return ::arrow::Status::SerializationError("Invalid wkb_geometry_type: ",
                                                   wkb_geometry_type);
    }
  }

  static uint32_t size(dimensions dims) {
    switch (dims) {
      case dimensions::XY:
        return 2;
      case dimensions::XYZ:
      case dimensions::XYM:
        return 3;
      case dimensions::XYZM:
        return 4;
      default:
        return 0;
    }
  }

  static std::string ToString(dimensions dims) {
    switch (dims) {
      case dimensions::XY:
        return "XY";
      case dimensions::XYZ:
        return "XYZ";
      case dimensions::XYM:
        return "XYM";
      case dimensions::XYZM:
        return "XYZM";
      default:
        return "";
    }
  }
};

struct GeometryType {
  enum class geometry_type {
    POINT = 1,
    LINESTRING = 2,
    POLYGON = 3,
    MULTIPOINT = 4,
    MULTILINESTRING = 5,
    MULTIPOLYGON = 6,
    GEOMETRYCOLLECTION = 7
  };

  static ::arrow::Result<geometry_type> FromWKB(uint32_t wkb_geometry_type) {
    switch (wkb_geometry_type % 1000) {
      case 1:
        return geometry_type::POINT;
      case 2:
        return geometry_type::LINESTRING;
      case 3:
        return geometry_type::POLYGON;
      case 4:
        return geometry_type::MULTIPOINT;
      case 5:
        return geometry_type::MULTILINESTRING;
      case 6:
        return geometry_type::MULTIPOLYGON;
      case 7:
        return geometry_type::GEOMETRYCOLLECTION;
      default:
        return ::arrow::Status::SerializationError("Invalid wkb_geometry_type: ",
                                                   wkb_geometry_type);
    }
  }

  static std::string ToString(geometry_type geometry_type) {
    switch (geometry_type) {
      case geometry_type::POINT:
        return "POINT";
      case geometry_type::LINESTRING:
        return "LINESTRING";
      case geometry_type::POLYGON:
        return "POLYGON";
      case geometry_type::MULTIPOINT:
        return "MULTIPOINT";
      case geometry_type::MULTILINESTRING:
        return "MULTILINESTRING";
      case geometry_type::MULTIPOLYGON:
        return "MULTIPOLYGON";
      case geometry_type::GEOMETRYCOLLECTION:
        return "GEOMETRYCOLLECTION";
      default:
        return "";
    }
  }
};

class WKBBuffer {
 public:
  WKBBuffer() : data_(NULLPTR), size_(0) {}
  WKBBuffer(const uint8_t* data, int64_t size) : data_(data), size_(size) {}

  void Init(const uint8_t* data, int64_t size) {
    data_ = data;
    size_ = size;
  }

  ::arrow::Result<uint8_t> ReadUInt8() {
    if (size_ < 1) {
      throw ParquetException("Can't read 1 byte from empty WKBBuffer");
    }

    size_ -= 1;
    return *data_++;
  }

  ::arrow::Result<uint32_t> ReadUInt32(bool swap) {
    if (size_ < sizeof(uint32_t)) {
      return ::arrow::Status::SerializationError(
          "Can't read 4 bytes from from WKBBuffer with ", size_, " remaining");
    }

    uint32_t value = ::arrow::util::SafeLoadAs<uint32_t>(data_);
    data_ += sizeof(uint32_t);
    size_ -= sizeof(uint32_t);
    if (ARROW_PREDICT_FALSE(swap)) {
      return value = ::arrow::bit_util::ByteSwap(value);
    } else {
      return value;
    }
  }

  template <typename Coord, typename Func>
  ::arrow::Status ReadDoubles(uint32_t n_coords, bool swap, Func&& func) {
    if (n_coords == 0) {
      return ::arrow::Status::OK();
    }

    size_t total_bytes = n_coords * sizeof(Coord);
    if (size_ < total_bytes) {
      return ::arrow::Status::SerializationError(
          "Can't read ", total_bytes, " bytes from WKBBuffer with ", size_, " remaining");
    }

    if (ARROW_PREDICT_FALSE(swap)) {
      Coord coord;
      for (uint32_t i = 0; i < n_coords; i++) {
        coord = ::arrow::util::SafeLoadAs<Coord>(data_);
        for (uint32_t j = 0; j < coord.size(); j++) {
          coord[j] = ::arrow::bit_util::ByteSwap(coord[j]);
        }

        func(coord);
        data_ += sizeof(Coord);
        size_ -= sizeof(Coord);
      }
    } else {
      for (uint32_t i = 0; i < n_coords; i++) {
        func(::arrow::util::SafeLoadAs<Coord>(data_));
        data_ += sizeof(Coord);
        size_ -= sizeof(Coord);
      }
    }

    return ::arrow::Status::OK();
  }

  size_t size() { return size_; }

 private:
  const uint8_t* data_;
  size_t size_;
};

struct BoundingBox {
  BoundingBox(const std::array<double, 4>& mins, const std::array<double, 4>& maxes) {
    std::memcpy(min, mins.data(), sizeof(min));
    std::memcpy(max, maxes.data(), sizeof(max));
  }
  BoundingBox() : min{kInf, kInf, kInf, kInf}, max{-kInf, -kInf, -kInf, -kInf} {}

  BoundingBox(const BoundingBox& other) = default;
  BoundingBox& operator=(const BoundingBox&) = default;

  void UpdateXY(std::array<double, 2> coord) { UpdateInternal(coord); }

  void UpdateXYZ(std::array<double, 3> coord) { UpdateInternal(coord); }

  void UpdateXYM(std::array<double, 3> coord) {
    min[0] = std::min(min[0], coord[0]);
    min[1] = std::min(min[1], coord[1]);
    min[3] = std::min(min[3], coord[2]);
    max[0] = std::max(max[0], coord[0]);
    max[1] = std::max(max[1], coord[1]);
    max[3] = std::max(max[3], coord[2]);
  }

  void UpdateXYZM(std::array<double, 4> coord) { UpdateInternal(coord); }

  void Reset() {
    for (int i = 0; i < 4; i++) {
      min[i] = kInf;
      max[i] = -kInf;
    }
  }

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

  double min[4];
  double max[4];

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
  return std::memcmp(lhs.min, rhs.min, sizeof(lhs.min)) == 0 &&
         std::memcmp(lhs.max, rhs.max, sizeof(lhs.max)) == 0;
}

class WKBGeometryBounder {
 public:
  WKBGeometryBounder() = default;
  WKBGeometryBounder(const WKBGeometryBounder&) = default;

  ::arrow::Status ReadGeometry(WKBBuffer* src, bool record_wkb_type = true) {
    ARROW_ASSIGN_OR_RAISE(uint8_t endian, src->ReadUInt8());
#if defined(ARROW_LITTLE_ENDIAN)
    bool swap = endian != 0x01;
#else
    bool swap = endian != 0x00;
#endif

    ARROW_ASSIGN_OR_RAISE(uint32_t wkb_geometry_type, src->ReadUInt32(swap));
    ARROW_ASSIGN_OR_RAISE(auto geometry_type, GeometryType::FromWKB(wkb_geometry_type));
    ARROW_ASSIGN_OR_RAISE(auto dimensions, Dimensions::FromWKB(wkb_geometry_type));

    // Keep track of geometry types encountered if at the top level
    if (record_wkb_type) {
      geospatial_types_.insert(static_cast<int32_t>(wkb_geometry_type));
    }

    switch (geometry_type) {
      case GeometryType::geometry_type::POINT:
        ARROW_RETURN_NOT_OK(ReadSequence(src, dimensions, 1, swap));
        break;

      case GeometryType::geometry_type::LINESTRING: {
        ARROW_ASSIGN_OR_RAISE(uint32_t n_coords, src->ReadUInt32(swap));
        ARROW_RETURN_NOT_OK(ReadSequence(src, dimensions, n_coords, swap));
        break;
      }
      case GeometryType::geometry_type::POLYGON: {
        ARROW_ASSIGN_OR_RAISE(uint32_t n_parts, src->ReadUInt32(swap));
        for (uint32_t i = 0; i < n_parts; i++) {
          ARROW_ASSIGN_OR_RAISE(uint32_t n_coords, src->ReadUInt32(swap));
          ARROW_RETURN_NOT_OK(ReadSequence(src, dimensions, n_coords, swap));
        }
        break;
      }

      // These are all encoded the same in WKB, even though this encoding would
      // allow for parts to be of a different geometry type or different dimensions.
      // For the purposes of bounding, this does not cause us problems.
      case GeometryType::geometry_type::MULTIPOINT:
      case GeometryType::geometry_type::MULTILINESTRING:
      case GeometryType::geometry_type::MULTIPOLYGON:
      case GeometryType::geometry_type::GEOMETRYCOLLECTION: {
        ARROW_ASSIGN_OR_RAISE(uint32_t n_parts, src->ReadUInt32(swap));
        for (uint32_t i = 0; i < n_parts; i++) {
          ARROW_RETURN_NOT_OK(ReadGeometry(src, /*record_wkb_type*/ false));
        }
        break;
      }
    }

    return ::arrow::Status::OK();
  }

  void ReadBox(const BoundingBox& box) { box_.Merge(box); }

  void ReadGeometryTypes(const std::vector<int32_t>& geospatial_types) {
    geospatial_types_.insert(geospatial_types.begin(), geospatial_types.end());
  }

  const BoundingBox& Bounds() const { return box_; }

  std::vector<int32_t> GeometryTypes() const {
    std::vector<int32_t> out(geospatial_types_.begin(), geospatial_types_.end());
    std::sort(out.begin(), out.end());
    return out;
  }

  void Reset() {
    box_.Reset();
    geospatial_types_.clear();
  }

 private:
  BoundingBox box_;
  std::unordered_set<int32_t> geospatial_types_;

  ::arrow::Status ReadSequence(WKBBuffer* src, Dimensions::dimensions dimensions,
                               uint32_t n_coords, bool swap) {
    using XY = std::array<double, 2>;
    using XYZ = std::array<double, 3>;
    using XYM = std::array<double, 3>;
    using XYZM = std::array<double, 4>;

    switch (dimensions) {
      case Dimensions::dimensions::XY:
        return src->ReadDoubles<XY>(n_coords, swap,
                                    [&](XY coord) { box_.UpdateXY(coord); });
      case Dimensions::dimensions::XYZ:
        return src->ReadDoubles<XYZ>(n_coords, swap,
                                     [&](XYZ coord) { box_.UpdateXYZ(coord); });
      case Dimensions::dimensions::XYM:
        return src->ReadDoubles<XYM>(n_coords, swap,
                                     [&](XYM coord) { box_.UpdateXYM(coord); });
      case Dimensions::dimensions::XYZM:
        return src->ReadDoubles<XYZM>(n_coords, swap,
                                      [&](XYZM coord) { box_.UpdateXYZM(coord); });
      default:
        return ::arrow::Status::Invalid("Unknown dimensions");
    }
  }
};

}  // namespace parquet::geometry
