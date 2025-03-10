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
#include <cmath>
#include <limits>
#include <string>
#include <unordered_set>

#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"
#include "parquet/exception.h"

namespace parquet::geometry {

constexpr double kInf = std::numeric_limits<double>::infinity();

struct Dimensions {
  enum class dimensions { XY = 0, XYZ = 1, XYM = 2, XYZM = 3 };

  static dimensions FromWKB(uint32_t wkb_geometry_type) {
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
        throw ParquetException("Invalid wkb_geometry_type: ", wkb_geometry_type);
    }
  }

  template <dimensions dims>
  constexpr static uint32_t size();

  static uint32_t size(dimensions dims);

  // Where to look in a coordinate with this dimension
  // for the X, Y, Z, and M dimensions, respectively.
  static std::array<int, 4> ToXYZM(dimensions dims) {
    switch (dims) {
      case dimensions::XY:
        return {0, 1, -1, -1};
      case dimensions::XYZ:
        return {0, 1, 2, -1};
      case dimensions::XYM:
        return {0, 1, -1, 2};
      case dimensions::XYZM:
        return {0, 1, 2, 3};
      default:
        throw ParquetException("Unknown geometry dimension");
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
        throw ParquetException("Unknown geometry dimension");
    }
  }
};

template <>
constexpr uint32_t Dimensions::size<Dimensions::dimensions::XY>() {
  return 2;
}

template <>
constexpr uint32_t Dimensions::size<Dimensions::dimensions::XYZ>() {
  return 3;
}

template <>
constexpr uint32_t Dimensions::size<Dimensions::dimensions::XYM>() {
  return 3;
}

template <>
constexpr uint32_t Dimensions::size<Dimensions::dimensions::XYZM>() {
  return 4;
}

inline uint32_t Dimensions::size(dimensions dims) {
  switch (dims) {
    case dimensions::XY:
      return size<dimensions::XY>();
    case dimensions::XYZ:
      return size<dimensions::XYZ>();
    case dimensions::XYM:
      return size<dimensions::XYM>();
    case dimensions::XYZM:
      return size<dimensions::XYZM>();
    default:
      throw ParquetException("Unknown geometry dimension");
  }
}

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

  static geometry_type FromWKB(uint32_t wkb_geometry_type) {
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
        throw ParquetException("Invalid wkb_geometry_type: ", wkb_geometry_type);
    }
  }

  static uint32_t ToWKB(geometry_type geometry_type, bool has_z, bool has_m) {
    uint32_t wkb_geom_type = 0;
    switch (geometry_type) {
      case geometry_type::POINT:
        wkb_geom_type = 1;
        break;
      case geometry_type::LINESTRING:
        wkb_geom_type = 2;
        break;
      case geometry_type::POLYGON:
        wkb_geom_type = 3;
        break;
      case geometry_type::MULTIPOINT:
        wkb_geom_type = 4;
        break;
      case geometry_type::MULTILINESTRING:
        wkb_geom_type = 5;
        break;
      case geometry_type::MULTIPOLYGON:
        wkb_geom_type = 6;
        break;
      case geometry_type::GEOMETRYCOLLECTION:
        wkb_geom_type = 7;
        break;
      default:
        throw ParquetException("Invalid geometry_type");
    }
    if (has_z) {
      wkb_geom_type += 1000;
    }
    if (has_m) {
      wkb_geom_type += 2000;
    }
    return wkb_geom_type;
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
  enum Endianness { WKB_BIG_ENDIAN = 0, WKB_LITTLE_ENDIAN = 1 };

  WKBBuffer() : data_(NULLPTR), size_(0) {}
  WKBBuffer(const uint8_t* data, int64_t size) : data_(data), size_(size) {}

  void Init(const uint8_t* data, int64_t size) {
    data_ = data;
    size_ = size;
  }

  uint8_t ReadUInt8() {
    if (size_ < 1) {
      throw ParquetException("Can't read 1 byte from empty WKBBuffer");
    }

    size_ -= 1;
    return *data_++;
  }

  uint32_t ReadUInt32(bool swap) {
    if (ARROW_PREDICT_FALSE(swap)) {
      return ReadUInt32<true>();
    } else {
      return ReadUInt32<false>();
    }
  }

  template <bool swap>
  uint32_t ReadUInt32() {
    if (size_ < sizeof(uint32_t)) {
      throw ParquetException("Can't read 4 bytes from WKBBuffer with ", size_,
                             "remaining");
    }

    uint32_t value;
    memcpy(&value, data_, sizeof(uint32_t));
    data_ += sizeof(uint32_t);
    size_ -= sizeof(uint32_t);

    if constexpr (swap) {
      value = ::arrow::bit_util::ByteSwap(value);
    }

    return value;
  }

  template <bool swap>
  void ReadDoubles(uint32_t n, double* out) {
    if (n == 0) {
      return;
    }

    size_t total_bytes = n * sizeof(double);
    if (size_ < total_bytes) {
      throw ParquetException("Can't read ", total_bytes, " bytes from WKBBuffer with ",
                             size_, "remaining");
    }

    memcpy(out, data_, total_bytes);
    data_ += total_bytes;
    size_ -= total_bytes;

    if constexpr (swap) {
      for (uint32_t i = 0; i < n; i++) {
        out[i] = ::arrow::bit_util::ByteSwap(out[i]);
      }
    }
  }

  size_t size() { return size_; }

 private:
  const uint8_t* data_;
  size_t size_;
};

struct BoundingBox {
  BoundingBox(Dimensions::dimensions dimensions, const std::array<double, 4>& mins,
              const std::array<double, 4>& maxes)
      : dimensions(dimensions) {
    std::memcpy(min, mins.data(), sizeof(min));
    std::memcpy(max, maxes.data(), sizeof(max));
  }
  explicit BoundingBox(Dimensions::dimensions dimensions = Dimensions::dimensions::XYZM)
      : dimensions(dimensions),
        min{kInf, kInf, kInf, kInf},
        max{-kInf, -kInf, -kInf, -kInf} {}

  BoundingBox(const BoundingBox& other) = default;
  BoundingBox& operator=(const BoundingBox&) = default;

  void Reset() {
    for (int i = 0; i < 4; i++) {
      min[i] = kInf;
      max[i] = -kInf;
    }
  }

  void Merge(const BoundingBox& other) {
    if (ARROW_PREDICT_TRUE(dimensions == other.dimensions)) {
      for (int i = 0; i < 4; i++) {
        min[i] = std::min(min[i], other.min[i]);
        max[i] = std::max(max[i], other.max[i]);
      }

      return;
    } else if (dimensions == Dimensions::dimensions::XYZM) {
      Merge(other.ToXYZM());
    } else {
      ParquetException::NYI();
    }
  }

  BoundingBox ToXYZM() const {
    BoundingBox xyzm(Dimensions::dimensions::XYZM);
    auto to_xyzm = Dimensions::ToXYZM(dimensions);
    for (int i = 0; i < 4; i++) {
      int dim_to_xyzm = to_xyzm[i];
      if (dim_to_xyzm == -1) {
        xyzm.min[i] = kInf;
        xyzm.max[i] = -kInf;
      } else {
        xyzm.min[i] = min[dim_to_xyzm];
        xyzm.max[i] = max[dim_to_xyzm];
      }
    }

    return xyzm;
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "BoundingBox " << Dimensions::ToString(dimensions) << " [" << min[0] << " => "
       << max[0];
    for (int i = 1; i < 4; i++) {
      ss << ", " << min[i] << " => " << max[i];
    }

    ss << "]";

    return ss.str();
  }

  Dimensions::dimensions dimensions;
  double min[4];
  double max[4];
};

inline bool operator==(const BoundingBox& lhs, const BoundingBox& rhs) {
  return lhs.dimensions == rhs.dimensions &&
         std::memcmp(lhs.min, rhs.min, sizeof(lhs.min)) == 0 &&
         std::memcmp(lhs.max, rhs.max, sizeof(lhs.max)) == 0;
}

template <Dimensions::dimensions dims, bool swap, uint32_t chunk_size>
class WKBSequenceBounder {
 public:
  explicit WKBSequenceBounder(double* chunk) : box_(dims), chunk_(chunk) {}
  WKBSequenceBounder(const WKBSequenceBounder&) = default;

  void ReadPoint(WKBBuffer* src) {
    constexpr uint32_t coord_size = Dimensions::size<dims>();
    src->ReadDoubles<swap>(coord_size, chunk_);
    for (uint32_t dim = 0; dim < coord_size; dim++) {
      if (ARROW_PREDICT_TRUE(!std::isnan(chunk_[dim]))) {
        box_.min[dim] = std::min(box_.min[dim], chunk_[dim]);
        box_.max[dim] = std::max(box_.max[dim], chunk_[dim]);
      }
    }
  }

  void ReadSequence(WKBBuffer* src) {
    constexpr uint32_t coord_size = Dimensions::size<dims>();
    constexpr uint32_t coords_per_chunk = chunk_size / coord_size;

    uint32_t n_coords = src->ReadUInt32<swap>();
    uint32_t n_chunks = n_coords / coords_per_chunk;
    for (uint32_t i = 0; i < n_chunks; i++) {
      src->ReadDoubles<swap>(coords_per_chunk * coord_size, chunk_);
      ReadChunk(coords_per_chunk);
    }

    uint32_t remaining_coords = n_coords - (n_chunks * coords_per_chunk);
    src->ReadDoubles<swap>(remaining_coords * coord_size, chunk_);
    ReadChunk(remaining_coords);
  }

  void ReadRings(WKBBuffer* src) {
    uint32_t n_rings = src->ReadUInt32<swap>();
    for (uint32_t i = 0; i < n_rings; i++) {
      ReadSequence(src);
    }
  }

  void Reset() { box_.Reset(); }

  void Finish(BoundingBox* out) { out->Merge(box_); }

 private:
  BoundingBox box_;
  double* chunk_;

  void ReadChunk(uint32_t n_coords) {
    constexpr uint32_t coord_size = Dimensions::size<dims>();
    for (uint32_t dim = 0; dim < coord_size; dim++) {
      for (uint32_t i = 0; i < n_coords; i++) {
        box_.min[dim] = std::min(box_.min[dim], chunk_[i * coord_size + dim]);
        box_.max[dim] = std::max(box_.max[dim], chunk_[i * coord_size + dim]);
      }
    }
  }
};

// We could (should?) avoid this madness by not templating the WKBSequenceBounder
class WKBGenericSequenceBounder {
 public:
  WKBGenericSequenceBounder()
      : chunk_{0.0},
        xy_(chunk_),
        xyz_(chunk_),
        xym_(chunk_),
        xyzm_(chunk_),
        xy_swap_(chunk_),
        xyz_swap_(chunk_),
        xym_swap_(chunk_),
        xyzm_swap_(chunk_) {}
  WKBGenericSequenceBounder(const WKBGenericSequenceBounder&) = default;

  void ReadPoint(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_TRUE(!swap)) {
      switch (dimensions) {
        case Dimensions::dimensions::XY:
          xy_.ReadPoint(src);
          break;
        case Dimensions::dimensions::XYZ:
          xyz_.ReadPoint(src);
          break;
        case Dimensions::dimensions::XYM:
          xym_.ReadPoint(src);
          break;
        case Dimensions::dimensions::XYZM:
          xyzm_.ReadPoint(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::dimensions::XY:
          xy_swap_.ReadPoint(src);
          break;
        case Dimensions::dimensions::XYZ:
          xyz_swap_.ReadPoint(src);
          break;
        case Dimensions::dimensions::XYM:
          xym_swap_.ReadPoint(src);
          break;
        case Dimensions::dimensions::XYZM:
          xyzm_swap_.ReadPoint(src);
          break;
      }
    }
  }

  void ReadSequence(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_TRUE(!swap)) {
      switch (dimensions) {
        case Dimensions::dimensions::XY:
          xy_.ReadSequence(src);
          break;
        case Dimensions::dimensions::XYZ:
          xyz_.ReadSequence(src);
          break;
        case Dimensions::dimensions::XYM:
          xym_.ReadSequence(src);
          break;
        case Dimensions::dimensions::XYZM:
          xyzm_.ReadSequence(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::dimensions::XY:
          xy_swap_.ReadSequence(src);
          break;
        case Dimensions::dimensions::XYZ:
          xyz_swap_.ReadSequence(src);
          break;
        case Dimensions::dimensions::XYM:
          xym_swap_.ReadSequence(src);
          break;
        case Dimensions::dimensions::XYZM:
          xyzm_swap_.ReadSequence(src);
          break;
      }
    }
  }

  void ReadRings(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_TRUE(!swap)) {
      switch (dimensions) {
        case Dimensions::dimensions::XY:
          xy_.ReadRings(src);
          break;
        case Dimensions::dimensions::XYZ:
          xyz_.ReadRings(src);
          break;
        case Dimensions::dimensions::XYM:
          xym_.ReadRings(src);
          break;
        case Dimensions::dimensions::XYZM:
          xyzm_.ReadRings(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::dimensions::XY:
          xy_swap_.ReadRings(src);
          break;
        case Dimensions::dimensions::XYZ:
          xyz_swap_.ReadRings(src);
          break;
        case Dimensions::dimensions::XYM:
          xym_swap_.ReadRings(src);
          break;
        case Dimensions::dimensions::XYZM:
          xyzm_swap_.ReadRings(src);
          break;
      }
    }
  }

  void Finish(BoundingBox* out) {
    xy_.Finish(out);
    xyz_.Finish(out);
    xym_.Finish(out);
    xyzm_.Finish(out);
    xy_swap_.Finish(out);
    xyz_swap_.Finish(out);
    xym_swap_.Finish(out);
    xyzm_swap_.Finish(out);
  }

  void Reset() {
    xy_.Reset();
    xyz_.Reset();
    xym_.Reset();
    xyzm_.Reset();
    xy_swap_.Reset();
    xyz_swap_.Reset();
    xym_swap_.Reset();
    xyzm_swap_.Reset();
  }

 private:
  double chunk_[64];
  WKBSequenceBounder<Dimensions::dimensions::XY, false, 64> xy_;
  WKBSequenceBounder<Dimensions::dimensions::XYZ, false, 64> xyz_;
  WKBSequenceBounder<Dimensions::dimensions::XYM, false, 64> xym_;
  WKBSequenceBounder<Dimensions::dimensions::XYZM, false, 64> xyzm_;
  WKBSequenceBounder<Dimensions::dimensions::XY, true, 64> xy_swap_;
  WKBSequenceBounder<Dimensions::dimensions::XYZ, true, 64> xyz_swap_;
  WKBSequenceBounder<Dimensions::dimensions::XYM, true, 64> xym_swap_;
  WKBSequenceBounder<Dimensions::dimensions::XYZM, true, 64> xyzm_swap_;
};

class WKBGeometryBounder {
 public:
  WKBGeometryBounder() : box_(Dimensions::dimensions::XYZM) {}
  WKBGeometryBounder(const WKBGeometryBounder&) = default;

  void ReadGeometry(WKBBuffer* src, bool record_wkb_type = true) {
    uint8_t endian = src->ReadUInt8();
#if defined(ARROW_LITTLE_ENDIAN)
    bool swap = endian != WKBBuffer::WKB_LITTLE_ENDIAN;
#else
    bool swap = endian != WKBBuffer::WKB_BIG_ENDIAN;
#endif

    uint32_t wkb_geometry_type = src->ReadUInt32(swap);
    auto geometry_type = GeometryType::FromWKB(wkb_geometry_type);
    auto dimensions = Dimensions::FromWKB(wkb_geometry_type);

    // Keep track of geometry types encountered if at the top level
    if (record_wkb_type) {
      geometry_types_.insert(static_cast<int32_t>(wkb_geometry_type));
    }

    switch (geometry_type) {
      case GeometryType::geometry_type::POINT:
        bounder_.ReadPoint(src, dimensions, swap);
        break;
      case GeometryType::geometry_type::LINESTRING:
        bounder_.ReadSequence(src, dimensions, swap);
        break;
      case GeometryType::geometry_type::POLYGON:
        bounder_.ReadRings(src, dimensions, swap);
        break;

      // These are all encoded the same in WKB, even though this encoding would
      // allow for parts to be of a different geometry type or different dimensions.
      // For the purposes of bounding, this does not cause us problems.
      case GeometryType::geometry_type::MULTIPOINT:
      case GeometryType::geometry_type::MULTILINESTRING:
      case GeometryType::geometry_type::MULTIPOLYGON:
      case GeometryType::geometry_type::GEOMETRYCOLLECTION: {
        uint32_t n_parts = src->ReadUInt32(swap);
        for (uint32_t i = 0; i < n_parts; i++) {
          ReadGeometry(src, /*record_wkb_type*/ false);
        }
        break;
      }
    }
  }

  void ReadBox(const BoundingBox& box) { box_.Merge(box); }

  void ReadGeometryTypes(const std::vector<int32_t>& geometry_types) {
    geometry_types_.insert(geometry_types.begin(), geometry_types.end());
  }

  const BoundingBox& Bounds() const { return box_; }

  std::vector<int32_t> GeometryTypes() const {
    std::vector<int32_t> out(geometry_types_.begin(), geometry_types_.end());
    std::sort(out.begin(), out.end());
    return out;
  }

  void Flush() { bounder_.Finish(&box_); }

  void Reset() {
    box_.Reset();
    bounder_.Reset();
    geometry_types_.clear();
  }

 private:
  BoundingBox box_;
  WKBGenericSequenceBounder bounder_;
  std::unordered_set<int32_t> geometry_types_;
};

#if defined(ARROW_LITTLE_ENDIAN)
static constexpr int kWkbNativeEndianness = geometry::WKBBuffer::WKB_LITTLE_ENDIAN;
#else
static constexpr int kWkbNativeEndianness = geometry::WKBBuffer::WKB_BIG_ENDIAN;
#endif

static inline std::string MakeWKBPoint(const double* xyzm, bool has_z, bool has_m) {
  // 1:endianness + 4:type + 8:x + 8:y
  int num_bytes = 21 + (has_z ? 8 : 0) + (has_m ? 8 : 0);
  std::string wkb(num_bytes, 0);
  char* ptr = wkb.data();

  ptr[0] = kWkbNativeEndianness;
  uint32_t geom_type = geometry::GeometryType::ToWKB(
      geometry::GeometryType::geometry_type::POINT, has_z, has_m);
  memcpy(&ptr[1], &geom_type, 4);
  memcpy(&ptr[5], &xyzm[0], 8);
  memcpy(&ptr[13], &xyzm[1], 8);
  ptr += 21;

  if (has_z) {
    memcpy(ptr, &xyzm[2], 8);
    ptr += 8;
  }
  if (has_m) {
    memcpy(ptr, &xyzm[3], 8);
  }

  return wkb;
}

}  // namespace parquet::geometry
