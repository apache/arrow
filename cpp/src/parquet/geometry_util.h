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
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"
#include "parquet/exception.h"

namespace parquet::geometry {

constexpr double kInf = std::numeric_limits<double>::infinity();

struct Dimensions {
  enum dimensions { XY = 0, XYZ = 1, XYM = 2, XYZM = 3 };

  static dimensions FromWKB(uint32_t wkb_geometry_type) {
    switch (wkb_geometry_type / 1000) {
      case 0:
        return XY;
      case 1:
        return XYZ;
      case 2:
        return XYM;
      case 3:
        return XYZM;
      default:
        throw ParquetException("Invalid wkb_geometry_type: ", wkb_geometry_type);
    }
  }

  template <dimensions dims>
  constexpr static uint32_t size();

  template <>
  constexpr uint32_t size<XY>() {
    return 2;
  }

  template <>
  constexpr uint32_t size<XYZ>() {
    return 3;
  }

  template <>
  constexpr uint32_t size<XYM>() {
    return 3;
  }

  template <>
  constexpr uint32_t size<XYZM>() {
    return 4;
  }

  static uint32_t size(dimensions dims) {
    switch (dims) {
      case XY:
        return size<XY>();
      case XYZ:
        return size<XYZ>();
      case XYM:
        return size<XYM>();
      case XYZM:
        return size<XYZM>();
      default:
        return 0;
    }
  }

  // Where to look in a coordinate with this dimension
  // for the X, Y, Z, and M dimensions, respectively.
  static std::array<int, 4> ToXYZM(dimensions dims) {
    switch (dims) {
      case XY:
        return {0, 1, -1, -1};
      case XYZ:
        return {0, 1, 2, -1};
      case XYM:
        return {0, 1, -1, 2};
      case XYZM:
        return {0, 1, 2, 3};
      default:
        return {-1, -1, -1, -1};
    }
  }

  static std::string ToString(dimensions dims) {
    switch (dims) {
      case XY:
        return "XY";
      case XYZ:
        return "XYZ";
      case XYM:
        return "XYM";
      case XYZM:
        return "XYZM";
      default:
        return "";
    }
  }
};

struct GeometryType {
  enum geometry_type {
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
        return POINT;
      case 2:
        return LINESTRING;
      case 3:
        return POLYGON;
      case 4:
        return MULTIPOINT;
      case 5:
        return MULTILINESTRING;
      case 6:
        return MULTIPOLYGON;
      case 7:
        return GEOMETRYCOLLECTION;
      default:
        throw ParquetException("Invalid wkb_geometry_type: ", wkb_geometry_type);
    }
  }

  static std::string ToString(geometry_type geometry_type) {
    switch (geometry_type) {
      case POINT:
        return "POINT";
      case LINESTRING:
        return "LINESTRING";
      case POLYGON:
        return "POLYGON";
      case MULTIPOINT:
        return "MULTIPOINT";
      case MULTILINESTRING:
        return "MULTILINESTRING";
      case MULTIPOLYGON:
        return "MULTIPOLYGON";
      case GEOMETRYCOLLECTION:
        return "GEOMETRYCOLLECTION";
      default:
        return "";
    }
  }
};

class WKBBuffer {
 public:
  WKBBuffer() : data_(nullptr), size_(0) {}
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
  explicit BoundingBox(Dimensions::dimensions dimensions = Dimensions::XYZM)
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
    } else if (dimensions == Dimensions::XYZM) {
      Merge(other.ToXYZM());
    } else {
      ParquetException::NYI();
    }
  }

  BoundingBox ToXYZM() const {
    BoundingBox xyzm(Dimensions::XYZM);
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
      src->ReadDoubles<swap>(coords_per_chunk, chunk_);
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
      : xy_(chunk_),
        xyz_(chunk_),
        xym_(chunk_),
        xyzm_(chunk_),
        xy_swap_(chunk_),
        xyz_swap_(chunk_),
        xym_swap_(chunk_),
        xyzm_swap_(chunk_) {}

  void ReadPoint(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_TRUE(!swap)) {
      switch (dimensions) {
        case Dimensions::XY:
          xy_.ReadPoint(src);
          break;
        case Dimensions::XYZ:
          xyz_.ReadPoint(src);
          break;
        case Dimensions::XYM:
          xym_.ReadPoint(src);
          break;
        case Dimensions::XYZM:
          xyzm_.ReadPoint(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::XY:
          xy_swap_.ReadPoint(src);
          break;
        case Dimensions::XYZ:
          xyz_swap_.ReadPoint(src);
          break;
        case Dimensions::XYM:
          xym_swap_.ReadPoint(src);
          break;
        case Dimensions::XYZM:
          xyzm_swap_.ReadPoint(src);
          break;
      }
    }
  }

  void ReadSequence(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_TRUE(!swap)) {
      switch (dimensions) {
        case Dimensions::XY:
          xy_.ReadSequence(src);
          break;
        case Dimensions::XYZ:
          xyz_.ReadSequence(src);
          break;
        case Dimensions::XYM:
          xym_.ReadSequence(src);
          break;
        case Dimensions::XYZM:
          xyzm_.ReadSequence(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::XY:
          xy_swap_.ReadSequence(src);
          break;
        case Dimensions::XYZ:
          xyz_swap_.ReadSequence(src);
          break;
        case Dimensions::XYM:
          xym_swap_.ReadSequence(src);
          break;
        case Dimensions::XYZM:
          xyzm_swap_.ReadSequence(src);
          break;
      }
    }
  }

  void ReadRings(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_TRUE(!swap)) {
      switch (dimensions) {
        case Dimensions::XY:
          xy_.ReadRings(src);
          break;
        case Dimensions::XYZ:
          xyz_.ReadRings(src);
          break;
        case Dimensions::XYM:
          xym_.ReadRings(src);
          break;
        case Dimensions::XYZM:
          xyzm_.ReadRings(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::XY:
          xy_swap_.ReadRings(src);
          break;
        case Dimensions::XYZ:
          xyz_swap_.ReadRings(src);
          break;
        case Dimensions::XYM:
          xym_swap_.ReadRings(src);
          break;
        case Dimensions::XYZM:
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
  WKBSequenceBounder<Dimensions::XY, false, 64> xy_;
  WKBSequenceBounder<Dimensions::XYZ, false, 64> xyz_;
  WKBSequenceBounder<Dimensions::XYM, false, 64> xym_;
  WKBSequenceBounder<Dimensions::XYZM, false, 64> xyzm_;
  WKBSequenceBounder<Dimensions::XY, true, 64> xy_swap_;
  WKBSequenceBounder<Dimensions::XYZ, true, 64> xyz_swap_;
  WKBSequenceBounder<Dimensions::XYM, true, 64> xym_swap_;
  WKBSequenceBounder<Dimensions::XYZM, true, 64> xyzm_swap_;
};

class WKBGeometryBounder {
 public:
  WKBGeometryBounder() : box_(Dimensions::XYZM) {}

  void ReadGeometry(WKBBuffer* src, bool record_wkb_type = true) {
    uint8_t endian = src->ReadUInt8();
#if defined(ARROW_LITTLE_ENDIAN)
    bool swap = endian != 0x01;
#else
    bool swap = endian != 0x00;
#endif

    uint32_t wkb_geometry_type = src->ReadUInt32(swap);
    auto geometry_type = GeometryType::FromWKB(wkb_geometry_type);
    auto dimensions = Dimensions::FromWKB(wkb_geometry_type);

    // Keep track of geometry types encountered if at the top level
    if (record_wkb_type) {
      wkb_types_.insert(wkb_geometry_type);
    }

    switch (geometry_type) {
      case GeometryType::POINT:
        bounder_.ReadPoint(src, dimensions, swap);
        break;
      case GeometryType::LINESTRING:
        bounder_.ReadSequence(src, dimensions, swap);
        break;
      case GeometryType::POLYGON:
        bounder_.ReadRings(src, dimensions, swap);
        break;

      // These are all encoded the same in WKB, even though this encoding would
      // allow for parts to be of a different geometry type or different dimensions.
      // For the purposes of bounding, this does not cause us problems.
      case GeometryType::MULTIPOINT:
      case GeometryType::MULTILINESTRING:
      case GeometryType::MULTIPOLYGON:
      case GeometryType::GEOMETRYCOLLECTION: {
        uint32_t n_parts = src->ReadUInt32(swap);
        for (uint32_t i = 0; i < n_parts; i++) {
          ReadGeometry(src, /*record_wkb_type*/ false);
        }
        break;
      }
    }
  }

  void ReadBox(const BoundingBox& box) { box_.Merge(box); }

  const BoundingBox& Bounds() const { return box_; }

  std::vector<uint32_t> WkbTypes() const {
    std::vector<uint32_t> out(wkb_types_.begin(), wkb_types_.end());
    std::sort(out.begin(), out.end());
    return out;
  }

  void Flush() { bounder_.Finish(&box_); }

  void Reset() {
    box_.Reset();
    bounder_.Reset();
    wkb_types_.clear();
  }

 private:
  BoundingBox box_;
  WKBGenericSequenceBounder bounder_;
  std::unordered_set<uint32_t> wkb_types_;
};

}  // namespace parquet::geometry
