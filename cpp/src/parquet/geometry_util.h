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

#include <limits>
#include <string>

#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"
#include "parquet/exception.h"

namespace parquet::geometry {

constexpr double kInf = std::numeric_limits<double>::infinity();

struct Dimensions {
  enum dimensions { XY = 1, XYZ = 2, XYM = 3, XYZM = 4 };

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
};

struct WKBGeometryHeader {
  GeometryType::geometry_type geometry_type;
  Dimensions::dimensions dimensions;
  bool swap;
};

class WKBBuffer {
 public:
  WKBBuffer(const uint8_t* data, int64_t size) : data_(data), size_(size) {}

  WKBGeometryHeader ReadGeometryHeader() {
    WKBGeometryHeader out;

    uint8_t endian = ReadUInt8();
#if defined(ARROW_LITTLE_ENDIAN)
    out.swap = endian != 0x01;
#else
    out.swap = endian != 0x00;
#endif

    uint32_t wkb_geometry_type = ReadUInt32(out.swap);
    out.geometry_type = GeometryType::FromWKB(wkb_geometry_type);
    out.dimensions = Dimensions::FromWKB(wkb_geometry_type);

    return out;
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
      value = arrow::bit_util::ByteSwap(value);
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
        out[i] = arrow::bit_util::ByteSwap(out[i]);
      }
    }
  }

  size_t size() { return size_; }

 private:
  const uint8_t* data_;
  size_t size_;
};

struct BoundingBox {
  explicit BoundingBox(Dimensions::dimensions dimensions)
      : dimensions(dimensions),
        min{kInf, kInf, kInf, kInf},
        max{-kInf, -kInf, -kInf, -kInf} {}

  Dimensions::dimensions dimensions;
  double min[4];
  double max[4];
};

template <Dimensions::dimensions dims, bool swap, uint32_t chunk_size>
class WKBSequenceBounder {
 public:
  explicit WKBSequenceBounder(double* chunk) : box_(dims), chunk_(chunk) {}

  void BoundPoint(WKBBuffer* src) {
    constexpr uint32_t coord_size = Dimensions::size<dims>();
    src->ReadDoubles<swap>(coord_size, chunk_);
    for (uint32_t dim = 0; dim < coord_size; dim++) {
      if (ARROW_PREDICT_TRUE(!std::isnan(chunk_[dim]))) {
        box_.min[dim] = std::min(box_.min[dim], chunk_[dim]);
        box_.max[dim] = std::max(box_.max[dim], chunk_[dim]);
      }
    }
  }

  void BoundSequence(WKBBuffer* src) {
    constexpr uint32_t coord_size = Dimensions::size<dims>();
    constexpr uint32_t coords_per_chunk = chunk_size / sizeof(double) / coord_size;

    uint32_t n_coords = src->ReadUInt32<swap>();
    uint32_t n_chunks = n_coords / coords_per_chunk;
    for (uint32_t i = 0; i < n_chunks; i++) {
      src->ReadDoubles<swap>(coords_per_chunk, chunk_);
      BoundChunk(coords_per_chunk);
    }

    uint32_t remaining_coords = n_coords - (n_chunks * coords_per_chunk);
    src->ReadDoubles<swap>(remaining_coords, chunk_);
    BoundChunk(remaining_coords);
  }

  void BoundRings(WKBBuffer* src) {
    uint32_t n_rings = src->ReadUInt32<swap>();
    for (uint32_t i = 0; i < n_rings; i++) {
      BoundSequence(src);
    }
  }

  void Finish(BoundingBox* out) { ParquetException::NYI(); }

 private:
  BoundingBox box_;
  double* chunk_;

  void BoundChunk(uint32_t n_coords) {
    constexpr uint32_t coord_size = Dimensions::size<dims>();
    for (uint32_t dim = 0; dim < coord_size; dim++) {
      for (uint32_t i = 0; i < n_coords; i++) {
        box_.min[dim] = std::min(box_.min[dim], chunk_[i * coord_size + dim]);
        box_.max[dim] = std::max(box_.max[dim], chunk_[i * coord_size + dim]);
      }
    }
  }
};

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

  void BoundPoint(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_FALSE(swap)) {
      switch (dimensions) {
        case Dimensions::XY:
          xy_.BoundPoint(src);
          break;
        case Dimensions::XYZ:
          xyz_.BoundPoint(src);
          break;
        case Dimensions::XYM:
          xym_.BoundPoint(src);
          break;
        case Dimensions::XYZM:
          xyzm_.BoundPoint(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::XY:
          xy_swap_.BoundPoint(src);
          break;
        case Dimensions::XYZ:
          xyz_swap_.BoundPoint(src);
          break;
        case Dimensions::XYM:
          xym_swap_.BoundPoint(src);
          break;
        case Dimensions::XYZM:
          xyzm_swap_.BoundPoint(src);
          break;
      }
    }
  }

  void BoundSequence(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_FALSE(swap)) {
      switch (dimensions) {
        case Dimensions::XY:
          xy_.BoundSequence(src);
          break;
        case Dimensions::XYZ:
          xyz_.BoundSequence(src);
          break;
        case Dimensions::XYM:
          xym_.BoundSequence(src);
          break;
        case Dimensions::XYZM:
          xyzm_.BoundSequence(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::XY:
          xy_swap_.BoundSequence(src);
          break;
        case Dimensions::XYZ:
          xyz_swap_.BoundSequence(src);
          break;
        case Dimensions::XYM:
          xym_swap_.BoundSequence(src);
          break;
        case Dimensions::XYZM:
          xyzm_swap_.BoundSequence(src);
          break;
      }
    }
  }

  void BoundRings(WKBBuffer* src, Dimensions::dimensions dimensions, bool swap) {
    if (ARROW_PREDICT_FALSE(swap)) {
      switch (dimensions) {
        case Dimensions::XY:
          xy_.BoundRings(src);
          break;
        case Dimensions::XYZ:
          xyz_.BoundRings(src);
          break;
        case Dimensions::XYM:
          xym_.BoundRings(src);
          break;
        case Dimensions::XYZM:
          xyzm_.BoundRings(src);
          break;
      }
    } else {
      switch (dimensions) {
        case Dimensions::XY:
          xy_swap_.BoundRings(src);
          break;
        case Dimensions::XYZ:
          xyz_swap_.BoundRings(src);
          break;
        case Dimensions::XYM:
          xym_swap_.BoundRings(src);
          break;
        case Dimensions::XYZM:
          xyzm_swap_.BoundRings(src);
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

  void BoundGeometry(WKBBuffer* src) {
    WKBGeometryHeader header = src->ReadGeometryHeader();
    switch (header.geometry_type) {
      case GeometryType::POINT:
        bounder_.BoundPoint(src, header.dimensions, header.swap);
        break;
      case GeometryType::LINESTRING:
        bounder_.BoundSequence(src, header.dimensions, header.swap);
        break;
      case GeometryType::POLYGON:
        bounder_.BoundRings(src, header.dimensions, header.swap);
        break;

      // These are all encoded the same in WKB, even though this encoding would
      // allow for parts to be of a different geometry type. For the purposes of
      // bounding, this does not cause us problems.
      case GeometryType::MULTIPOINT:
      case GeometryType::MULTILINESTRING:
      case GeometryType::MULTIPOLYGON:
      case GeometryType::GEOMETRYCOLLECTION: {
        uint32_t n_parts = src->ReadUInt32(header.swap);
        for (uint32_t i = 0; i < n_parts; i++) {
          BoundGeometry(src);
        }
        break;
      }
    }
  }

  void Finish(BoundingBox* out) { bounder_.Finish(out); }

 private:
  BoundingBox box_;
  WKBGenericSequenceBounder bounder_;
};

}  // namespace parquet::geometry
