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

#include "parquet/geospatial/util_internal.h"

#include <sstream>

#include "arrow/util/endian.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"
#include "parquet/endian_internal.h"
#include "parquet/exception.h"

namespace parquet::geospatial {

std::string BoundingBox::ToString() const {
  std::stringstream ss;
  ss << "BoundingBox" << std::endl;
  ss << "  x: [" << min[0] << ", " << max[0] << "]" << std::endl;
  ss << "  y: [" << min[1] << ", " << max[1] << "]" << std::endl;
  ss << "  z: [" << min[2] << ", " << max[2] << "]" << std::endl;
  ss << "  m: [" << min[3] << ", " << max[3] << "]" << std::endl;

  return ss.str();
}

/// \brief Object to keep track of the low-level consumption of a well-known binary
/// geometry
///
/// Briefly, ISO well-known binary supported by the Parquet spec is an endian byte
/// (0x01 or 0x00), followed by geometry type + dimensions encoded as a (uint32_t),
/// followed by geometry-specific data. Coordinate sequences are represented by a
/// uint32_t (the number of coordinates) plus a sequence of doubles (number of coordinates
/// multiplied by the number of dimensions).
class WKBBuffer {
 public:
  WKBBuffer() : data_(nullptr), size_(0) {}
  WKBBuffer(const uint8_t* data, int64_t size) : data_(data), size_(size) {}

  uint8_t ReadUInt8() { return ReadChecked<uint8_t>(); }

  uint32_t ReadUInt32(bool swap) {
    auto value = ReadChecked<uint32_t>();
    if (swap) {
      return ::arrow::bit_util::ByteSwap(value);
    } else {
      return value;
    }
  }

  template <typename Coord, typename Visit>
  void ReadCoords(uint32_t n_coords, bool swap, Visit&& visit) {
    size_t total_bytes = n_coords * sizeof(Coord);
    if (size_ < total_bytes) {
      throw ParquetException("Can't read coordinate sequence of ", total_bytes,
                             " bytes from WKBBuffer with ", size_, " remaining");
    }

    if (swap) {
      Coord coord;
      for (uint32_t i = 0; i < n_coords; i++) {
        coord = ReadUnchecked<Coord>();
        for (auto& c : coord) {
          c = ::arrow::bit_util::ByteSwap(c);
        }

        visit(coord);
      }
    } else {
      for (uint32_t i = 0; i < n_coords; i++) {
        visit(ReadUnchecked<Coord>());
      }
    }
  }

  size_t size() { return size_; }

 private:
  const uint8_t* data_;
  size_t size_;

  template <typename T>
  T ReadChecked() {
    if (ARROW_PREDICT_FALSE(size_ < sizeof(T))) {
      throw ParquetException("Can't read ", sizeof(T), " bytes from WKBBuffer with ",
                             size_, " remaining");
    }

    return ReadUnchecked<T>();
  }

  template <typename T>
  T ReadUnchecked() {
    T out = ::arrow::util::SafeLoadAs<T>(data_);
    data_ += sizeof(T);
    size_ -= sizeof(T);
    return out;
  }
};

using GeometryTypeAndDimensions = std::pair<GeometryType, Dimensions>;

namespace {

GeometryTypeAndDimensions ParseGeometryType(uint32_t wkb_geometry_type) {
  // The number 1000 can be used because WKB geometry types are constructed
  // on purpose such that this relationship is true (e.g., LINESTRING ZM maps
  // to 3002).
  uint32_t geometry_type_component = wkb_geometry_type % 1000;
  uint32_t dimensions_component = wkb_geometry_type / 1000;

  auto min_geometry_type_value = static_cast<uint32_t>(GeometryType::kValueMin);
  auto max_geometry_type_value = static_cast<uint32_t>(GeometryType::kValueMax);
  auto min_dimension_value = static_cast<uint32_t>(Dimensions::kValueMin);
  auto max_dimension_value = static_cast<uint32_t>(Dimensions::kValueMax);

  if (geometry_type_component < min_geometry_type_value ||
      geometry_type_component > max_geometry_type_value ||
      dimensions_component < min_dimension_value ||
      dimensions_component > max_dimension_value) {
    throw ParquetException("Invalid WKB geometry type: ", wkb_geometry_type);
  }

  return {static_cast<GeometryType>(geometry_type_component),
          static_cast<Dimensions>(dimensions_component)};
}

}  // namespace

std::vector<int32_t> WKBGeometryBounder::GeometryTypes() const {
  std::vector<int32_t> out(geospatial_types_.begin(), geospatial_types_.end());
  std::sort(out.begin(), out.end());
  return out;
}

void WKBGeometryBounder::MergeGeometry(std::string_view bytes_wkb) {
  MergeGeometry(::arrow::util::span(reinterpret_cast<const uint8_t*>(bytes_wkb.data()),
                                    bytes_wkb.size()));
}

void WKBGeometryBounder::MergeGeometry(::arrow::util::span<const uint8_t> bytes_wkb) {
  WKBBuffer src{bytes_wkb.data(), static_cast<int64_t>(bytes_wkb.size())};
  MergeGeometryInternal(&src, /*record_wkb_type=*/true);
  if (src.size() != 0) {
    throw ParquetException("Exepcted zero bytes after consuming WKB but got ",
                           src.size());
  }
}

void WKBGeometryBounder::MergeGeometryInternal(WKBBuffer* src, bool record_wkb_type) {
  uint8_t endian = src->ReadUInt8();
  bool payload_little_endian;
  if (endian == 0x01) {  // little-endian payload
    payload_little_endian = true;
  } else if (endian == 0x00) {  // big-endian payload
    payload_little_endian = false;
  } else {
    throw ParquetException("Invalid WKB endian flag: ", static_cast<int>(endian));
  }
  // Swap when the payload endianness differs from the host endianness. Rely on the
  // Parquet endianness shim so builds that force little-endian IO on s390x don't silently
  // treat the payload as native.
  constexpr bool kHostIsLittleEndian = ::parquet::internal::kHostIsLittleEndian;
  bool swap = payload_little_endian != kHostIsLittleEndian;

  uint32_t wkb_geometry_type = src->ReadUInt32(swap);
  auto geometry_type_and_dimensions = ParseGeometryType(wkb_geometry_type);
  auto [geometry_type, dimensions] = geometry_type_and_dimensions;

  // Keep track of geometry types encountered if at the top level
  if (record_wkb_type) {
    geospatial_types_.insert(static_cast<int32_t>(wkb_geometry_type));
  }

  switch (geometry_type) {
    case GeometryType::kPoint:
      MergeSequence(src, dimensions, 1, swap);
      break;

    case GeometryType::kLinestring: {
      uint32_t n_coords = src->ReadUInt32(swap);
      MergeSequence(src, dimensions, n_coords, swap);
      break;
    }
    case GeometryType::kPolygon: {
      uint32_t n_parts = src->ReadUInt32(swap);
      for (uint32_t i = 0; i < n_parts; i++) {
        uint32_t n_coords = src->ReadUInt32(swap);
        MergeSequence(src, dimensions, n_coords, swap);
      }
      break;
    }

    // These are all encoded the same in WKB, even though this encoding would
    // allow for parts to be of a different geometry type or different dimensions.
    // For the purposes of bounding, this does not cause us problems. We pass
    // record_wkb_type = false because we do not want the child geometry to be
    // added to the geometry_types list (e.g., for a MultiPoint, we only want
    // the code for MultiPoint to be added, not the code for Point).
    case GeometryType::kMultiPoint:
    case GeometryType::kMultiLinestring:
    case GeometryType::kMultiPolygon:
    case GeometryType::kGeometryCollection: {
      uint32_t n_parts = src->ReadUInt32(swap);
      for (uint32_t i = 0; i < n_parts; i++) {
        MergeGeometryInternal(src, /*record_wkb_type*/ false);
      }
      break;
    }
  }
}

void WKBGeometryBounder::MergeSequence(WKBBuffer* src, Dimensions dimensions,
                                       uint32_t n_coords, bool swap) {
  switch (dimensions) {
    case Dimensions::kXY:
      src->ReadCoords<BoundingBox::XY>(
          n_coords, swap, [&](BoundingBox::XY coord) { box_.UpdateXY(coord); });
      break;
    case Dimensions::kXYZ:
      src->ReadCoords<BoundingBox::XYZ>(
          n_coords, swap, [&](BoundingBox::XYZ coord) { box_.UpdateXYZ(coord); });
      break;
    case Dimensions::kXYM:
      src->ReadCoords<BoundingBox::XYM>(
          n_coords, swap, [&](BoundingBox::XYM coord) { box_.UpdateXYM(coord); });
      break;
    case Dimensions::kXYZM:
      src->ReadCoords<BoundingBox::XYZM>(
          n_coords, swap, [&](BoundingBox::XYZM coord) { box_.UpdateXYZM(coord); });
      break;
    default:
      throw ParquetException("Unknown dimensions");
  }
}

}  // namespace parquet::geospatial
