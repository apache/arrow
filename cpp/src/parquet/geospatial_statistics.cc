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

#include "parquet/geospatial_statistics.h"
#include <cmath>
#include <memory>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/logging.h"
#include "parquet/exception.h"
#include "parquet/geospatial_util_internal.h"

using arrow::util::SafeLoad;

namespace parquet {

class GeospatialStatisticsImpl {
 public:
  bool Equals(const GeospatialStatisticsImpl& other) const {
    if (is_valid_ != other.is_valid_) {
      return false;
    }

    if (!is_valid_ && !other.is_valid_) {
      return true;
    }

    if (bounder_.GeometryTypes() != other.bounder_.GeometryTypes()) {
      return false;
    }

    if (bounder_.Bounds() != other.bounder_.Bounds()) {
      return false;
    }

    return true;
  }

  void Merge(const GeospatialStatisticsImpl& other) {
    is_valid_ = is_valid_ && other.is_valid_;
    if (!is_valid_) {
      return;
    }

    if (is_wraparound_x() || is_wraparound_y() || other.is_wraparound_x() ||
        other.is_wraparound_y()) {
      throw ParquetException(
          "Wraparound X or Y is not suppored by GeospatialStatistics::Merge()");
    }

    bounder_.ReadBox(other.bounder_.Bounds());
    std::vector<int32_t> other_geometry_types = other.bounder_.GeometryTypes();
    bounder_.ReadGeometryTypes(other_geometry_types);
  }

  void Update(const ByteArray* values, int64_t num_values, int64_t null_count) {
    if (!is_valid_) {
      return;
    }

    if (is_wraparound_x() || is_wraparound_y()) {
      throw ParquetException(
          "Wraparound X or Y is not suppored by GeospatialStatistics::Update()");
    }

    for (int64_t i = 0; i < num_values; i++) {
      const ByteArray& item = values[i];
      ::arrow::Status status =
          bounder_.ReadGeometry({reinterpret_cast<const char*>(item.ptr), item.len});
      if (!status.ok()) {
        is_valid_ = false;
        return;
      }
    }
  }

  void UpdateSpaced(const ByteArray* values, const uint8_t* valid_bits,
                    int64_t valid_bits_offset, int64_t num_spaced_values,
                    int64_t num_values, int64_t null_count) {
    DCHECK_GT(num_spaced_values, 0);

    if (!is_valid_) {
      return;
    }

    if (is_wraparound_x() || is_wraparound_y()) {
      throw ParquetException(
          "Wraparound X or Y is not suppored by GeospatialStatistics::Update()");
    }

    ::arrow::Status status = ::arrow::internal::VisitSetBitRuns(
        valid_bits, valid_bits_offset, num_spaced_values,
        [&](int64_t position, int64_t length) {
          for (int64_t i = 0; i < length; i++) {
            ByteArray item = SafeLoad(values + i + position);
            ARROW_RETURN_NOT_OK(bounder_.ReadGeometry(
                {reinterpret_cast<const char*>(item.ptr), item.len}));
          }

          return ::arrow::Status::OK();
        });

    if (!status.ok()) {
      is_valid_ = false;
    }
  }

  void Update(const ::arrow::Array& values) {
    if (!is_valid_) {
      return;
    }

    if (is_wraparound_x() || is_wraparound_y()) {
      throw ParquetException(
          "Wraparound X or Y is not suppored by GeospatialStatistics::Update()");
    }

    // Note that ::arrow::Type::EXTENSION seems to be handled before this is called
    switch (values.type_id()) {
      case ::arrow::Type::BINARY:
        UpdateArrayImpl<::arrow::BinaryArray>(values);
        break;
      case ::arrow::Type::LARGE_BINARY:
        UpdateArrayImpl<::arrow::LargeBinaryArray>(values);
        break;
      // This does not currently handle run-end encoded, dictionary encodings, or views
      default:
        throw ParquetException(
            "Unsupported Array type in GeospatialStatistics::Update(Array): ",
            values.type()->ToString());
    }
  }

  void Reset() {
    bounder_.Reset();
    is_valid_ = true;
  }

  EncodedGeospatialStatistics Encode() const {
    if (!is_valid_) {
      return {};
    }

    const geometry::BoundingBox::XYZM& mins = bounder_.Bounds().min;
    const geometry::BoundingBox::XYZM& maxes = bounder_.Bounds().max;

    EncodedGeospatialStatistics out;
    out.geospatial_types = bounder_.GeometryTypes();

    out.xmin = mins[0];
    out.xmax = maxes[0];
    out.ymin = mins[1];
    out.ymax = maxes[1];
    out.zmin = mins[2];
    out.zmax = maxes[2];
    out.mmin = mins[3];
    out.mmax = maxes[3];

    return out;
  }

  void Update(const EncodedGeospatialStatistics& encoded) {
    if (!is_valid_) {
      return;
    }

    // We can create GeospatialStatistics from a wraparound bounding box, but we can't
    // update an existing one because the merge logic is not yet implemented.
    if (!BoundsEmpty() && (is_wraparound_x() || is_wraparound_y() ||
                           IsWraparound(encoded.xmin, encoded.xmax) ||
                           IsWraparound(encoded.ymin, encoded.ymax))) {
      throw ParquetException(
          "Wraparound X or Y is not suppored by GeospatialStatistics::Update()");
    }

    geometry::BoundingBox box;
    box.min[0] = encoded.xmin;
    box.max[0] = encoded.xmax;
    box.min[1] = encoded.ymin;
    box.max[1] = encoded.ymax;

    if (encoded.has_z()) {
      box.min[2] = encoded.zmin;
      box.max[2] = encoded.zmax;
    }

    if (encoded.has_m()) {
      box.min[3] = encoded.mmin;
      box.max[3] = encoded.mmax;
    }

    bounder_.ReadBox(box);
    bounder_.ReadGeometryTypes(encoded.geospatial_types);
  }

  bool is_wraparound_x() const {
    return IsWraparound(get_lower_bound()[0], get_upper_bound()[0]);
  }

  bool is_wraparound_y() const {
    return IsWraparound(get_lower_bound()[1], get_upper_bound()[1]);
  }

  bool is_valid() const { return is_valid_; }

  const std::array<double, 4>& get_lower_bound() const { return bounder_.Bounds().min; }

  const std::array<double, 4>& get_upper_bound() const { return bounder_.Bounds().max; }

  std::vector<int32_t> get_geometry_types() const { return bounder_.GeometryTypes(); }

 private:
  geometry::WKBGeometryBounder bounder_;
  bool is_valid_ = true;

  template <typename ArrayType>
  void UpdateArrayImpl(const ::arrow::Array& values) {
    const auto& binary_array = static_cast<const ArrayType&>(values);
    for (int64_t i = 0; i < binary_array.length(); ++i) {
      if (!binary_array.IsNull(i)) {
        std::string_view byte_array = binary_array.GetView(i);
        ::arrow::Status status = bounder_.ReadGeometry(binary_array.GetView(i));
        if (!status.ok()) {
          is_valid_ = false;
          return;
        }
      }
    }
  }

  static bool IsWraparound(double dmin, double dmax) {
    return !std::isinf(dmin - dmax) && dmin > dmax;
  }

  bool BoundsEmpty() {
    for (int i = 0; i < 4; i++) {
      if (!std::isinf(bounder_.Bounds().min[i] - bounder_.Bounds().max[i])) {
        return false;
      }
    }

    return true;
  }
};

GeospatialStatistics::GeospatialStatistics()
    : impl_(std::make_unique<GeospatialStatisticsImpl>()) {}

GeospatialStatistics::GeospatialStatistics(const EncodedGeospatialStatistics& encoded)
    : GeospatialStatistics() {
  Decode(encoded);
}

GeospatialStatistics::~GeospatialStatistics() = default;

bool GeospatialStatistics::Equals(const GeospatialStatistics& other) const {
  return impl_->Equals(*other.impl_);
}

void GeospatialStatistics::Merge(const GeospatialStatistics& other) {
  impl_->Merge(*other.impl_);
}

void GeospatialStatistics::Update(const ByteArray* values, int64_t num_values,
                                  int64_t null_count) {
  impl_->Update(values, num_values, null_count);
}

void GeospatialStatistics::UpdateSpaced(const ByteArray* values,
                                        const uint8_t* valid_bits,
                                        int64_t valid_bits_offset,
                                        int64_t num_spaced_values, int64_t num_values,
                                        int64_t null_count) {
  impl_->UpdateSpaced(values, valid_bits, valid_bits_offset, num_spaced_values,
                      num_values, null_count);
}

void GeospatialStatistics::Update(const ::arrow::Array& values) { impl_->Update(values); }

void GeospatialStatistics::Reset() { impl_->Reset(); }

bool GeospatialStatistics::is_valid() const { return impl_->is_valid(); }

EncodedGeospatialStatistics GeospatialStatistics::Encode() const {
  return impl_->Encode();
}

void GeospatialStatistics::Decode(const EncodedGeospatialStatistics& encoded) {
  impl_->Reset();
  impl_->Update(encoded);
}

double GeospatialStatistics::get_xmin() const { return impl_->get_lower_bound()[0]; }

double GeospatialStatistics::get_xmax() const { return impl_->get_upper_bound()[0]; }

double GeospatialStatistics::get_ymin() const { return impl_->get_lower_bound()[1]; }

double GeospatialStatistics::get_ymax() const { return impl_->get_upper_bound()[1]; }

double GeospatialStatistics::get_zmin() const { return impl_->get_lower_bound()[2]; }

double GeospatialStatistics::get_zmax() const { return impl_->get_upper_bound()[2]; }

double GeospatialStatistics::get_mmin() const { return impl_->get_lower_bound()[3]; }

double GeospatialStatistics::get_mmax() const { return impl_->get_upper_bound()[3]; }

std::array<double, 4> GeospatialStatistics::get_lower_bound() const {
  return impl_->get_lower_bound();
}

std::array<double, 4> GeospatialStatistics::get_upper_bound() const {
  return impl_->get_upper_bound();
}

bool GeospatialStatistics::has_z() const { return (get_zmax() - get_zmin()) > 0; }

bool GeospatialStatistics::has_m() const { return (get_mmax() - get_mmin()) > 0; }

std::vector<int32_t> GeospatialStatistics::get_geometry_types() const {
  return impl_->get_geometry_types();
}

std::string GeospatialStatistics::ToString() const {
  if (!is_valid()) {
    return "GeospatialStatistics <invalid>\n";
  }

  std::stringstream ss;
  ss << "GeospatialStatistics " << std::endl;
  ss << "  x: "
     << "[" << get_xmin() << ", " << get_xmax() << "]" << std::endl;
  ss << "  y: "
     << "[" << get_ymin() << ", " << get_ymax() << "]" << std::endl;

  if (has_z()) {
    ss << "  z: "
       << "[" << get_zmin() << ", " << get_zmax() << "]" << std::endl;
  }

  if (has_m()) {
    ss << "  m: "
       << "[" << get_mmin() << ", " << get_mmax() << "]" << std::endl;
  }

  ss << "  geometry_types:";
  for (int32_t geometry_type : get_geometry_types()) {
    ss << " " << geometry_type;
  }

  ss << std::endl;

  return ss.str();
}

}  // namespace parquet
