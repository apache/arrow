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

class GeoStatisticsImpl {
 public:
  bool Equals(const GeoStatisticsImpl& other) const {
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

  void Merge(const GeoStatisticsImpl& other) {
    is_valid_ = is_valid_ && other.is_valid_;
    if (!is_valid_) {
      return;
    }

    if (is_wraparound_x() || is_wraparound_y() || other.is_wraparound_x() ||
        other.is_wraparound_y()) {
      throw ParquetException(
          "Wraparound X or Y is not supported by GeoStatistics::Merge()");
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
          "Wraparound X or Y is not suppored by GeoStatistics::Update()");
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
          "Wraparound X or Y is not suppored by GeoStatistics::Update()");
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
          "Wraparound X or Y is not suppored by GeoStatistics::Update()");
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
        throw ParquetException("Unsupported Array type in GeoStatistics::Update(Array): ",
                               values.type()->ToString());
    }
  }

  void Reset() {
    bounder_.Reset();
    is_valid_ = true;
  }

  EncodedGeoStatistics Encode() const {
    if (!is_valid_) {
      return {};
    }

    const geometry::BoundingBox::XYZM& mins = bounder_.Bounds().min;
    const geometry::BoundingBox::XYZM& maxes = bounder_.Bounds().max;

    EncodedGeoStatistics out;
    out.geospatial_types = bounder_.GeometryTypes();

    out.xmin = mins[0];
    out.xmax = maxes[0];
    out.ymin = mins[1];
    out.ymax = maxes[1];

    if (!bound_empty(2)) {
      out.zmin = mins[2];
      out.zmax = maxes[2];
    }

    if (!bound_empty(3)) {
      out.mmin = mins[3];
      out.mmax = maxes[3];
    }

    return out;
  }

  void Update(const EncodedGeoStatistics& encoded) {
    if (!is_valid_) {
      return;
    }

    // We can create GeoStatistics from a wraparound bounding box, but we can't
    // update an existing one because the merge logic is not yet implemented.
    if (!bounds_empty() && (is_wraparound_x() || is_wraparound_y() ||
                            is_wraparound(encoded.xmin, encoded.xmax) ||
                            is_wraparound(encoded.ymin, encoded.ymax))) {
      throw ParquetException(
          "Wraparound X or Y is not suppored by GeoStatistics::Update()");
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
    return is_wraparound(get_lower_bound()[0], get_upper_bound()[0]);
  }

  bool is_wraparound_y() const {
    return is_wraparound(get_lower_bound()[1], get_upper_bound()[1]);
  }

  bool is_valid() const { return is_valid_; }

  bool bounds_empty() const {
    for (int i = 0; i < 4; i++) {
      if (!bound_empty(i)) {
        return false;
      }
    }

    return true;
  }

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
        ::arrow::Status status = bounder_.ReadGeometry(binary_array.GetView(i));
        if (!status.ok()) {
          is_valid_ = false;
          return;
        }
      }
    }
  }

  static bool is_wraparound(double dmin, double dmax) {
    return !std::isinf(dmin - dmax) && dmin > dmax;
  }

  bool bound_empty(int i) const {
    return std::isinf(bounder_.Bounds().min[i] - bounder_.Bounds().max[i]);
  }
};

GeoStatistics::GeoStatistics() : impl_(std::make_unique<GeoStatisticsImpl>()) {}

GeoStatistics::GeoStatistics(const EncodedGeoStatistics& encoded) : GeoStatistics() {
  Decode(encoded);
}

GeoStatistics::~GeoStatistics() = default;

bool GeoStatistics::Equals(const GeoStatistics& other) const {
  return impl_->Equals(*other.impl_);
}

void GeoStatistics::Merge(const GeoStatistics& other) { impl_->Merge(*other.impl_); }

void GeoStatistics::Update(const ByteArray* values, int64_t num_values,
                           int64_t null_count) {
  impl_->Update(values, num_values, null_count);
}

void GeoStatistics::UpdateSpaced(const ByteArray* values, const uint8_t* valid_bits,
                                 int64_t valid_bits_offset, int64_t num_spaced_values,
                                 int64_t num_values, int64_t null_count) {
  impl_->UpdateSpaced(values, valid_bits, valid_bits_offset, num_spaced_values,
                      num_values, null_count);
}

void GeoStatistics::Update(const ::arrow::Array& values) { impl_->Update(values); }

void GeoStatistics::Reset() { impl_->Reset(); }

bool GeoStatistics::is_valid() const { return impl_->is_valid(); }

EncodedGeoStatistics GeoStatistics::Encode() const { return impl_->Encode(); }

void GeoStatistics::Decode(const EncodedGeoStatistics& encoded) {
  impl_->Reset();
  impl_->Update(encoded);
}

double GeoStatistics::get_xmin() const { return impl_->get_lower_bound()[0]; }

double GeoStatistics::get_xmax() const { return impl_->get_upper_bound()[0]; }

double GeoStatistics::get_ymin() const { return impl_->get_lower_bound()[1]; }

double GeoStatistics::get_ymax() const { return impl_->get_upper_bound()[1]; }

double GeoStatistics::get_zmin() const { return impl_->get_lower_bound()[2]; }

double GeoStatistics::get_zmax() const { return impl_->get_upper_bound()[2]; }

double GeoStatistics::get_mmin() const { return impl_->get_lower_bound()[3]; }

double GeoStatistics::get_mmax() const { return impl_->get_upper_bound()[3]; }

std::array<double, 4> GeoStatistics::get_lower_bound() const {
  return impl_->get_lower_bound();
}

std::array<double, 4> GeoStatistics::get_upper_bound() const {
  return impl_->get_upper_bound();
}

bool GeoStatistics::is_empty() const {
  return impl_->get_geometry_types().empty() && impl_->bounds_empty();
}

bool GeoStatistics::has_z() const { return (get_zmax() - get_zmin()) > 0; }

bool GeoStatistics::has_m() const { return (get_mmax() - get_mmin()) > 0; }

std::vector<int32_t> GeoStatistics::get_geometry_types() const {
  return impl_->get_geometry_types();
}

std::string GeoStatistics::ToString() const {
  if (!is_valid()) {
    return "GeoStatistics <invalid>\n";
  }

  std::stringstream ss;
  ss << "GeoStatistics " << std::endl;
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
