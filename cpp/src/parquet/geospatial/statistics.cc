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

#include "parquet/geospatial/statistics.h"

#include <cmath>
#include <memory>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/util/bit_run_reader.h"
#include "parquet/exception.h"
#include "parquet/geospatial/util_internal.h"

using arrow::util::SafeLoad;

namespace parquet::geospatial {

class GeoStatisticsImpl {
 public:
  bool Equals(const GeoStatisticsImpl& other) const {
    return is_valid_ == other.is_valid_ &&
           bounder_.GeometryTypes() == other.bounder_.GeometryTypes() &&
           bounder_.Bounds() == other.bounder_.Bounds();
  }

  void Merge(const GeoStatisticsImpl& other) {
    is_valid_ = is_valid_ && other.is_valid_;
    if (!is_valid_) {
      return;
    }

    if (is_wraparound_x() || other.is_wraparound_x()) {
      throw ParquetException("Wraparound X is not supported by GeoStatistics::Merge()");
    }

    bounder_.MergeBox(other.bounder_.Bounds());
    std::vector<int32_t> other_geometry_types = other.bounder_.GeometryTypes();
    bounder_.MergeGeometryTypes(other_geometry_types);
  }

  void Update(const ByteArray* values, int64_t num_values) {
    if (!is_valid_) {
      return;
    }

    if (is_wraparound_x()) {
      throw ParquetException("Wraparound X is not suppored by GeoStatistics::Update()");
    }

    for (int64_t i = 0; i < num_values; i++) {
      const ByteArray& item = values[i];
      try {
        bounder_.MergeGeometry({reinterpret_cast<const char*>(item.ptr), item.len});
      } catch (ParquetException&) {
        is_valid_ = false;
        return;
      }
    }
  }

  void UpdateSpaced(const ByteArray* values, const uint8_t* valid_bits,
                    int64_t valid_bits_offset, int64_t num_spaced_values,
                    int64_t num_values) {
    DCHECK_GT(num_spaced_values, 0);

    if (!is_valid_) {
      return;
    }

    if (is_wraparound_x()) {
      throw ParquetException("Wraparound X is not suppored by GeoStatistics::Update()");
    }

    ::arrow::Status status = ::arrow::internal::VisitSetBitRuns(
        valid_bits, valid_bits_offset, num_spaced_values,
        [&](int64_t position, int64_t length) {
          for (int64_t i = 0; i < length; i++) {
            ByteArray item = SafeLoad(values + i + position);
            PARQUET_CATCH_NOT_OK(bounder_.MergeGeometry(
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

    if (is_wraparound_x()) {
      throw ParquetException("Wraparound X is not suppored by GeoStatistics::Update()");
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

    const geospatial::BoundingBox::XYZM& mins = bounder_.Bounds().min;
    const geospatial::BoundingBox::XYZM& maxes = bounder_.Bounds().max;

    EncodedGeoStatistics out;
    out.geospatial_types = bounder_.GeometryTypes();

    if (!bound_empty(0) && !bound_empty(1)) {
      out.xmin = mins[0];
      out.xmax = maxes[0];
      out.ymin = mins[1];
      out.ymax = maxes[1];
      out.has_xy = true;
    }

    if (!bound_empty(2)) {
      out.zmin = mins[2];
      out.zmax = maxes[2];
      out.has_z = true;
    }

    if (!bound_empty(3)) {
      out.mmin = mins[3];
      out.mmax = maxes[3];
      out.has_m = true;
    }

    return out;
  }

  void Update(const EncodedGeoStatistics& encoded) {
    if (!is_valid_) {
      return;
    }

    // We can create GeoStatistics from a wraparound bounding box, but we can't
    // update an existing one because the merge logic is not yet implemented.
    if (!bounds_empty() &&
        (is_wraparound_x() || is_wraparound(encoded.xmin, encoded.xmax))) {
      throw ParquetException("Wraparound X is not suppored by GeoStatistics::Update()");
    }

    geospatial::BoundingBox box;

    if (encoded.has_xy) {
      box.min[0] = encoded.xmin;
      box.max[0] = encoded.xmax;
      box.min[1] = encoded.ymin;
      box.max[1] = encoded.ymax;
    }

    if (encoded.has_z) {
      box.min[2] = encoded.zmin;
      box.max[2] = encoded.zmax;
    }

    if (encoded.has_m) {
      box.min[3] = encoded.mmin;
      box.max[3] = encoded.mmax;
    }

    bounder_.MergeBox(box);
    bounder_.MergeGeometryTypes(encoded.geospatial_types);
  }

  bool is_wraparound_x() const {
    return is_wraparound(lower_bound()[0], upper_bound()[0]);
  }

  bool is_valid() const { return is_valid_; }

  bool bounds_empty() const {
    for (int i = 0; i < kMaxDimensions; i++) {
      if (!bound_empty(i)) {
        return false;
      }
    }

    return true;
  }

  bool bound_empty(int i) const {
    return std::isinf(bounder_.Bounds().min[i] - bounder_.Bounds().max[i]);
  }

  const std::array<double, kMaxDimensions>& lower_bound() const {
    return bounder_.Bounds().min;
  }

  const std::array<double, kMaxDimensions>& upper_bound() const {
    return bounder_.Bounds().max;
  }

  std::vector<int32_t> geometry_types() const { return bounder_.GeometryTypes(); }

 private:
  geospatial::WKBGeometryBounder bounder_;
  bool is_valid_ = true;

  template <typename ArrayType>
  void UpdateArrayImpl(const ::arrow::Array& values) {
    const auto& binary_array = static_cast<const ArrayType&>(values);
    for (int64_t i = 0; i < binary_array.length(); ++i) {
      if (!binary_array.IsNull(i)) {
        try {
          bounder_.MergeGeometry(binary_array.GetView(i));
        } catch (ParquetException&) {
          is_valid_ = false;
          return;
        }
      }
    }
  }

  static bool is_wraparound(double dmin, double dmax) {
    return !std::isinf(dmin - dmax) && dmin > dmax;
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

void GeoStatistics::Update(const ByteArray* values, int64_t num_values) {
  impl_->Update(values, num_values);
}

void GeoStatistics::UpdateSpaced(const ByteArray* values, const uint8_t* valid_bits,
                                 int64_t valid_bits_offset, int64_t num_spaced_values,
                                 int64_t num_values) {
  impl_->UpdateSpaced(values, valid_bits, valid_bits_offset, num_spaced_values,
                      num_values);
}

void GeoStatistics::Update(const ::arrow::Array& values) { impl_->Update(values); }

void GeoStatistics::Reset() { impl_->Reset(); }

bool GeoStatistics::is_valid() const { return impl_->is_valid(); }

EncodedGeoStatistics GeoStatistics::Encode() const { return impl_->Encode(); }

void GeoStatistics::Decode(const EncodedGeoStatistics& encoded) {
  impl_->Reset();
  impl_->Update(encoded);
}

double GeoStatistics::xmin() const { return impl_->lower_bound()[0]; }

double GeoStatistics::xmax() const { return impl_->upper_bound()[0]; }

double GeoStatistics::ymin() const { return impl_->lower_bound()[1]; }

double GeoStatistics::ymax() const { return impl_->upper_bound()[1]; }

double GeoStatistics::zmin() const { return impl_->lower_bound()[2]; }

double GeoStatistics::zmax() const { return impl_->upper_bound()[2]; }

double GeoStatistics::mmin() const { return impl_->lower_bound()[3]; }

double GeoStatistics::mmax() const { return impl_->upper_bound()[3]; }

std::array<double, 4> GeoStatistics::lower_bound() const { return impl_->lower_bound(); }

std::array<double, 4> GeoStatistics::upper_bound() const { return impl_->upper_bound(); }

bool GeoStatistics::is_empty() const {
  return impl_->geometry_types().empty() && impl_->bounds_empty();
}

bool GeoStatistics::has_x() const { return !impl_->bound_empty(0); }

bool GeoStatistics::has_y() const { return !impl_->bound_empty(1); }

bool GeoStatistics::has_z() const { return !impl_->bound_empty(2); }

bool GeoStatistics::has_m() const { return !impl_->bound_empty(3); }

std::array<bool, 4> GeoStatistics::has_dimension() const {
  return {has_x(), has_y(), has_z(), has_m()};
}

std::vector<int32_t> GeoStatistics::geometry_types() const {
  return impl_->geometry_types();
}

std::string GeoStatistics::ToString() const {
  if (!is_valid()) {
    return "GeoStatistics <invalid>\n";
  }

  std::stringstream ss;
  ss << "GeoStatistics " << std::endl;
  ss << "  x: "
     << "[" << xmin() << ", " << xmax() << "]" << std::endl;
  ss << "  y: "
     << "[" << ymin() << ", " << ymax() << "]" << std::endl;

  if (has_z()) {
    ss << "  z: "
       << "[" << zmin() << ", " << zmax() << "]" << std::endl;
  }

  if (has_m()) {
    ss << "  m: "
       << "[" << mmin() << ", " << mmax() << "]" << std::endl;
  }

  ss << "  geometry_types:";
  for (int32_t geometry_type : geometry_types()) {
    ss << " " << geometry_type;
  }

  ss << std::endl;

  return ss.str();
}

}  // namespace parquet::geospatial
