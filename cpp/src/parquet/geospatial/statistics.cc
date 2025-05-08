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
#include <optional>

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

    // We don't yet support updating wraparound bounds. Rather than throw,
    // we just mark the X bounds as invalid such that they are not used.
    auto other_bounds = other.bounder_.Bounds();
    if (is_wraparound_x() || other.is_wraparound_x()) {
      other_bounds.min[0] = kNaN;
      other_bounds.max[0] = kNaN;
    }

    bounder_.MergeBox(other_bounds);
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
    has_geometry_types_ = true;
  }

  EncodedGeoStatistics Encode() const {
    if (!is_valid_) {
      return {};
    }

    const geospatial::BoundingBox::XYZM& mins = bounder_.Bounds().min;
    const geospatial::BoundingBox::XYZM& maxes = bounder_.Bounds().max;

    EncodedGeoStatistics out;

    if (has_geometry_types_) {
      out.geospatial_types = bounder_.GeometryTypes();
    }

    bool write_x = !bound_empty(0) && bound_valid(0);
    bool write_y = !bound_empty(1) && bound_valid(1);
    bool write_z = write_x && write_y && !bound_empty(2) && bound_valid(2);
    bool write_m = write_x && write_y && !bound_empty(3) && bound_valid(3);

    if (write_x && write_y) {
      out.xmin = mins[0];
      out.xmax = maxes[0];
      out.ymin = mins[1];
      out.ymax = maxes[1];
      out.xy_bounds_present = true;
    }

    if (write_z) {
      out.zmin = mins[2];
      out.zmax = maxes[2];
      out.z_bounds_present = true;
    }

    if (write_m) {
      out.mmin = mins[3];
      out.mmax = maxes[3];
      out.m_bounds_present = true;
    }

    return out;
  }

  void Decode(const EncodedGeoStatistics& encoded) {
    Reset();

    geospatial::BoundingBox box;
    if (encoded.xy_bounds_present) {
      box.min[0] = encoded.xmin;
      box.max[0] = encoded.xmax;
      box.min[1] = encoded.ymin;
      box.max[1] = encoded.ymax;
    } else {
      box.min[0] = kNaN;
      box.max[0] = kNaN;
      box.min[1] = kNaN;
      box.max[1] = kNaN;
    }

    if (encoded.z_bounds_present) {
      box.min[2] = encoded.zmin;
      box.max[2] = encoded.zmax;
    } else {
      box.min[2] = kNaN;
      box.max[2] = kNaN;
    }

    if (encoded.m_bounds_present) {
      box.min[3] = encoded.mmin;
      box.max[3] = encoded.mmax;
    } else {
      box.min[3] = kNaN;
      box.max[3] = kNaN;
    }

    bounder_.MergeBox(box);
    bounder_.MergeGeometryTypes(encoded.geospatial_types);
    has_geometry_types_ = has_geometry_types_ && encoded.geospatial_types_present();
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

  bool bound_valid(int i) const {
    return !std::isnan(bounder_.Bounds().min[i]) && !std::isnan(bounder_.Bounds().max[i]);
  }

  const std::array<double, kMaxDimensions>& lower_bound() const {
    return bounder_.Bounds().min;
  }

  const std::array<double, kMaxDimensions>& upper_bound() const {
    return bounder_.Bounds().max;
  }

  std::optional<std::vector<int32_t>> geometry_types() const {
    if (has_geometry_types_) {
      return bounder_.GeometryTypes();
    } else {
      return std::nullopt;
    }
  }

 private:
  geospatial::WKBGeometryBounder bounder_;
  bool is_valid_ = true;
  bool has_geometry_types_ = true;

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

  // The Parquet specification allows X bounds to be "wraparound" to allow for
  // more compact bounding boxes when a geometry happens to include components
  // on both sides of the antimeridian (e.g., the nation of Fiji). This function
  // checks for that case (see GeoStatistics::lower_bound/upper_bound for more
  // details).
  static bool is_wraparound(double xmin, double xmax) {
    return !std::isinf(xmin - xmax) && xmin > xmax;
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

std::optional<EncodedGeoStatistics> GeoStatistics::Encode() const {
  if (is_valid()) {
    return impl_->Encode();
  } else {
    return std::nullopt;
  }
}

void GeoStatistics::Decode(const EncodedGeoStatistics& encoded) {
  impl_->Decode(encoded);
}

std::array<double, 4> GeoStatistics::lower_bound() const { return impl_->lower_bound(); }

std::array<double, 4> GeoStatistics::upper_bound() const { return impl_->upper_bound(); }

std::array<bool, 4> GeoStatistics::dimension_empty() const {
  return {impl_->bound_empty(0), impl_->bound_empty(1), impl_->bound_empty(2),
          impl_->bound_empty(3)};
}

std::array<bool, 4> GeoStatistics::dimension_valid() const {
  return {impl_->bound_valid(0), impl_->bound_valid(1), impl_->bound_valid(2),
          impl_->bound_valid(3)};
}

std::optional<std::vector<int32_t>> GeoStatistics::geometry_types() const {
  return impl_->geometry_types();
}

std::string GeoStatistics::ToString() const {
  if (!is_valid()) {
    return "<GeoStatistics> invalid";
  }

  std::stringstream ss;
  ss << "<GeoStatistics>";

  std::string dim_label("xyzm");
  auto dim_valid = dimension_valid();
  auto dim_empty = dimension_empty();
  auto lower = lower_bound();
  auto upper = upper_bound();

  for (int i = 0; i < kMaxDimensions; i++) {
    ss << " " << dim_label[i] << ": ";
    if (!dim_valid[i]) {
      ss << "invalid";
    } else if (dim_empty[i]) {
      ss << "empty";
    } else {
      ss << "[" << lower[i] << ", " << upper[i] << "]";
    }
  }

  std::optional<std::vector<int32_t>> maybe_geometry_types = geometry_types();
  ss << " geometry_types: ";
  if (maybe_geometry_types.has_value()) {
    ss << "[";
    std::string sep("");
    for (int32_t geometry_type : *maybe_geometry_types) {
      ss << sep << geometry_type;
      sep = ", ";
    }
    ss << "]";
  } else {
    ss << "invalid";
  }

  return ss.str();
}

}  // namespace parquet::geospatial
