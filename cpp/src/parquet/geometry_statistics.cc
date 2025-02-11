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

#include "parquet/geometry_statistics.h"
#include <memory>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/util/bit_run_reader.h"
#include "parquet/geometry_util_internal.h"

using arrow::util::SafeLoad;

namespace parquet {

class GeospatialStatisticsImpl {
 public:
  GeospatialStatisticsImpl() = default;
  GeospatialStatisticsImpl(const GeospatialStatisticsImpl&) = default;

  bool Equals(const GeospatialStatisticsImpl& other) const {
    if (is_valid_ != other.is_valid_) {
      return false;
    }

    if (!is_valid_ && !other.is_valid_) {
      return true;
    }

    auto geospatial_types = bounder_.GeometryTypes();
    auto other_geospatial_types = other.bounder_.GeometryTypes();
    if (geospatial_types.size() != other_geospatial_types.size()) {
      return false;
    }

    for (size_t i = 0; i < geospatial_types.size(); i++) {
      if (geospatial_types[i] != other_geospatial_types[i]) {
        return false;
      }
    }

    return bounder_.Bounds() == other.bounder_.Bounds();
  }

  void Merge(const GeospatialStatisticsImpl& other) {
    if (!is_valid_ || !other.is_valid_) {
      is_valid_ = false;
      return;
    }

    bounder_.ReadBox(other.bounder_.Bounds());
    bounder_.ReadGeometryTypes(other.bounder_.GeometryTypes());
  }

  void Update(const ByteArray* values, int64_t num_values, int64_t null_count) {
    if (!is_valid_) {
      return;
    }

    geometry::WKBBuffer buf;
    try {
      for (int64_t i = 0; i < num_values; i++) {
        const ByteArray& item = values[i];
        buf.Init(item.ptr, item.len);
        bounder_.ReadGeometry(&buf);
      }

      bounder_.Flush();
    } catch (ParquetException&) {
      is_valid_ = false;
    }
  }

  void UpdateSpaced(const ByteArray* values, const uint8_t* valid_bits,
                    int64_t valid_bits_offset, int64_t num_spaced_values,
                    int64_t num_values, int64_t null_count) {
    DCHECK_GT(num_spaced_values, 0);

    geometry::WKBBuffer buf;
    try {
      ::arrow::internal::VisitSetBitRunsVoid(
          valid_bits, valid_bits_offset, num_spaced_values,
          [&](int64_t position, int64_t length) {
            for (int64_t i = 0; i < length; i++) {
              ByteArray item = SafeLoad(values + i + position);
              buf.Init(item.ptr, item.len);
              bounder_.ReadGeometry(&buf);
            }
          });
      bounder_.Flush();
    } catch (ParquetException&) {
      is_valid_ = false;
    }
  }

  void Update(const ::arrow::Array& values) {
    const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(values);
    geometry::WKBBuffer buf;
    try {
      for (int64_t i = 0; i < binary_array.length(); ++i) {
        if (!binary_array.IsNull(i)) {
          std::string_view byte_array = binary_array.GetView(i);
          buf.Init(reinterpret_cast<const uint8_t*>(byte_array.data()),
                   byte_array.length());
          bounder_.ReadGeometry(&buf);
          bounder_.Flush();
        }
      }
    } catch (ParquetException&) {
      is_valid_ = false;
    }
  }

  void Reset() {
    bounder_.Reset();
    is_valid_ = true;
  }

  EncodedGeospatialStatistics Encode() const {
    const double* mins = bounder_.Bounds().min;
    const double* maxes = bounder_.Bounds().max;

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

  std::string EncodeMin() const {
    const double* mins = bounder_.Bounds().min;
    bool has_z = !std::isinf(mins[2]);
    bool has_m = !std::isinf(mins[3]);
    return geometry::MakeWKBPoint(mins, has_z, has_m);
  }

  std::string EncodeMax() const {
    const double* maxes = bounder_.Bounds().max;
    bool has_z = !std::isinf(maxes[2]);
    bool has_m = !std::isinf(maxes[3]);
    return geometry::MakeWKBPoint(maxes, has_z, has_m);
  }

  void Update(const EncodedGeospatialStatistics& encoded) {
    if (!is_valid_) {
      return;
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

  bool is_valid() const { return is_valid_; }

  const double* GetMinBounds() { return bounder_.Bounds().min; }

  const double* GetMaxBounds() { return bounder_.Bounds().max; }

  std::vector<int32_t> GetGeometryTypes() const { return bounder_.GeometryTypes(); }

 private:
  geometry::WKBGeometryBounder bounder_;
  bool is_valid_ = true;
};

GeospatialStatistics::GeospatialStatistics()
    : impl_(std::make_unique<GeospatialStatisticsImpl>()) {}

GeospatialStatistics::GeospatialStatistics(std::unique_ptr<GeospatialStatisticsImpl> impl)
    : impl_(std::move(impl)) {}

GeospatialStatistics::GeospatialStatistics(const EncodedGeospatialStatistics& encoded)
    : GeospatialStatistics() {
  Decode(encoded);
}

GeospatialStatistics::GeospatialStatistics(GeospatialStatistics&&) = default;

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

std::string GeospatialStatistics::EncodeMin() const { return impl_->EncodeMin(); }

std::string GeospatialStatistics::EncodeMax() const { return impl_->EncodeMax(); }

void GeospatialStatistics::Decode(const EncodedGeospatialStatistics& encoded) {
  impl_->Update(encoded);
}

std::shared_ptr<GeospatialStatistics> GeospatialStatistics::clone() const {
  std::unique_ptr<GeospatialStatisticsImpl> impl =
      std::make_unique<GeospatialStatisticsImpl>(*impl_);
  return std::make_shared<GeospatialStatistics>(std::move(impl));
}

double GeospatialStatistics::GetXMin() const {
  const double* mins = impl_->GetMinBounds();
  return mins[0];
}

double GeospatialStatistics::GetXMax() const {
  const double* maxes = impl_->GetMaxBounds();
  return maxes[0];
}

double GeospatialStatistics::GetYMin() const {
  const double* mins = impl_->GetMinBounds();
  return mins[1];
}

double GeospatialStatistics::GetYMax() const {
  const double* maxes = impl_->GetMaxBounds();
  return maxes[1];
}

double GeospatialStatistics::GetZMin() const {
  const double* mins = impl_->GetMinBounds();
  return mins[2];
}

double GeospatialStatistics::GetZMax() const {
  const double* maxes = impl_->GetMaxBounds();
  return maxes[2];
}

double GeospatialStatistics::GetMMin() const {
  const double* mins = impl_->GetMinBounds();
  return mins[3];
}

double GeospatialStatistics::GetMMax() const {
  const double* maxes = impl_->GetMaxBounds();
  return maxes[3];
}

bool GeospatialStatistics::HasZ() const { return (GetZMax() - GetZMin()) > 0; }

bool GeospatialStatistics::HasM() const { return (GetMMax() - GetMMin()) > 0; }

std::vector<int32_t> GeospatialStatistics::GetGeometryTypes() const {
  return impl_->GetGeometryTypes();
}

}  // namespace parquet
