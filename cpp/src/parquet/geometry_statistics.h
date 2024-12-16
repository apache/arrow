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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "parquet/platform.h"
#include "parquet/types.h"

namespace parquet {

class PARQUET_EXPORT EncodedGeometryStatistics {
 public:
  static constexpr double kInf = std::numeric_limits<double>::infinity();

  EncodedGeometryStatistics() = default;
  EncodedGeometryStatistics(const EncodedGeometryStatistics&) = default;
  EncodedGeometryStatistics(EncodedGeometryStatistics&&) = default;
  EncodedGeometryStatistics& operator=(const EncodedGeometryStatistics&) = default;

  double xmin{kInf};
  double xmax{-kInf};
  double ymin{kInf};
  double ymax{-kInf};
  double zmin{kInf};
  double zmax{-kInf};
  double mmin{kInf};
  double mmax{-kInf};
  std::vector<int32_t> geometry_types;

  bool has_z() const { return (zmax - zmin) >= 0; }

  bool has_m() const { return (mmax - mmin) >= 0; }

  bool is_set() const { return !geometry_types.empty(); }
};

class GeometryStatisticsImpl;

class PARQUET_EXPORT GeometryStatistics {
 public:
  GeometryStatistics();
  explicit GeometryStatistics(std::unique_ptr<GeometryStatisticsImpl> impl);
  explicit GeometryStatistics(const EncodedGeometryStatistics& encoded);
  GeometryStatistics(GeometryStatistics&&);

  ~GeometryStatistics();

  bool Equals(const GeometryStatistics& other) const;

  void Merge(const GeometryStatistics& other);

  void Update(const ByteArray* values, int64_t num_values, int64_t null_count);

  void UpdateSpaced(const ByteArray* values, const uint8_t* valid_bits,
                    int64_t valid_bits_offset, int64_t num_spaced_values,
                    int64_t num_values, int64_t null_count);

  void Update(const ::arrow::Array& values);

  void Reset();

  EncodedGeometryStatistics Encode() const;
  std::string EncodeMin() const;
  std::string EncodeMax() const;

  bool is_valid() const;

  std::shared_ptr<GeometryStatistics> clone() const;

  void Decode(const EncodedGeometryStatistics& encoded);

  double GetXMin() const;
  double GetXMax() const;
  double GetYMin() const;
  double GetYMax() const;
  double GetZMin() const;
  double GetZMax() const;
  double GetMMin() const;
  double GetMMax() const;

  bool HasZ() const;
  bool HasM() const;

  std::vector<int32_t> GetGeometryTypes() const;

 private:
  std::unique_ptr<GeometryStatisticsImpl> impl_;
};

}  // namespace parquet
