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

#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace parquet {

namespace geometry {

constexpr double kInf = std::numeric_limits<double>::infinity();

struct BoundingBox {
  BoundingBox(): min{kInf, kInf, kInf, kInf}, max{-kInf, -kInf, -kInf, -kInf} {}
  double min[4];
  double max[4];
};

class WKBCoordSeq {
public:
  size_t Init(const uint8_t* data, size_t data_size) {
    if (data_size < sizeof(uint32_t)) {
        // error
    }

    // Read uint32_t size_coords
    memcpy(&num_coords_, data, sizeof(uint32_t));

    size_t data_size_required = sizeof(uint32_t) + (num_coords_ * num_dims_ * sizeof(double));

    if (data_size_required > data_size) {
        // error
    }

    data_ = data;
    coord_data_size_ = data_size_required - sizeof(uint32_t);
    return data_size_required;
  }

  void UpdateBox(BoundingBox* box) {
    size_t coord_size_bytes = num_dims_ * sizeof(double);
    double coord[4];
    for (size_t offset = 0; offset < coord_data_size_; offset += coord_size_bytes) {
      memcpy(coord, data_ + offset, coord_size_bytes);
      for (uint32_t i = 0; i < num_dims_; i++) {
        box->max[i] = std::max(box->max[i], coord[i]);
      }
    }
  }

 private:
  const uint8_t* data_;
  size_t coord_data_size_;
  uint32_t num_coords_;
  uint32_t num_dims_;
};



}  // namespace geometry

}  // namespace parquet
