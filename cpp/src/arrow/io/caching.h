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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/io/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace io {
namespace internal {

/// \brief A read cache designed to hide IO latencies when reading.
///
/// To use this, you must first pass it the ranges you'll need in the future.
/// The cache will combine those ranges according to parameters (see constructor)
/// and start fetching the combined ranges in the background.
/// You can then individually fetch them using Read().
class ARROW_EXPORT ReadRangeCache {
 public:
  static constexpr int64_t kDefaultHoleSizeLimit = 8192;
  static constexpr int64_t kDefaultRangeSizeLimit = 32 * 1024 * 1024;

  /// Construct a read cache
  ///
  /// \param[in] hole_size_limit The maximum distance in bytes between two
  ///   consecutive ranges; beyond this value, ranges are not combined
  /// \param[in] range_size_limit The maximum size in bytes of a combined range;
  ///   if combining two consecutive ranges would produce a range of a size
  ///   greater than this, they are not combined
  explicit ReadRangeCache(std::shared_ptr<RandomAccessFile> file,
                          int64_t hole_size_limit = kDefaultHoleSizeLimit,
                          int64_t range_size_limit = kDefaultRangeSizeLimit);
  ~ReadRangeCache();

  /// \brief Cache the given ranges in the background.
  ///
  /// The caller must ensure that the ranges do not overlap with each other,
  /// nor with previously cached ranges.  Otherwise, behaviour will be undefined.
  Status Cache(std::vector<ReadRange> ranges);

  /// \brief Read a range previously given to Cache().
  Result<std::shared_ptr<Buffer>> Read(ReadRange range);

 protected:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace internal
}  // namespace io
}  // namespace arrow
