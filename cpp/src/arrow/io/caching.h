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
#include <utility>
#include <vector>

#include "arrow/io/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace io {

struct ARROW_EXPORT CacheOptions {
  static constexpr double kDefaultIdealBandwidthUtilizationFrac = 0.9;
  static constexpr int64_t kDefaultMaxIdealRequestSizeMib = 64;

  /// /brief The maximum distance in bytes between two consecutive
  ///   ranges; beyond this value, ranges are not combined
  int64_t hole_size_limit;
  /// /brief The maximum size in bytes of a combined range; if
  ///   combining two consecutive ranges would produce a range of a
  ///   size greater than this, they are not combined
  int64_t range_size_limit;

  bool operator==(const CacheOptions& other) const {
    return hole_size_limit == other.hole_size_limit &&
           range_size_limit == other.range_size_limit;
  }

  /// \brief Construct CacheOptions from network storage metrics (e.g. S3).
  ///
  /// \param[in] time_to_first_byte_millis Seek-time or Time-To-First-Byte (TTFB) in
  ///   milliseconds, also called call setup latency of a new S3 request.
  ///   The value is a positive integer.
  /// \param[in] transfer_bandwidth_mib_per_sec Data transfer Bandwidth (BW) in MiB/sec.
  ///   The value is a positive integer.
  /// \param[in] ideal_bandwidth_utilization_frac Transfer bandwidth utilization fraction
  ///   (per connection) to maximize the net data load.
  ///   The value is a positive double precision number less than 1.
  /// \param[in] max_ideal_request_size_mib The maximum single data request size (in MiB)
  ///   to maximize the net data load.
  ///   The value is a positive integer.
  /// \return A new instance of CacheOptions.
  static CacheOptions MakeFromNetworkMetrics(
      int64_t time_to_first_byte_millis, int64_t transfer_bandwidth_mib_per_sec,
      double ideal_bandwidth_utilization_frac = kDefaultIdealBandwidthUtilizationFrac,
      int64_t max_ideal_request_size_mib = kDefaultMaxIdealRequestSizeMib);

  static CacheOptions Defaults();
};

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

  /// Construct a read cache with default
  explicit ReadRangeCache(std::shared_ptr<RandomAccessFile> file, AsyncContext ctx)
      : ReadRangeCache(file, std::move(ctx), CacheOptions::Defaults()) {}

  /// Construct a read cache with given options
  explicit ReadRangeCache(std::shared_ptr<RandomAccessFile> file, AsyncContext ctx,
                          CacheOptions options);
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
