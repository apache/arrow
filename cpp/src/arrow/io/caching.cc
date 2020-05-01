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

#include <algorithm>
#include <cmath>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/caching.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace io {

CacheOptions CacheOptions::Defaults() {
  return CacheOptions{internal::ReadRangeCache::kDefaultHoleSizeLimit,
                      internal::ReadRangeCache::kDefaultRangeSizeLimit};
}

CacheOptions CacheOptions::MakeFromNetworkMetrics(int64_t time_to_first_byte_millis,
                                                  int64_t transfer_bandwidth_mib_per_sec,
                                                  double ideal_bandwidth_utilization_frac,
                                                  int64_t max_ideal_request_size_mib) {
  //
  // The I/O coalescing algorithm uses two parameters:
  //   1. hole_size_limit (a.k.a max_io_gap): Max I/O gap/hole size in bytes
  //   2. range_size_limit (a.k.a ideal_request_size): Ideal I/O Request size in bytes
  //
  // These parameters can be derived from network metrics (e.g. S3) as described below:
  //
  // In an S3 compatible storage, there are two main metrics:
  //   1. Seek-time or Time-To-First-Byte (TTFB) in seconds: call setup latency of a new
  //      S3 request
  //   2. Transfer Bandwidth (BW) for data in bytes/sec
  //
  // 1. Computing hole_size_limit:
  //
  //   hole_size_limit = TTFB * BW
  //
  //   This is also called Bandwidth-Delay-Product (BDP).
  //   Two byte ranges that have a gap can still be mapped to the same read
  //   if the gap is less than the bandwidth-delay product [TTFB * TransferBandwidth],
  //   i.e. if the Time-To-First-Byte (or call setup latency of a new S3 request) is
  //   expected to be greater than just reading and discarding the extra bytes on an
  //   existing HTTP request.
  //
  // 2. Computing range_size_limit:
  //
  //   We want to have high bandwidth utilization per S3 connections,
  //   i.e. transfer large amounts of data to amortize the seek overhead.
  //   But, we also want to leverage parallelism by slicing very large IO chunks.
  //   We define two more config parameters with suggested default values to control
  //   the slice size and seek to balance the two effects with the goal of maximizing
  //   net data load performance.
  //
  //   BW_util_frac (ideal bandwidth utilization): Transfer bandwidth utilization fraction
  //     (per connection) to maximize the net data load. 90% is a good default number for
  //     an effective transfer bandwidth.
  //
  //   MAX_IDEAL_REQUEST_SIZE: The maximum single data request size (in MiB) to maximize
  //     the net data load. 64 MiB is a good default number for the ideal request size.
  //
  //   The amount of data that needs to be transferred in a single S3 get_object
  //   request to achieve effective bandwidth eff_BW = BW_util_frac * BW is as follows:
  //     eff_BW = range_size_limit / (TTFB + range_size_limit / BW)
  //
  //   Substituting TTFB = hole_size_limit / BW and eff_BW = BW_util_frac * BW, we get the
  //   following result:
  //     range_size_limit = hole_size_limit * BW_util_frac / (1 - BW_util_frac)
  //
  //   Applying the MAX_IDEAL_REQUEST_SIZE, we get the following:
  //     range_size_limit = min(MAX_IDEAL_REQUEST_SIZE,
  //                            hole_size_limit * BW_util_frac / (1 - BW_util_frac))
  //
  DCHECK_GT(time_to_first_byte_millis, 0) << "TTFB must be > 0";
  DCHECK_GT(transfer_bandwidth_mib_per_sec, 0) << "Transfer bandwidth must be > 0";
  DCHECK_GT(ideal_bandwidth_utilization_frac, 0)
      << "Ideal bandwidth utilization fraction must be > 0";
  DCHECK_LT(ideal_bandwidth_utilization_frac, 1.0)
      << "Ideal bandwidth utilization fraction must be < 1";
  DCHECK_GT(max_ideal_request_size_mib, 0) << "Max Ideal request size must be > 0";

  const double time_to_first_byte_sec = time_to_first_byte_millis / 1000.0;
  const int64_t transfer_bandwidth_bytes_per_sec =
      transfer_bandwidth_mib_per_sec * 1024 * 1024;
  const int64_t max_ideal_request_size_bytes = max_ideal_request_size_mib * 1024 * 1024;

  // hole_size_limit = TTFB * BW
  const auto hole_size_limit = static_cast<int64_t>(
      std::round(time_to_first_byte_sec * transfer_bandwidth_bytes_per_sec));
  DCHECK_GT(hole_size_limit, 0) << "Computed hole_size_limit must be > 0";

  // range_size_limit = min(MAX_IDEAL_REQUEST_SIZE,
  //                        hole_size_limit * BW_util_frac / (1 - BW_util_frac))
  const int64_t range_size_limit = std::min(
      max_ideal_request_size_bytes,
      static_cast<int64_t>(std::round(hole_size_limit * ideal_bandwidth_utilization_frac /
                                      (1 - ideal_bandwidth_utilization_frac))));
  DCHECK_GT(range_size_limit, 0) << "Computed range_size_limit must be > 0";

  return {hole_size_limit, range_size_limit};
}

namespace internal {

struct RangeCacheEntry {
  ReadRange range;
  Future<std::shared_ptr<Buffer>> future;

  friend bool operator<(const RangeCacheEntry& left, const RangeCacheEntry& right) {
    return left.range.offset < right.range.offset;
  }
};

struct ReadRangeCache::Impl {
  std::shared_ptr<RandomAccessFile> file;
  CacheOptions options;

  // Ordered by offset (so as to find a matching region by binary search)
  std::vector<RangeCacheEntry> entries;

  // Add new entries, themselves ordered by offset
  void AddEntries(std::vector<RangeCacheEntry> new_entries) {
    if (entries.size() > 0) {
      std::vector<RangeCacheEntry> merged(entries.size() + new_entries.size());
      std::merge(entries.begin(), entries.end(), new_entries.begin(), new_entries.end(),
                 merged.begin());
      entries = std::move(merged);
    } else {
      entries = std::move(new_entries);
    }
  }
};

ReadRangeCache::ReadRangeCache(std::shared_ptr<RandomAccessFile> file,
                               CacheOptions options)
    : impl_(new Impl()) {
  impl_->file = std::move(file);
  impl_->options = options;
}

ReadRangeCache::~ReadRangeCache() {}

Status ReadRangeCache::Cache(std::vector<ReadRange> ranges) {
  ranges = internal::CoalesceReadRanges(std::move(ranges), impl_->options.hole_size_limit,
                                        impl_->options.range_size_limit);
  std::vector<RangeCacheEntry> entries;
  entries.reserve(ranges.size());
  for (const auto& range : ranges) {
    auto fut = impl_->file->ReadAsync(range.offset, range.length);
    entries.push_back({range, std::move(fut)});
  }

  impl_->AddEntries(std::move(entries));
  return Status::OK();
}

Result<std::shared_ptr<Buffer>> ReadRangeCache::Read(ReadRange range) {
  if (range.length == 0) {
    static const uint8_t byte = 0;
    return std::make_shared<Buffer>(&byte, 0);
  }

  const auto it = std::lower_bound(
      impl_->entries.begin(), impl_->entries.end(), range,
      [](const RangeCacheEntry& entry, const ReadRange& range) {
        return entry.range.offset + entry.range.length < range.offset + range.length;
      });
  if (it != impl_->entries.end() && it->range.Contains(range)) {
    ARROW_ASSIGN_OR_RAISE(auto buf, it->future.result());
    return SliceBuffer(std::move(buf), range.offset - it->range.offset, range.length);
  }
  return Status::Invalid("ReadRangeCache did not find matching cache entry");
}

}  // namespace internal
}  // namespace io
}  // namespace arrow
