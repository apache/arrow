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
#include <atomic>
#include <cmath>
#include <mutex>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/caching.h"

#include <arrow/memory_pool.h>
#include <parquet/exception.h>
#include <numeric>

#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace io {

CacheOptions CacheOptions::Defaults() {
  return CacheOptions{internal::ReadRangeCache::kDefaultHoleSizeLimit,
                      internal::ReadRangeCache::kDefaultRangeSizeLimit,
                      /*lazy=*/false,
                      /*prefetch_limit=*/0};
}

CacheOptions CacheOptions::LazyDefaults() {
  return CacheOptions{internal::ReadRangeCache::kDefaultHoleSizeLimit,
                      internal::ReadRangeCache::kDefaultRangeSizeLimit,
                      /*lazy=*/true,
                      /*prefetch_limit=*/0};
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

  return {hole_size_limit, range_size_limit, /*lazy=*/false, /*prefetch_limit=*/0};
}

namespace internal {

std::vector<ReadRange> GetReadRangesExcludingHoles(const ReadRange& read_range,
                                                   const std::vector<ReadRange>& holes) {
  std::vector<ReadRange> ranges;
  int64_t offset = read_range.offset;
  for (const auto& hole : holes) {
    if (hole.offset >= read_range.offset + read_range.length ||
        hole.offset + hole.length <= read_range.offset) {
      throw parquet::ParquetException("Parquet error: holes not subset of read range");
    }
    if (hole.offset > offset) {
      ranges.push_back({offset, hole.offset - offset});
    }
    offset = hole.offset + hole.length;
  }
  if (offset < read_range.offset + read_range.length) {
    ranges.push_back({offset, read_range.offset + read_range.length - offset});
  }
  return ranges;
}

int64_t GetActualBufferOffset(const int64_t offset, const int64_t buffer_start_offset,
                              const std::vector<ReadRange>& holes) {
  int padding = 0;
  for (const auto& hole : holes) {
    if (hole.offset >= offset) {
      break;
    }
    if (hole.offset + hole.length <= offset) {
      padding += hole.length;
    } else {
      padding += offset - hole.offset;
    }
  }
  return offset - padding - buffer_start_offset;
}

struct RangeCacheEntry {
  ReadRange range;                 // nominal range for this entry
  std::vector<ReadRange> holes;    // nominal range - holes = actual read ranges
  Future<int64_t> future;          // the future for actual read ranges
  std::shared_ptr<Buffer> buffer;  // actual read ranges are read into this buffer with
                                   // pre-calculated position

  RangeCacheEntry() = default;
  RangeCacheEntry(const ReadRange& range, std::vector<ReadRange>& holes,
                  Future<int64_t>& future, std::unique_ptr<Buffer>& buffer)
      : range(range),
        holes(std::move(holes)),
        future(std::move(future)),
        buffer(std::move(buffer)) {}

  friend bool operator<(const RangeCacheEntry& left, const RangeCacheEntry& right) {
    return left.range.offset < right.range.offset;
  }
};

struct ReadRangeCache::Impl {
  std::shared_ptr<RandomAccessFile> owned_file;
  RandomAccessFile* file;
  IOContext ctx;
  CacheOptions options;

  // Ordered by offset (so as to find a matching region by binary search)
  std::vector<RangeCacheEntry> entries;

  virtual ~Impl() = default;

  virtual Future<int64_t> MaybeRead(RangeCacheEntry* entry) { return entry->future; }

  Future<int64_t> DoAsyncRead(const ReadRange& range, const std::vector<ReadRange>& holes,
                              std::unique_ptr<Buffer>& buffer) const {
    int64_t total_size = range.length;
    for (const auto& hole : holes) {
      total_size -= hole.length;
    }

    buffer = *AllocateBuffer(total_size, 64, ctx.pool());

    auto actual_read_ranges = GetReadRangesExcludingHoles(range, holes);
    return file->ReadAsync(ctx, actual_read_ranges, buffer->mutable_data());
  }

  // Make cache entries for ranges
  virtual std::vector<RangeCacheEntry> MakeCacheEntries(
      const std::vector<ReadRange>& ranges,
      std::vector<std::vector<ReadRange>>& holes_foreach_range) {
    std::vector<RangeCacheEntry> new_entries;
    new_entries.reserve(ranges.size());

    for (size_t i = 0; i < ranges.size(); i++) {
      std::unique_ptr<Buffer> buffer;
      auto future = DoAsyncRead(ranges[i], holes_foreach_range[i], buffer);
      new_entries.emplace_back(ranges[i], holes_foreach_range[i], future, buffer);
    }
    return new_entries;
  }

  void CoalesceRanges(
      std::vector<ReadRange>& ranges,
      const std::vector<std::vector<ReadRange>>& holes_foreach_range_orig,
      std::vector<std::vector<ReadRange>>& holes_foreach_range_new) const {
    auto result = CoalesceReadRanges(std::move(ranges), options.hole_size_limit,
                                     options.range_size_limit);
    if (!result.ok()) {
      throw parquet::ParquetException("Failed to coalesce ranges: " +
                                      result.status().message());
    }
    ranges = std::move(result.ValueOrDie());
    holes_foreach_range_new.resize(ranges.size());

    std::vector<ReadRange> flatten_holes;
    size_t index = 0;
    flatten_holes.reserve(
        std::accumulate(holes_foreach_range_orig.begin(), holes_foreach_range_orig.end(),
                        0, [](int sum, const auto& v) { return sum + v.size(); }));
    for (const auto& v : holes_foreach_range_orig) {
      std::move(v.begin(), v.end(), std::back_inserter(flatten_holes));
    }
    for (size_t i = 0; i < ranges.size(); ++i) {
      std::vector<ReadRange> current_range_holes;
      const auto& range = ranges.at(i);
      for (; index < flatten_holes.size(); ++index) {
        const auto& hole = flatten_holes.at(index);
        if (hole.offset >= range.offset + range.length) {
          break;
        }
        if (!(hole.offset >= range.offset &&
              hole.offset + hole.length <= range.offset + range.length)) {
          throw parquet::ParquetException(
              "Parquet error: holes not subset of read range");
        }
        current_range_holes.push_back({hole.offset, hole.length});
      }
      holes_foreach_range_new.at(i) = std::move(current_range_holes);
    }
    if (ranges.size() != holes_foreach_range_new.size()) {
      throw parquet::ParquetException("ranges.size() !=  holes_foreach_range_new.size()");
    }
  }

  // Add the given ranges to the cache, coalescing them where possible
  virtual Status Cache(std::vector<ReadRange> ranges,
                       std::vector<std::vector<ReadRange>> holes_foreach_range_orig) {
    std::vector<std::vector<ReadRange>> holes_foreach_range_new;
    CoalesceRanges(ranges, holes_foreach_range_orig, holes_foreach_range_new);

    std::vector<RangeCacheEntry> new_entries =
        MakeCacheEntries(ranges, holes_foreach_range_new);
    // Add new entries, themselves ordered by offset
    if (entries.size() > 0) {
      std::vector<RangeCacheEntry> merged(entries.size() + new_entries.size());
      std::merge(entries.begin(), entries.end(), new_entries.begin(), new_entries.end(),
                 merged.begin());
      entries = std::move(merged);
    } else {
      entries = std::move(new_entries);
    }
    // Prefetch immediately, regardless of executor availability, if possible
    return file->WillNeed(ranges);
  }

  // Read the given range from the cache, blocking if needed. Cannot read a range
  // that spans cache entries.
  virtual Result<std::shared_ptr<Buffer>> Read(ReadRange range) {
    if (range.length == 0) {
      static const uint8_t byte = 0;
      return std::make_shared<Buffer>(&byte, 0);
    }

    const auto it = std::lower_bound(
        entries.begin(), entries.end(), range,
        [](const RangeCacheEntry& entry, const ReadRange& range) {
          return entry.range.offset + entry.range.length < range.offset + range.length;
        });
    if (it != entries.end() && it->range.Contains(range)) {
      const auto fut = MaybeRead(&*it);
      const auto result = fut.result();
      if (!result.ok()) {
        throw parquet::ParquetException(
            "Parquet error: read failed for one of the sub range");
      }

      if (options.lazy && options.prefetch_limit > 0) {
        int64_t num_prefetched = 0;
        for (auto next_it = it + 1;
             next_it != entries.end() && num_prefetched < options.prefetch_limit;
             ++next_it) {
          if (!next_it->future.is_valid()) {
            std::unique_ptr<Buffer> buffer;
            next_it->future = DoAsyncRead(next_it->range, next_it->holes, buffer);
            next_it->buffer = std::move(buffer);
          }
          ++num_prefetched;
        }
      }

      const auto actual_start =
          GetActualBufferOffset(range.offset, it->range.offset, it->holes);
      const auto actual_end =
          GetActualBufferOffset(range.offset + range.length, it->range.offset, it->holes);
      return SliceBuffer(it->buffer, actual_start, actual_end - actual_start);
    }
    return Status::Invalid("ReadRangeCache did not find matching cache entry");
  }

  virtual Future<> Wait() {
    std::vector<Future<>> futures;
    for (auto& entry : entries) {
      futures.emplace_back(MaybeRead(&entry));
    }
    return AllComplete(futures);
  }

  // Return a Future that completes when the given ranges have been read.
  virtual Future<> WaitFor(std::vector<ReadRange> ranges) {
    auto end = std::remove_if(ranges.begin(), ranges.end(),
                              [](const ReadRange& range) { return range.length == 0; });
    ranges.resize(end - ranges.begin());
    std::vector<Future<>> futures;
    futures.reserve(ranges.size());
    for (auto& range : ranges) {
      const auto it = std::lower_bound(
          entries.begin(), entries.end(), range,
          [](const RangeCacheEntry& entry, const ReadRange& range) {
            return entry.range.offset + entry.range.length < range.offset + range.length;
          });
      if (it != entries.end() && it->range.Contains(range)) {
        futures.push_back(Future<>(MaybeRead(&*it)));
      } else {
        return Status::Invalid("Range was not requested for caching: offset=",
                               range.offset, " length=", range.length);
      }
    }
    return AllComplete(futures);
  }
};

// Don't read ranges when they're first added. Instead, wait until they're requested
// (either through Read or WaitFor).
struct ReadRangeCache::LazyImpl : public ReadRangeCache::Impl {
  // Protect against concurrent modification of entries[i]->future
  std::mutex entry_mutex;

  virtual ~LazyImpl() = default;

  Future<int64_t> MaybeRead(RangeCacheEntry* entry) override {
    // Called by superclass Read()/WaitFor() so we have the lock
    if (!entry->future.is_valid()) {
      std::unique_ptr<Buffer> buffer;
      entry->future = DoAsyncRead(entry->range, entry->holes, buffer);
      entry->buffer = std::move(buffer);
    }
    return entry->future;
  }

  std::vector<RangeCacheEntry> MakeCacheEntries(
      const std::vector<ReadRange>& ranges,
      std::vector<std::vector<ReadRange>>& holes_foreach_range) override {
    std::vector<RangeCacheEntry> new_entries;
    new_entries.reserve(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
      const auto& range = ranges[i];
      auto& holes = holes_foreach_range[i];
      auto temp_buffer = std::make_unique<Buffer>(NULLPTR, 0);
      auto temp_future = Future<int64_t>();
      // In the lazy variant, don't read data here - later, a call to Read or WaitFor
      // will call back to MaybeRead (under the lock) which will fill the future.
      new_entries.emplace_back(range, holes, temp_future, temp_buffer);
    }
    return new_entries;
  }

  Status Cache(std::vector<ReadRange> ranges,
               std::vector<std::vector<ReadRange>> holes_foreach_range) override {
    std::unique_lock<std::mutex> guard(entry_mutex);
    return ReadRangeCache::Impl::Cache(std::move(ranges), std::move(holes_foreach_range));
  }

  Result<std::shared_ptr<Buffer>> Read(ReadRange range) override {
    std::unique_lock<std::mutex> guard(entry_mutex);
    return ReadRangeCache::Impl::Read(range);
  }

  Future<> Wait() override {
    std::unique_lock<std::mutex> guard(entry_mutex);
    return ReadRangeCache::Impl::Wait();
  }

  Future<> WaitFor(std::vector<ReadRange> ranges) override {
    std::unique_lock<std::mutex> guard(entry_mutex);
    return ReadRangeCache::Impl::WaitFor(std::move(ranges));
  }
};

ReadRangeCache::ReadRangeCache(std::shared_ptr<RandomAccessFile> owned_file,
                               RandomAccessFile* file, IOContext ctx,
                               CacheOptions options)
    : impl_(options.lazy ? new LazyImpl() : new Impl()) {
  impl_->owned_file = std::move(owned_file);
  impl_->file = file;
  impl_->ctx = std::move(ctx);
  impl_->options = options;
}

ReadRangeCache::~ReadRangeCache() = default;

Status ReadRangeCache::Cache(std::vector<ReadRange> ranges,
                             std::vector<std::vector<ReadRange>> holes_foreach_range) {
  return impl_->Cache(std::move(ranges), std::move(holes_foreach_range));
}

Result<std::shared_ptr<Buffer>> ReadRangeCache::Read(ReadRange range) {
  return impl_->Read(range);
}

Future<> ReadRangeCache::Wait() { return impl_->Wait(); }

Future<> ReadRangeCache::WaitFor(std::vector<ReadRange> ranges) {
  return impl_->WaitFor(std::move(ranges));
}

}  // namespace internal
}  // namespace io
}  // namespace arrow
