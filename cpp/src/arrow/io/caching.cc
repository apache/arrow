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
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/util/future.h"
#include "arrow/util/logging_internal.h"

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

struct RangeCacheEntry {
  ReadRange range;
  Future<std::shared_ptr<Buffer>> future;

  RangeCacheEntry() = default;
  RangeCacheEntry(const ReadRange& range_, Future<std::shared_ptr<Buffer>> future_)
      : range(range_), future(std::move(future_)) {}

  friend bool operator<(const RangeCacheEntry& left, const RangeCacheEntry& right) {
    return left.range.offset < right.range.offset;
  }
};

struct ReadRangeCache::Impl {
  std::shared_ptr<RandomAccessFile> owned_file;
  RandomAccessFile* file;
  IOContext ctx;
  CacheOptions options;

  // Ordered by offset (so as to find a matching region by binary search).
  // Mutation of `entries` and of individual entries' futures must be
  // serialized via `entry_mutex`. Every public method that touches either
  // acquires the mutex before delegating to the protected *Locked helpers
  // below, so both the eager and lazy variants are safe to call concurrently
  // from multiple threads.
  std::vector<RangeCacheEntry> entries;
  std::mutex entry_mutex;

  virtual ~Impl() = default;

  // -- Polymorphic hooks. Always called with entry_mutex held. --

  // Get the future corresponding to a range
  virtual Future<std::shared_ptr<Buffer>> MaybeRead(RangeCacheEntry* entry) {
    return entry->future;
  }

  // Make cache entries for ranges
  virtual std::vector<RangeCacheEntry> MakeCacheEntries(
      const std::vector<ReadRange>& ranges) {
    std::vector<RangeCacheEntry> new_entries;
    new_entries.reserve(ranges.size());
    for (const auto& range : ranges) {
      new_entries.emplace_back(range, file->ReadAsync(ctx, range.offset, range.length));
    }
    return new_entries;
  }

  // -- Public entry points (acquire entry_mutex, then delegate). --

  // Add the given ranges to the cache, coalescing them where possible.
  Status Cache(std::vector<ReadRange> ranges) {
    ARROW_ASSIGN_OR_RAISE(
        ranges, internal::CoalesceReadRanges(std::move(ranges), options.hole_size_limit,
                                             options.range_size_limit));
    Status st;
    {
      std::unique_lock<std::mutex> guard(entry_mutex);
      std::vector<RangeCacheEntry> new_entries = MakeCacheEntries(ranges);
      // Add new entries, themselves ordered by offset
      if (entries.size() > 0) {
        std::vector<RangeCacheEntry> merged(entries.size() + new_entries.size());
        std::merge(entries.begin(), entries.end(), new_entries.begin(),
                   new_entries.end(), merged.begin());
        entries = std::move(merged);
      } else {
        entries = std::move(new_entries);
      }
    }
    // Prefetch immediately, regardless of executor availability, if possible.
    // Do this outside the lock: WillNeed() may block on an mmap advise / I/O
    // hint and we don't want to serialize concurrent Reads on it.
    st = file->WillNeed(ranges);
    // As this is optimisation only, I/O failures should not be treated as fatal
    if (st.IsIOError()) {
      return Status::OK();
    }
    return st;
  }

  // Read the given range from the cache, blocking if needed. Cannot read a
  // range that spans cache entries.
  Result<std::shared_ptr<Buffer>> Read(ReadRange range) {
    if (range.length == 0) {
      static const uint8_t byte = 0;
      return std::make_shared<Buffer>(&byte, 0);
    }

    Future<std::shared_ptr<Buffer>> fut;
    int64_t slice_offset = 0;
    {
      std::unique_lock<std::mutex> guard(entry_mutex);
      const auto it = std::lower_bound(
          entries.begin(), entries.end(), range,
          [](const RangeCacheEntry& entry, const ReadRange& range) {
            return entry.range.offset + entry.range.length < range.offset + range.length;
          });
      if (it == entries.end() || !it->range.Contains(range)) {
        return Status::Invalid("ReadRangeCache did not find matching cache entry");
      }
      fut = MaybeRead(&*it);
      slice_offset = range.offset - it->range.offset;
      if (options.lazy && options.prefetch_limit > 0) {
        int64_t num_prefetched = 0;
        for (auto next_it = it + 1;
             next_it != entries.end() && num_prefetched < options.prefetch_limit;
             ++next_it) {
          if (!next_it->future.is_valid()) {
            next_it->future =
                file->ReadAsync(ctx, next_it->range.offset, next_it->range.length);
          }
          ++num_prefetched;
        }
      }
    }
    // Drop the lock before blocking on the I/O future so other threads can
    // still do lookups while a previously queued read is in flight.
    ARROW_ASSIGN_OR_RAISE(auto buf, fut.result());
    return SliceBuffer(std::move(buf), slice_offset, range.length);
  }

  Future<> Wait() {
    std::vector<Future<>> futures;
    {
      std::unique_lock<std::mutex> guard(entry_mutex);
      futures.reserve(entries.size());
      for (auto& entry : entries) {
        futures.emplace_back(MaybeRead(&entry));
      }
    }
    return AllComplete(futures);
  }

  // Evict cache entries whose byte range is fully contained within
  // [start_offset, start_offset + length).
  Result<int64_t> EvictEntriesInRange(int64_t start_offset, int64_t length) {
    if (length <= 0) {
      return 0;
    }
    const int64_t end_offset = start_offset + length;
    int64_t n_evicted = 0;
    std::unique_lock<std::mutex> guard(entry_mutex);
    // entries is sorted by range.offset, so we can fast-forward to the first
    // entry whose offset is >= start_offset and stop once we pass end_offset.
    auto it = std::lower_bound(
        entries.begin(), entries.end(), start_offset,
        [](const RangeCacheEntry& entry, int64_t offset) {
          return entry.range.offset < offset;
        });
    while (it != entries.end() && it->range.offset < end_offset) {
      if (it->range.offset + it->range.length <= end_offset) {
        it = entries.erase(it);
        ++n_evicted;
      } else {
        // Entry extends beyond the requested window (e.g. coalesced with a
        // neighboring range). Leave it alone and keep scanning in case
        // smaller siblings follow.
        ++it;
      }
    }
    return n_evicted;
  }

  // Return a Future that completes when the given ranges have been read.
  Future<> WaitFor(std::vector<ReadRange> ranges) {
    auto end = std::remove_if(ranges.begin(), ranges.end(),
                              [](const ReadRange& range) { return range.length == 0; });
    ranges.resize(end - ranges.begin());
    std::vector<Future<>> futures;
    futures.reserve(ranges.size());
    {
      std::unique_lock<std::mutex> guard(entry_mutex);
      for (auto& range : ranges) {
        const auto it = std::lower_bound(
            entries.begin(), entries.end(), range,
            [](const RangeCacheEntry& entry, const ReadRange& range) {
              return entry.range.offset + entry.range.length <
                     range.offset + range.length;
            });
        if (it != entries.end() && it->range.Contains(range)) {
          futures.push_back(Future<>(MaybeRead(&*it)));
        } else {
          return Status::Invalid("Range was not requested for caching: offset=",
                                 range.offset, " length=", range.length);
        }
      }
    }
    return AllComplete(futures);
  }
};

// Don't read ranges when they're first added. Instead, wait until they're
// requested (either through Read or WaitFor). Thread safety is inherited from
// the base Impl: both MakeCacheEntries and MaybeRead are only ever invoked
// from the public entry points, and those hold the base class's entry_mutex
// before delegating.
struct ReadRangeCache::LazyImpl : public ReadRangeCache::Impl {
  virtual ~LazyImpl() = default;

  Future<std::shared_ptr<Buffer>> MaybeRead(RangeCacheEntry* entry) override {
    // Called by the superclass under entry_mutex, so it is safe to mutate
    // entry->future here.
    if (!entry->future.is_valid()) {
      entry->future = file->ReadAsync(ctx, entry->range.offset, entry->range.length);
    }
    return entry->future;
  }

  std::vector<RangeCacheEntry> MakeCacheEntries(
      const std::vector<ReadRange>& ranges) override {
    std::vector<RangeCacheEntry> new_entries;
    new_entries.reserve(ranges.size());
    for (const auto& range : ranges) {
      // In the lazy variant, don't read data here - later, a call to Read or WaitFor
      // will call back to MaybeRead (under the lock) which will fill the future.
      new_entries.emplace_back(range, Future<std::shared_ptr<Buffer>>());
    }
    return new_entries;
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

Status ReadRangeCache::Cache(std::vector<ReadRange> ranges) {
  return impl_->Cache(std::move(ranges));
}

Result<std::shared_ptr<Buffer>> ReadRangeCache::Read(ReadRange range) {
  return impl_->Read(range);
}

Future<> ReadRangeCache::Wait() { return impl_->Wait(); }

Future<> ReadRangeCache::WaitFor(std::vector<ReadRange> ranges) {
  return impl_->WaitFor(std::move(ranges));
}

Result<int64_t> ReadRangeCache::EvictEntriesInRange(int64_t start_offset, int64_t length) {
  return impl_->EvictEntriesInRange(start_offset, length);
}

}  // namespace internal
}  // namespace io
}  // namespace arrow
