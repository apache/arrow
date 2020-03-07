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
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/caching.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace io {
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
  int64_t hole_size_limit;
  int64_t range_size_limit;

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
                               int64_t hole_size_limit, int64_t range_size_limit)
    : impl_(new Impl()) {
  impl_->file = std::move(file);
  impl_->hole_size_limit = hole_size_limit;
  impl_->range_size_limit = range_size_limit;
}

ReadRangeCache::~ReadRangeCache() {}

Status ReadRangeCache::Cache(std::vector<ReadRange> ranges) {
  ranges = internal::CoalesceReadRanges(std::move(ranges), impl_->hole_size_limit,
                                        impl_->range_size_limit);
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
