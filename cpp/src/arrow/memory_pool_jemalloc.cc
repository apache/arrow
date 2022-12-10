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

#include "arrow/memory_pool_internal.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"  // IWYU pragma: keep

// We can't put the jemalloc memory pool implementation into
// memory_pool.c because jemalloc.h may redefine malloc() and its
// family by macros. If malloc() and its family are redefined by
// jemalloc, our system memory pool will also use jemalloc's malloc() and
// its family.

#ifdef ARROW_JEMALLOC_VENDORED
#define JEMALLOC_MANGLE
// Explicitly link to our version of jemalloc
#include "jemalloc_ep/dist/include/jemalloc/jemalloc.h"
#else
#include <jemalloc/jemalloc.h>
#endif

#ifdef ARROW_JEMALLOC_VENDORED
// Compile-time configuration for vendored jemalloc options.
// Note the prefix ("je_arrow_") must match the symbol prefix given when
// building jemalloc.
// See discussion in https://github.com/jemalloc/jemalloc/issues/1621

// ARROW-6910(wesm): we found that jemalloc's default behavior with respect to
// dirty / muzzy pages (see definitions of these in the jemalloc documentation)
// conflicted with user expectations, and would even cause memory use problems
// in some cases. By enabling the background_thread option and reducing the
// decay time from 10 seconds to 1 seconds, memory is released more
// aggressively (and in the background) to the OS. This can be configured
// further by using the arrow::jemalloc_set_decay_ms API

#undef USE_JEMALLOC_BACKGROUND_THREAD
#ifndef __APPLE__
// ARROW-6977: jemalloc's background_thread isn't always enabled on macOS
#define USE_JEMALLOC_BACKGROUND_THREAD
#endif

// In debug mode, add memory poisoning on alloc / free
#ifdef NDEBUG
#define JEMALLOC_DEBUG_OPTIONS ""
#else
#define JEMALLOC_DEBUG_OPTIONS ",junk:true"
#endif

const char* je_arrow_malloc_conf =
    ("oversize_threshold:0"
#ifdef USE_JEMALLOC_BACKGROUND_THREAD
     ",dirty_decay_ms:1000"
     ",muzzy_decay_ms:1000"
     ",background_thread:true"
#else
     // ARROW-6994: return memory immediately to the OS if the
     // background_thread option isn't available
     ",dirty_decay_ms:0"
     ",muzzy_decay_ms:0"
#endif
     JEMALLOC_DEBUG_OPTIONS);  // NOLINT: whitespace/parens

#endif  // ARROW_JEMALLOC_VENDORED

namespace arrow {

namespace memory_pool {

namespace internal {

Status JemallocAllocator::AllocateAligned(int64_t size, int64_t alignment,
                                          uint8_t** out) {
  if (size == 0) {
    *out = kZeroSizeArea;
    return Status::OK();
  }
  *out = reinterpret_cast<uint8_t*>(
      mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(static_cast<size_t>(alignment))));
  if (*out == NULL) {
    return Status::OutOfMemory("malloc of size ", size, " failed");
  }
  return Status::OK();
}

Status JemallocAllocator::ReallocateAligned(int64_t old_size, int64_t new_size,
                                            int64_t alignment, uint8_t** ptr) {
  uint8_t* previous_ptr = *ptr;
  if (previous_ptr == kZeroSizeArea) {
    DCHECK_EQ(old_size, 0);
    return AllocateAligned(new_size, alignment, ptr);
  }
  if (new_size == 0) {
    DeallocateAligned(previous_ptr, old_size, alignment);
    *ptr = kZeroSizeArea;
    return Status::OK();
  }
  *ptr =
      reinterpret_cast<uint8_t*>(rallocx(*ptr, static_cast<size_t>(new_size),
                                         MALLOCX_ALIGN(static_cast<size_t>(alignment))));
  if (*ptr == NULL) {
    *ptr = previous_ptr;
    return Status::OutOfMemory("realloc of size ", new_size, " failed");
  }
  return Status::OK();
}

void JemallocAllocator::DeallocateAligned(uint8_t* ptr, int64_t size, int64_t alignment) {
  if (ptr == kZeroSizeArea) {
    DCHECK_EQ(size, 0);
  } else {
    sdallocx(ptr, static_cast<size_t>(size),
             MALLOCX_ALIGN(static_cast<size_t>(alignment)));
  }
}

void JemallocAllocator::ReleaseUnused() {
  mallctl("arena." ARROW_STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", NULL, NULL, NULL, 0);
}

}  // namespace internal

}  // namespace memory_pool

#define RETURN_IF_JEMALLOC_ERROR(ERR)                  \
  do {                                                 \
    if (err != 0) {                                    \
      return Status::UnknownError(std::strerror(ERR)); \
    }                                                  \
  } while (0)

Status jemalloc_set_decay_ms(int ms) {
  ssize_t decay_time_ms = static_cast<ssize_t>(ms);

  int err = mallctl("arenas.dirty_decay_ms", nullptr, nullptr, &decay_time_ms,
                    sizeof(decay_time_ms));
  RETURN_IF_JEMALLOC_ERROR(err);
  err = mallctl("arenas.muzzy_decay_ms", nullptr, nullptr, &decay_time_ms,
                sizeof(decay_time_ms));
  RETURN_IF_JEMALLOC_ERROR(err);

  return Status::OK();
}

#undef RETURN_IF_JEMALLOC_ERROR

Result<int64_t> jemalloc_get_stat(const char* name) {
  size_t sz;
  int err;

  // Update the statistics cached by mallctl.
  if (std::strcmp(name, "stats.allocated") == 0 ||
      std::strcmp(name, "stats.active") == 0 ||
      std::strcmp(name, "stats.metadata") == 0 ||
      std::strcmp(name, "stats.resident") == 0 ||
      std::strcmp(name, "stats.mapped") == 0 ||
      std::strcmp(name, "stats.retained") == 0) {
    uint64_t epoch;
    sz = sizeof(epoch);
    mallctl("epoch", &epoch, &sz, &epoch, sz);
  }

  // Depending on the stat being queried and on the platform, we could need
  // to pass a uint32_t or uint64_t pointer. Try both.
  {
    uint64_t value = 0;
    sz = sizeof(value);
    err = mallctl(name, &value, &sz, nullptr, 0);
    if (!err) {
      return value;
    }
  }
  // EINVAL means the given value length (`sz`) was incorrect.
  if (err == EINVAL) {
    uint32_t value = 0;
    sz = sizeof(value);
    err = mallctl(name, &value, &sz, nullptr, 0);
    if (!err) {
      return value;
    }
  }

  return arrow::internal::IOErrorFromErrno(err, "Failed retrieving ", &name);
}

Status jemalloc_peak_reset() {
  int err = mallctl("thread.peak.reset", nullptr, nullptr, nullptr, 0);
  return err ? arrow::internal::IOErrorFromErrno(err, "Failed resetting thread.peak.")
             : Status::OK();
}

Result<std::string> jemalloc_stats_string(const char* opts) {
  std::string stats;
  auto write_cb = [&stats](const char* str) { stats.append(str); };
  ARROW_UNUSED(jemalloc_stats_print(write_cb, opts));
  return stats;
}

Status jemalloc_stats_print(const char* opts) {
  malloc_stats_print(nullptr, nullptr, opts);
  return Status::OK();
}

Status jemalloc_stats_print(std::function<void(const char*)> write_cb, const char* opts) {
  auto cb_wrapper = [](void* opaque, const char* str) {
    (*static_cast<std::function<void(const char*)>*>(opaque))(str);
  };
  malloc_stats_print(cb_wrapper, &write_cb, opts);
  return Status::OK();
}

}  // namespace arrow
