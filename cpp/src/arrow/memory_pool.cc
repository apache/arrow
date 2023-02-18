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

#include <algorithm>  // IWYU pragma: keep
#include <atomic>
#include <cstdlib>   // IWYU pragma: keep
#include <cstring>   // IWYU pragma: keep
#include <iostream>  // IWYU pragma: keep
#include <limits>
#include <memory>
#include <mutex>
#include <optional>

#if defined(sun) || defined(__sun)
#include <stdlib.h>
#endif

#include "arrow/buffer.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/config.h"
#include "arrow/util/debug.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"  // IWYU pragma: keep
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/ubsan.h"

#ifdef __GLIBC__
#include <malloc.h>
#endif

#ifdef ARROW_MIMALLOC
#include <mimalloc.h>
#endif

namespace arrow {

namespace memory_pool {

namespace internal {

alignas(kDefaultBufferAlignment) int64_t zero_size_area[1] = {kDebugXorSuffix};

}  // namespace internal

}  // namespace memory_pool

namespace {

constexpr char kDefaultBackendEnvVar[] = "ARROW_DEFAULT_MEMORY_POOL";
constexpr char kDebugMemoryEnvVar[] = "ARROW_DEBUG_MEMORY_POOL";

enum class MemoryPoolBackend : uint8_t { System, Jemalloc, Mimalloc };

struct SupportedBackend {
  const char* name;
  MemoryPoolBackend backend;
};

// See ARROW-12248 for why we use static in-function singletons rather than
// global constants below (in SupportedBackends() and UserSelectedBackend()).
// In some contexts (especially R bindings) `default_memory_pool()` may be
// called before all globals are initialized, and then the ARROW_DEFAULT_MEMORY_POOL
// environment variable would be ignored.

const std::vector<SupportedBackend>& SupportedBackends() {
  static std::vector<SupportedBackend> backends = {
  // ARROW-12316: Apple => mimalloc first, then jemalloc
  //              non-Apple => jemalloc first, then mimalloc
#if defined(ARROW_JEMALLOC) && !defined(__APPLE__)
    {"jemalloc", MemoryPoolBackend::Jemalloc},
#endif
#ifdef ARROW_MIMALLOC
    {"mimalloc", MemoryPoolBackend::Mimalloc},
#endif
#if defined(ARROW_JEMALLOC) && defined(__APPLE__)
    {"jemalloc", MemoryPoolBackend::Jemalloc},
#endif
    {"system", MemoryPoolBackend::System}
  };
  return backends;
}

// Return the MemoryPoolBackend selected by the user through the
// ARROW_DEFAULT_MEMORY_POOL environment variable, if any.
std::optional<MemoryPoolBackend> UserSelectedBackend() {
  static auto user_selected_backend = []() -> std::optional<MemoryPoolBackend> {
    auto unsupported_backend = [](const std::string& name) {
      std::vector<std::string> supported;
      for (const auto backend : SupportedBackends()) {
        supported.push_back(std::string("'") + backend.name + "'");
      }
      ARROW_LOG(WARNING) << "Unsupported backend '" << name << "' specified in "
                         << kDefaultBackendEnvVar << " (supported backends are "
                         << internal::JoinStrings(supported, ", ") << ")";
    };

    auto maybe_name = internal::GetEnvVar(kDefaultBackendEnvVar);
    if (!maybe_name.ok()) {
      return {};
    }
    const auto name = *std::move(maybe_name);
    if (name.empty()) {
      // An empty environment variable is considered missing
      return {};
    }
    const auto found = std::find_if(
        SupportedBackends().begin(), SupportedBackends().end(),
        [&](const SupportedBackend& backend) { return name == backend.name; });
    if (found != SupportedBackends().end()) {
      return found->backend;
    }
    unsupported_backend(name);
    return {};
  }();

  return user_selected_backend;
}

MemoryPoolBackend DefaultBackend() {
  auto backend = UserSelectedBackend();
  if (backend.has_value()) {
    return backend.value();
  }
  struct SupportedBackend default_backend = SupportedBackends().front();
  return default_backend.backend;
}

using MemoryDebugHandler = std::function<void(uint8_t* ptr, int64_t size, const Status&)>;

struct DebugState {
  void Invoke(uint8_t* ptr, int64_t size, const Status& st) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (handler_) {
      handler_(ptr, size, st);
    }
  }

  void SetHandler(MemoryDebugHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);
    handler_ = std::move(handler);
  }

  static DebugState* Instance() {
    // Instance is constructed on-demand. If it was a global static variable,
    // it could be constructed after being used.
    static DebugState instance;
    return &instance;
  }

 private:
  DebugState() = default;

  ARROW_DISALLOW_COPY_AND_ASSIGN(DebugState);

  std::mutex mutex_;
  MemoryDebugHandler handler_;
};

void DebugAbort(uint8_t* ptr, int64_t size, const Status& st) { st.Abort(); }

void DebugTrap(uint8_t* ptr, int64_t size, const Status& st) {
  ARROW_LOG(ERROR) << st.ToString();
  arrow::internal::DebugTrap();
}

void DebugWarn(uint8_t* ptr, int64_t size, const Status& st) {
  ARROW_LOG(WARNING) << st.ToString();
}

bool IsDebugEnabled() {
  static const bool is_enabled = []() {
    auto maybe_env_value = internal::GetEnvVar(kDebugMemoryEnvVar);
    if (!maybe_env_value.ok()) {
      return false;
    }
    auto env_value = *std::move(maybe_env_value);
    if (env_value.empty()) {
      return false;
    }
    auto debug_state = DebugState::Instance();
    if (env_value == "abort") {
      debug_state->SetHandler(DebugAbort);
      return true;
    }
    if (env_value == "trap") {
      debug_state->SetHandler(DebugTrap);
      return true;
    }
    if (env_value == "warn") {
      debug_state->SetHandler(DebugWarn);
      return true;
    }
    ARROW_LOG(WARNING) << "Invalid value for " << kDebugMemoryEnvVar << ": '" << env_value
                       << "'. Valid values are 'abort', 'trap', 'warn'.";
    return false;
  }();

  return is_enabled;
}

// An allocator wrapper that adds a suffix at the end of allocation to check
// for writes beyond the allocated area.
template <typename WrappedAllocator>
class DebugAllocator {
 public:
  static Status AllocateAligned(int64_t size, int64_t alignment, uint8_t** out) {
    if (size == 0) {
      *out = memory_pool::internal::kZeroSizeArea;
    } else {
      ARROW_ASSIGN_OR_RAISE(int64_t raw_size, RawSize(size));
      DCHECK(raw_size > size) << "bug in raw size computation: " << raw_size
                              << " for size " << size;
      RETURN_NOT_OK(WrappedAllocator::AllocateAligned(raw_size, alignment, out));
      InitAllocatedArea(*out, size);
    }
    return Status::OK();
  }

  static void ReleaseUnused() { WrappedAllocator::ReleaseUnused(); }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, int64_t alignment,
                                  uint8_t** ptr) {
    CheckAllocatedArea(*ptr, old_size, "reallocation");
    if (*ptr == memory_pool::internal::kZeroSizeArea) {
      return AllocateAligned(new_size, alignment, ptr);
    }
    if (new_size == 0) {
      // Note that an overflow check isn't needed as `old_size` is supposed to have
      // been successfully passed to AllocateAligned() before.
      WrappedAllocator::DeallocateAligned(*ptr, old_size + kOverhead, alignment);
      *ptr = memory_pool::internal::kZeroSizeArea;
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(int64_t raw_new_size, RawSize(new_size));
    DCHECK(raw_new_size > new_size)
        << "bug in raw size computation: " << raw_new_size << " for size " << new_size;
    RETURN_NOT_OK(WrappedAllocator::ReallocateAligned(old_size + kOverhead, raw_new_size,
                                                      alignment, ptr));
    InitAllocatedArea(*ptr, new_size);
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size, int64_t alignment) {
    CheckAllocatedArea(ptr, size, "deallocation");
    if (ptr != memory_pool::internal::kZeroSizeArea) {
      WrappedAllocator::DeallocateAligned(ptr, size + kOverhead, alignment);
    }
  }

 private:
  static Result<int64_t> RawSize(int64_t size) {
    if (ARROW_PREDICT_FALSE(internal::AddWithOverflow(size, kOverhead, &size))) {
      return Status::OutOfMemory("Memory allocation size too large");
    }
    return size;
  }

  static void InitAllocatedArea(uint8_t* ptr, int64_t size) {
    DCHECK_NE(size, 0);
    util::SafeStore(ptr + size, size ^ memory_pool::internal::kDebugXorSuffix);
  }

  static void CheckAllocatedArea(uint8_t* ptr, int64_t size, const char* context) {
    // Check that memory wasn't clobbered at the end of the allocated area.
    int64_t stored_size =
        memory_pool::internal::kDebugXorSuffix ^ util::SafeLoadAs<int64_t>(ptr + size);
    if (ARROW_PREDICT_FALSE(stored_size != size)) {
      auto st = Status::Invalid("Wrong size on ", context, ": given size = ", size,
                                ", actual size = ", stored_size);
      DebugState::Instance()->Invoke(ptr, size, st);
    }
  }

  static constexpr int64_t kOverhead = sizeof(int64_t);
};

// Helper class directing allocations to the standard system allocator.
class SystemAllocator {
 public:
  // Allocate memory according to the alignment requirements for Arrow
  // (as of May 2016 64 bytes)
  static Status AllocateAligned(int64_t size, int64_t alignment, uint8_t** out) {
    if (size == 0) {
      *out = memory_pool::internal::kZeroSizeArea;
      return Status::OK();
    }
#ifdef _WIN32
    // Special code path for Windows
    *out = reinterpret_cast<uint8_t*>(
        _aligned_malloc(static_cast<size_t>(size), static_cast<size_t>(alignment)));
    if (!*out) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
#elif defined(sun) || defined(__sun)
    *out = reinterpret_cast<uint8_t*>(
        memalign(static_cast<size_t>(alignment), static_cast<size_t>(size)));
    if (!*out) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
#else
    const int result =
        posix_memalign(reinterpret_cast<void**>(out), static_cast<size_t>(alignment),
                       static_cast<size_t>(size));
    if (result == ENOMEM) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }

    if (result == EINVAL) {
      return Status::Invalid("invalid alignment parameter: ",
                             static_cast<size_t>(alignment));
    }
#endif
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, int64_t alignment,
                                  uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == memory_pool::internal::kZeroSizeArea) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, alignment, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size, alignment);
      *ptr = memory_pool::internal::kZeroSizeArea;
      return Status::OK();
    }
    // Note: We cannot use realloc() here as it doesn't guarantee alignment.

    // Allocate new chunk
    uint8_t* out = nullptr;
    RETURN_NOT_OK(AllocateAligned(new_size, alignment, &out));
    DCHECK(out);
    // Copy contents and release old memory chunk
    memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
#ifdef _WIN32
    _aligned_free(*ptr);
#else
    free(*ptr);
#endif  // defined(_WIN32)
    *ptr = out;
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size, int64_t /*alignment*/) {
    if (ptr == memory_pool::internal::kZeroSizeArea) {
      DCHECK_EQ(size, 0);
    } else {
#ifdef _WIN32
      _aligned_free(ptr);
#else
      free(ptr);
#endif
    }
  }

  static void ReleaseUnused() {
#ifdef __GLIBC__
    // The return value of malloc_trim is not an error but to inform
    // you if memory was actually released or not, which we do not care about here
    ARROW_UNUSED(malloc_trim(0));
#endif
  }
};

#ifdef ARROW_MIMALLOC

// Helper class directing allocations to the mimalloc allocator.
class MimallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, int64_t alignment, uint8_t** out) {
    if (size == 0) {
      *out = memory_pool::internal::kZeroSizeArea;
      return Status::OK();
    }
    *out = reinterpret_cast<uint8_t*>(
        mi_malloc_aligned(static_cast<size_t>(size), static_cast<size_t>(alignment)));
    if (*out == NULL) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    return Status::OK();
  }

  static void ReleaseUnused() { mi_collect(true); }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, int64_t alignment,
                                  uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == memory_pool::internal::kZeroSizeArea) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, alignment, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size, alignment);
      *ptr = memory_pool::internal::kZeroSizeArea;
      return Status::OK();
    }
    *ptr = reinterpret_cast<uint8_t*>(
        mi_realloc_aligned(previous_ptr, static_cast<size_t>(new_size), alignment));
    if (*ptr == NULL) {
      *ptr = previous_ptr;
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size, int64_t /*alignment*/) {
    if (ptr == memory_pool::internal::kZeroSizeArea) {
      DCHECK_EQ(size, 0);
    } else {
      mi_free(ptr);
    }
  }
};

#endif  // defined(ARROW_MIMALLOC)

}  // namespace

int64_t MemoryPool::max_memory() const { return -1; }

///////////////////////////////////////////////////////////////////////
// MemoryPool implementation that delegates its core duty
// to an Allocator class.

#ifndef NDEBUG
static constexpr uint8_t kAllocPoison = 0xBC;
static constexpr uint8_t kReallocPoison = 0xBD;
static constexpr uint8_t kDeallocPoison = 0xBE;
#endif

template <typename Allocator>
class BaseMemoryPoolImpl : public MemoryPool {
 public:
  ~BaseMemoryPoolImpl() override {}

  Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
    if (size < 0) {
      return Status::Invalid("negative malloc size");
    }
    if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
      return Status::OutOfMemory("malloc size overflows size_t");
    }
    RETURN_NOT_OK(Allocator::AllocateAligned(size, alignment, out));
#ifndef NDEBUG
    // Poison data
    if (size > 0) {
      DCHECK_NE(*out, nullptr);
      (*out)[0] = kAllocPoison;
      (*out)[size - 1] = kAllocPoison;
    }
#endif

    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                    uint8_t** ptr) override {
    if (new_size < 0) {
      return Status::Invalid("negative realloc size");
    }
    if (static_cast<uint64_t>(new_size) >= std::numeric_limits<size_t>::max()) {
      return Status::OutOfMemory("realloc overflows size_t");
    }
    RETURN_NOT_OK(Allocator::ReallocateAligned(old_size, new_size, alignment, ptr));
#ifndef NDEBUG
    // Poison data
    if (new_size > old_size) {
      DCHECK_NE(*ptr, nullptr);
      (*ptr)[old_size] = kReallocPoison;
      (*ptr)[new_size - 1] = kReallocPoison;
    }
#endif

    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
#ifndef NDEBUG
    // Poison data
    if (size > 0) {
      DCHECK_NE(buffer, nullptr);
      buffer[0] = kDeallocPoison;
      buffer[size - 1] = kDeallocPoison;
    }
#endif
    Allocator::DeallocateAligned(buffer, size, alignment);

    stats_.UpdateAllocatedBytes(-size, /*is_free*/ true);
  }

  void ReleaseUnused() override { Allocator::ReleaseUnused(); }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  int64_t max_memory() const override { return stats_.max_memory(); }

  int64_t total_bytes_allocated() const override {
    return stats_.total_bytes_allocated();
  }

  int64_t num_allocations() const override { return stats_.num_allocations(); }

 protected:
  internal::MemoryPoolStats stats_;
};

class SystemMemoryPool : public BaseMemoryPoolImpl<SystemAllocator> {
 public:
  std::string backend_name() const override { return "system"; }
};

class SystemDebugMemoryPool : public BaseMemoryPoolImpl<DebugAllocator<SystemAllocator>> {
 public:
  std::string backend_name() const override { return "system"; }
};

#ifdef ARROW_JEMALLOC
class JemallocMemoryPool
    : public BaseMemoryPoolImpl<memory_pool::internal::JemallocAllocator> {
 public:
  std::string backend_name() const override { return "jemalloc"; }
};

class JemallocDebugMemoryPool
    : public BaseMemoryPoolImpl<
          DebugAllocator<memory_pool::internal::JemallocAllocator>> {
 public:
  std::string backend_name() const override { return "jemalloc"; }
};
#endif

#ifdef ARROW_MIMALLOC
class MimallocMemoryPool : public BaseMemoryPoolImpl<MimallocAllocator> {
 public:
  std::string backend_name() const override { return "mimalloc"; }
};

class MimallocDebugMemoryPool
    : public BaseMemoryPoolImpl<DebugAllocator<MimallocAllocator>> {
 public:
  std::string backend_name() const override { return "mimalloc"; }
};
#endif

std::unique_ptr<MemoryPool> MemoryPool::CreateDefault() {
  auto backend = DefaultBackend();
  switch (backend) {
    case MemoryPoolBackend::System:
      return IsDebugEnabled() ? std::unique_ptr<MemoryPool>(new SystemDebugMemoryPool)
                              : std::unique_ptr<MemoryPool>(new SystemMemoryPool);
#ifdef ARROW_JEMALLOC
    case MemoryPoolBackend::Jemalloc:
      return IsDebugEnabled() ? std::unique_ptr<MemoryPool>(new JemallocDebugMemoryPool)
                              : std::unique_ptr<MemoryPool>(new JemallocMemoryPool);
#endif
#ifdef ARROW_MIMALLOC
    case MemoryPoolBackend::Mimalloc:
      return IsDebugEnabled() ? std::unique_ptr<MemoryPool>(new MimallocDebugMemoryPool)
                              : std::unique_ptr<MemoryPool>(new MimallocMemoryPool);
#endif
    default:
      ARROW_LOG(FATAL) << "Internal error: cannot create default memory pool";
      return nullptr;
  }
}

static struct GlobalState {
  ~GlobalState() { finalizing_.store(true, std::memory_order_relaxed); }

  bool is_finalizing() const { return finalizing_.load(std::memory_order_relaxed); }

  MemoryPool* system_memory_pool() {
    if (IsDebugEnabled()) {
      return &system_debug_pool_;
    } else {
      return &system_pool_;
    }
  }

#ifdef ARROW_JEMALLOC
  MemoryPool* jemalloc_memory_pool() {
    if (IsDebugEnabled()) {
      return &jemalloc_debug_pool_;
    } else {
      return &jemalloc_pool_;
    }
  }
#endif

#ifdef ARROW_MIMALLOC
  MemoryPool* mimalloc_memory_pool() {
    if (IsDebugEnabled()) {
      return &mimalloc_debug_pool_;
    } else {
      return &mimalloc_pool_;
    }
  }
#endif

 private:
  std::atomic<bool> finalizing_{false};  // constructed first, destroyed last

  SystemMemoryPool system_pool_;
  SystemDebugMemoryPool system_debug_pool_;
#ifdef ARROW_JEMALLOC
  JemallocMemoryPool jemalloc_pool_;
  JemallocDebugMemoryPool jemalloc_debug_pool_;
#endif
#ifdef ARROW_MIMALLOC
  MimallocMemoryPool mimalloc_pool_;
  MimallocDebugMemoryPool mimalloc_debug_pool_;
#endif
} global_state;

MemoryPool* system_memory_pool() { return global_state.system_memory_pool(); }

Status jemalloc_memory_pool(MemoryPool** out) {
#ifdef ARROW_JEMALLOC
  *out = global_state.jemalloc_memory_pool();
  return Status::OK();
#else
  return Status::NotImplemented("This Arrow build does not enable jemalloc");
#endif
}

Status mimalloc_memory_pool(MemoryPool** out) {
#ifdef ARROW_MIMALLOC
  *out = global_state.mimalloc_memory_pool();
  return Status::OK();
#else
  return Status::NotImplemented("This Arrow build does not enable mimalloc");
#endif
}

MemoryPool* default_memory_pool() {
  auto backend = DefaultBackend();
  switch (backend) {
    case MemoryPoolBackend::System:
      return global_state.system_memory_pool();
#ifdef ARROW_JEMALLOC
    case MemoryPoolBackend::Jemalloc:
      return global_state.jemalloc_memory_pool();
#endif
#ifdef ARROW_MIMALLOC
    case MemoryPoolBackend::Mimalloc:
      return global_state.mimalloc_memory_pool();
#endif
    default:
      ARROW_LOG(FATAL) << "Internal error: cannot create default memory pool";
      return nullptr;
  }
}

#ifndef ARROW_JEMALLOC
Status jemalloc_set_decay_ms(int ms) {
  return Status::NotImplemented("jemalloc support is not built");
}

Result<int64_t> jemalloc_get_stat(const char* name) {
  return Status::NotImplemented("jemalloc support is not built");
}

Status jemalloc_peak_reset() {
  return Status::NotImplemented("jemalloc support is not built");
}

Status jemalloc_stats_print(const char* opts) {
  return Status::NotImplemented("jemalloc support is not built");
}

Status jemalloc_stats_print(std::function<void(const char*)> write_cb, const char* opts) {
  return Status::NotImplemented("jemalloc support is not built");
}

Result<std::string> jemalloc_stats_string(const char* opts) {
  return Status::NotImplemented("jemalloc support is not built");
}

#endif

///////////////////////////////////////////////////////////////////////
// LoggingMemoryPool implementation

LoggingMemoryPool::LoggingMemoryPool(MemoryPool* pool) : pool_(pool) {}

Status LoggingMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  Status s = pool_->Allocate(size, alignment, out);
  std::cout << "Allocate: size = " << size << ", alignment = " << alignment << std::endl;
  return s;
}

Status LoggingMemoryPool::Reallocate(int64_t old_size, int64_t new_size,
                                     int64_t alignment, uint8_t** ptr) {
  Status s = pool_->Reallocate(old_size, new_size, ptr);
  std::cout << "Reallocate: old_size = " << old_size << ", new_size = " << new_size
            << ", alignment = " << alignment << std::endl;
  return s;
}

void LoggingMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  pool_->Free(buffer, size, alignment);
  std::cout << "Free: size = " << size << ", alignment = " << alignment << std::endl;
}

int64_t LoggingMemoryPool::bytes_allocated() const {
  int64_t nb_bytes = pool_->bytes_allocated();
  std::cout << "bytes_allocated: " << nb_bytes << std::endl;
  return nb_bytes;
}

int64_t LoggingMemoryPool::max_memory() const {
  int64_t mem = pool_->max_memory();
  std::cout << "max_memory: " << mem << std::endl;
  return mem;
}

int64_t LoggingMemoryPool::total_bytes_allocated() const {
  int64_t mem = pool_->total_bytes_allocated();
  std::cout << "total_bytes_allocated: " << mem << std::endl;
  return mem;
}

int64_t LoggingMemoryPool::num_allocations() const {
  int64_t mem = pool_->num_allocations();
  std::cout << "num_allocations: " << mem << std::endl;
  return mem;
}

std::string LoggingMemoryPool::backend_name() const { return pool_->backend_name(); }

///////////////////////////////////////////////////////////////////////
// ProxyMemoryPool implementation

class ProxyMemoryPool::ProxyMemoryPoolImpl {
 public:
  explicit ProxyMemoryPoolImpl(MemoryPool* pool) : pool_(pool) {}

  Status Allocate(int64_t size, int64_t alignment, uint8_t** out) {
    RETURN_NOT_OK(pool_->Allocate(size, alignment, out));
    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                    uint8_t** ptr) {
    RETURN_NOT_OK(pool_->Reallocate(old_size, new_size, alignment, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) {
    pool_->Free(buffer, size, alignment);
    stats_.UpdateAllocatedBytes(-size, /*is_free=*/true);
  }

  int64_t bytes_allocated() const { return stats_.bytes_allocated(); }

  int64_t max_memory() const { return stats_.max_memory(); }

  int64_t total_bytes_allocated() const { return stats_.total_bytes_allocated(); }

  int64_t num_allocations() const { return stats_.num_allocations(); }

  std::string backend_name() const { return pool_->backend_name(); }

 private:
  MemoryPool* pool_;
  internal::MemoryPoolStats stats_;
};

ProxyMemoryPool::ProxyMemoryPool(MemoryPool* pool) {
  impl_.reset(new ProxyMemoryPoolImpl(pool));
}

ProxyMemoryPool::~ProxyMemoryPool() {}

Status ProxyMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
  return impl_->Allocate(size, alignment, out);
}

Status ProxyMemoryPool::Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                                   uint8_t** ptr) {
  return impl_->Reallocate(old_size, new_size, alignment, ptr);
}

void ProxyMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t alignment) {
  return impl_->Free(buffer, size, alignment);
}

int64_t ProxyMemoryPool::bytes_allocated() const { return impl_->bytes_allocated(); }

int64_t ProxyMemoryPool::max_memory() const { return impl_->max_memory(); }

int64_t ProxyMemoryPool::total_bytes_allocated() const {
  return impl_->total_bytes_allocated();
}

int64_t ProxyMemoryPool::num_allocations() const { return impl_->num_allocations(); }

std::string ProxyMemoryPool::backend_name() const { return impl_->backend_name(); }

std::vector<std::string> SupportedMemoryBackendNames() {
  std::vector<std::string> supported;
  for (const auto backend : SupportedBackends()) {
    supported.push_back(backend.name);
  }
  return supported;
}

// -----------------------------------------------------------------------
// Pool buffer and allocation

/// A Buffer whose lifetime is tied to a particular MemoryPool
class PoolBuffer final : public ResizableBuffer {
 public:
  explicit PoolBuffer(std::shared_ptr<MemoryManager> mm, MemoryPool* pool,
                      int64_t alignment)
      : ResizableBuffer(nullptr, 0, std::move(mm)), pool_(pool), alignment_(alignment) {}

  ~PoolBuffer() override {
    // Avoid calling pool_->Free if the global pools are destroyed
    // (XXX this will not work with user-defined pools)

    // This can happen if a Future is destructing on one thread while or
    // after memory pools are destructed on the main thread (as there is
    // no guarantee of destructor order between thread/memory pools)
    uint8_t* ptr = mutable_data();
    if (ptr && !global_state.is_finalizing()) {
      pool_->Free(ptr, capacity_, alignment_);
    }
  }

  Status Reserve(const int64_t capacity) override {
    if (capacity < 0) {
      return Status::Invalid("Negative buffer capacity: ", capacity);
    }
    uint8_t* ptr = mutable_data();
    if (!ptr || capacity > capacity_) {
      int64_t new_capacity = bit_util::RoundUpToMultipleOf64(capacity);
      if (ptr) {
        RETURN_NOT_OK(pool_->Reallocate(capacity_, new_capacity, alignment_, &ptr));
      } else {
        RETURN_NOT_OK(pool_->Allocate(new_capacity, alignment_, &ptr));
      }
      data_ = ptr;
      capacity_ = new_capacity;
    }
    return Status::OK();
  }

  Status Resize(const int64_t new_size, bool shrink_to_fit = true) override {
    if (ARROW_PREDICT_FALSE(new_size < 0)) {
      return Status::Invalid("Negative buffer resize: ", new_size);
    }
    uint8_t* ptr = mutable_data();
    if (ptr && shrink_to_fit && new_size <= size_) {
      // Buffer is non-null and is not growing, so shrink to the requested size without
      // excess space.
      int64_t new_capacity = bit_util::RoundUpToMultipleOf64(new_size);
      if (capacity_ != new_capacity) {
        // Buffer hasn't got yet the requested size.
        RETURN_NOT_OK(pool_->Reallocate(capacity_, new_capacity, alignment_, &ptr));
        data_ = ptr;
        capacity_ = new_capacity;
      }
    } else {
      RETURN_NOT_OK(Reserve(new_size));
    }
    size_ = new_size;

    return Status::OK();
  }

  static std::shared_ptr<PoolBuffer> MakeShared(MemoryPool* pool, int64_t alignment) {
    std::shared_ptr<MemoryManager> mm;
    if (pool == nullptr) {
      pool = default_memory_pool();
      mm = default_cpu_memory_manager();
    } else {
      mm = CPUDevice::memory_manager(pool);
    }
    return std::make_shared<PoolBuffer>(std::move(mm), pool, alignment);
  }

  static std::unique_ptr<PoolBuffer> MakeUnique(MemoryPool* pool, int64_t alignment) {
    std::shared_ptr<MemoryManager> mm;
    if (pool == nullptr) {
      pool = default_memory_pool();
      mm = default_cpu_memory_manager();
    } else {
      mm = CPUDevice::memory_manager(pool);
    }
    return std::make_unique<PoolBuffer>(std::move(mm), pool, alignment);
  }

 private:
  MemoryPool* pool_;
  int64_t alignment_;
};

namespace {
// A utility that does most of the work of the `AllocateBuffer` and
// `AllocateResizableBuffer` methods. The argument `buffer` should be a smart pointer to
// a PoolBuffer.
template <typename BufferPtr, typename PoolBufferPtr>
inline Result<BufferPtr> ResizePoolBuffer(PoolBufferPtr&& buffer, const int64_t size) {
  RETURN_NOT_OK(buffer->Resize(size));
  buffer->ZeroPadding();
  return std::move(buffer);
}

}  // namespace

Result<std::unique_ptr<Buffer>> AllocateBuffer(const int64_t size, MemoryPool* pool) {
  return AllocateBuffer(size, kDefaultBufferAlignment, pool);
}

Result<std::unique_ptr<Buffer>> AllocateBuffer(const int64_t size,
                                               const int64_t alignment,
                                               MemoryPool* pool) {
  return ResizePoolBuffer<std::unique_ptr<Buffer>>(
      PoolBuffer::MakeUnique(pool, alignment), size);
}

Result<std::unique_ptr<ResizableBuffer>> AllocateResizableBuffer(const int64_t size,
                                                                 MemoryPool* pool) {
  return AllocateResizableBuffer(size, kDefaultBufferAlignment, pool);
}

Result<std::unique_ptr<ResizableBuffer>> AllocateResizableBuffer(const int64_t size,
                                                                 const int64_t alignment,
                                                                 MemoryPool* pool) {
  return ResizePoolBuffer<std::unique_ptr<ResizableBuffer>>(
      PoolBuffer::MakeUnique(pool, alignment), size);
}

}  // namespace arrow
